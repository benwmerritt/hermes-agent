"""One Discord connector, a durable queue, and retained complete workers."""

from __future__ import annotations

import asyncio
from dataclasses import asdict
import hashlib
import logging
import os
from pathlib import Path
import time

from .kubernetes import WorkerIdentity
from .ledger import AdmissionError, Ledger, OwnershipError, canonical
from .media import MediaStore
from .wire import event_identifier, routing_key, to_wire

log = logging.getLogger(__name__)


class Controller:
    def __init__(self, config: dict, backend, knowledge, *, connector_factory=None):
        self.config, self.backend, self.knowledge = config, backend, knowledge
        root = Path(config["data_dir"])
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.ledger = Ledger(root / "router.sqlite", max_workers=config.get("max_workers", 3),
                             max_queued=config.get("max_queued", 100))
        self.media = MediaStore(root / "media", self.ledger)
        if connector_factory is None:
            from .discord_connector import DiscordConnector
            connector_factory = DiscordConnector
        self.connector = connector_factory(config, self.route, self.prompt_response, ledger=self.ledger,
                                           media=self.media, relay_url=config["relay_url"], fence_owner=self.fence_owner)
        self.connections = {}
        self.wake = asyncio.Event()
        self.stopping = False
        self.last_tick = 0.0
        self.last_inspection = 0.0
        self.task = None
        self.operations = asyncio.Lock()

    async def start(self):
        for owner in self.ledger.conversations():
            if owner["status"] in ("queued", "stopped", "recovery_required"):
                self.knowledge.revoke_worker(owner["id"], owner["generation"])
            else:
                # Grants are durable. Re-registering on every controller restart
                # would resurrect credentials deliberately revoked after a crash.
                if owner["status"] == "ready":
                    self.ledger.transition(owner["id"], owner["generation"], "unavailable",
                                           detail="controller restarted; waiting for the same retained worker")
        await self.connector.connect()
        self.task = asyncio.create_task(self.run(), name="conversation-controller")
        self.wake.set()

    async def close(self):
        self.stopping = True
        self.wake.set()
        if self.task:
            await asyncio.wait_for(self.task, 30)
        for connection in list(self.connections.values()):
            await connection.socket.close(code=1012, reason="controller restart; reconnect same owner")
        await self.connector.disconnect()
        await self.backend.aclose()

    def register_knowledge(self, owner):
        self.knowledge.register_worker(token=owner["knowledge_token"], agent_id=self.config.get("agent_id", "timmy"),
                                       conversation_key=owner["id"], generation=owner["generation"],
                                       audience=owner["audience"], read_audiences=[owner["audience"]])

    def worker_config(self, owner):
        native_config = self.config["native_config"]
        return {"schema_version": 1, "worker_id": owner["id"], "generation": owner["generation"],
                "conversation_key": owner["native_key"], "source": owner["source"],
                "allowed_user_ids": [owner["actor"]], "hermes_home": "/data/hermes",
                "workspace": "/data/workspace", "config_source": "/config/hermes.yaml",
                "config_revision": hashlib.sha256(canonical(native_config).encode()).hexdigest(),
                "personality": self.config.get("personality", ""),
                "relay": {"url": self.config["relay_url"], "bot_id": self.config["bot_id"]},
                "knowledge": {"url": self.config["relay_url"], "audience": owner["audience"]},
                "secret_files": {key: f"/credentials/{key}" for key in
                                 ("OPENAI_API_KEY", "GATEWAY_RELAY_SECRET", "HERMES_CLUSTER_KNOWLEDGE_TOKEN")}}

    async def provision(self, owner):
        current = self.ledger.get(owner["id"])
        if current["generation"] != owner["generation"] or current["status"] != "provisioning":
            raise OwnershipError("worker is no longer authorized for provisioning")
        self.register_knowledge(owner)
        secret_name = f'hermes-worker-{owner["id"]}-{owner["generation"]}'
        provider_key = os.environ.get("OPENAI_API_KEY", "")
        if not provider_key:
            raise RuntimeError("provider credential is unavailable")
        await self.backend.create_credentials(secret_name, {"OPENAI_API_KEY": provider_key,
                                               "GATEWAY_RELAY_SECRET": owner["relay_secret"],
                                               "HERMES_CLUSTER_KNOWLEDGE_TOKEN": owner["knowledge_token"]})
        current = self.ledger.get(owner["id"])
        if current["generation"] != owner["generation"] or current["status"] != "provisioning":
            raise OwnershipError("worker was fenced while preparing credentials")
        old = owner["identity"]
        kwargs = {}
        if owner["generation"] > 1:
            if not old or not old.get("pvc_uid"):
                raise OwnershipError("recovery lacks retained claim identity")
            kwargs = {"require_existing_claim": True, "expected_claim_uid": old["pvc_uid"],
                      "expected_volume_name": old.get("volume_name")}
        identity = await self.backend.create_worker(owner["id"], owner["generation"],
                                                     self.worker_config(owner), secret_name, **kwargs)
        current = self.ledger.get(owner["id"])
        self.ledger.transition(owner["id"], owner["generation"], current["status"], identity=asdict(identity),
                               detail="Pod created; waiting for the retained worker to connect" if current["status"] == "provisioning" else current["detail"])

    async def run(self):
        while not self.stopping:
            self.wake.clear()
            try:
                async with self.operations:
                    # Interrupted provisioning is idempotent at the exact generation
                    # and object-content boundary; it cannot allocate another owner.
                    for owner in self.ledger.conversations():
                        if owner["status"] == "provisioning" and (not owner["identity"] or owner["identity"]["generation"] != owner["generation"]):
                            await self.provision_or_fence(owner)
                    while owner := self.ledger.reserve_next():
                        await self.provision_or_fence(owner)
                for cid, connection in list(self.connections.items()):
                    if self.ledger.get(cid)["status"] != "ready":
                        continue
                    for event in self.ledger.queued(cid):
                        self.ledger.dispatch(event["id"], cid, connection.owner["generation"])
                        try:
                            await connection.send({"type": "inbound", "event": event["payload"], "bufferId": event["id"]})
                        except Exception:
                            current = self.ledger.get(cid)
                            if current["generation"] == connection.owner["generation"] and current["status"] == "ready":
                                self.ledger.transition(cid, connection.owner["generation"], "unavailable",
                                                       detail="delivery uncertain; message will not be replayed automatically")
                            break
                if time.monotonic() - self.last_inspection > 10:
                    await self.inspect_workers()
                    self.last_inspection = time.monotonic()
                self.last_tick = time.monotonic()
            except Exception as exc:
                # Do not log API response bodies, credentials, prompts or transcripts.
                log.error("controller iteration failed: %s", type(exc).__name__)
            try:
                await asyncio.wait_for(self.wake.wait(), 2)
            except asyncio.TimeoutError:
                pass

    async def provision_or_fence(self, owner):
        try:
            await self.provision(owner)
        except Exception as exc:
            await self.fence_owner(owner, f"provisioning needs inspection: {type(exc).__name__}")
            self.ledger.record(owner["id"], "provisioning_failed", {"error_type": type(exc).__name__})
            await self.connector.notice(owner, "This worker could not start. Its request is retained for operator inspection.")

    async def authorized_audience(self, owner, source):
        try:
            audience = await self.connector.audience(source)
        except OwnershipError:
            if owner:
                await self.fence_owner(owner, "Discord destination is no longer authorized")
            raise
        if owner and audience != owner["audience"]:
            await self.fence_owner(owner, "Discord destination audience changed")
            raise OwnershipError("destination audience changed")
        return audience

    async def inspect_workers(self):
        for owner in self.ledger.conversations():
            if not owner["identity"] or owner["status"] in ("queued", "stopped"):
                continue
            status = await self.backend.status(WorkerIdentity(**owner["identity"]))
            current = self.ledger.get(owner["id"])
            if current["generation"] != owner["generation"] or current["identity"] != owner["identity"]:
                continue
            owner = current
            if status.terminated:
                graceful = owner["status"] == "draining" and status.phase == "Succeeded"
                state = "stopped" if graceful else "recovery_required"
                if owner["status"] != state:
                    self.ledger.transition(owner["id"], owner["generation"], state,
                                           detail="container termination verified; saved state retained")
                    self.knowledge.revoke_worker(owner["id"], owner["generation"])
                    await self.connector.notice(owner, "Worker stopped; its files and learned state are retained." if graceful else
                                                "This worker was interrupted. Its files are retained. Inspect uncertain actions before resuming; no prior request will be replayed automatically.")
            elif status.phase not in ("Running", "Pending") and owner["status"] not in ("unavailable", "recovery_required"):
                self.ledger.transition(owner["id"], owner["generation"], "unavailable",
                                       detail=f"worker unavailable: {status.phase}; no replacement authorized")
                await self.connector.notice(owner, "This conversation's node or worker is unavailable. Its state stays on that node; no empty replacement will be started.")

    async def connect_owner(self, connection):
        owner = self.ledger.get(connection.owner["id"])
        for _ in range(100):
            if owner["identity"] and owner["identity"]["generation"] == owner["generation"]:
                break
            await asyncio.sleep(0.1)
            owner = self.ledger.get(owner["id"])
        async with self.operations:
            owner = self.ledger.get(connection.owner["id"])
            if (not owner or owner["generation"] != connection.owner["generation"] or not owner["identity"]
                    or owner["identity"]["generation"] != owner["generation"]):
                raise OwnershipError("worker has no matching recorded Pod generation")
            if owner["status"] not in ("provisioning", "unavailable", "ready"):
                raise OwnershipError("worker is draining, stopped or requires reconciliation")
            if owner["id"] in self.connections:
                raise OwnershipError("another connection owns this worker")
            status = await self.backend.status(WorkerIdentity(**owner["identity"]))
            if status.phase != "Running" or status.terminated:
                raise OwnershipError("recorded Pod is not a running owner")
            await self.authorized_audience(owner, owner["source"])
            current = self.ledger.get(owner["id"])
            if current["generation"] != owner["generation"] or current["status"] not in ("provisioning", "unavailable", "ready"):
                raise OwnershipError("worker was fenced during reconnect")
            self.connections[owner["id"]] = connection
            self.ledger.transition(owner["id"], owner["generation"], "ready", detail="authenticated retained worker connected")

    async def disconnect_owner(self, connection):
        cid = connection.owner["id"]
        if self.connections.get(cid) is not connection:
            return
        del self.connections[cid]
        owner = self.ledger.get(cid)
        if owner["generation"] == connection.owner["generation"] and owner["status"] == "ready":
            self.ledger.transition(cid, owner["generation"], "unavailable", detail="Relay disconnected; same owner may reconnect")

    async def route(self, event):
        cid, native_key, source = routing_key(self.config.get("agent_id", "timmy"), event)
        owner = self.ledger.get(cid)
        audience = await self.authorized_audience(owner, event)
        if event.text.strip().split(maxsplit=1)[0:1] == ["/cluster"]:
            return await self.cluster_command(owner, event)
        try:
            payload = to_wire(event, cid, self.media, self.config["relay_url"])
            owner, added = self.ledger.admit(agent_id=self.config.get("agent_id", "timmy"), native_key=native_key,
                                            source=source, audience=audience, event_id=event_identifier(event),
                                            payload=payload, actor=str(source["user_id"]))
        except (AdmissionError, OwnershipError, ValueError) as exc:
            await self.connector.notice(owner or {"source": source, "audience": audience, "id": cid, "generation": 0}, str(exc))
            return
        if not added:
            return
        self.ledger.remember_message(str(event.message_id or getattr(event.raw_message, "id", "")), cid, owner["generation"], "user")
        if owner["status"] == "queued":
            await self.connector.notice(owner, "Conversation queued. A complete worker will start when a retained worker slot is available.")
        elif owner["status"] in ("stopped", "unavailable", "recovery_required"):
            await self.connector.notice(owner, "Your follow-up is saved. This worker is unavailable or stopped; use /cluster status to inspect it before resuming.")
        self.wake.set()

    async def prompt_response(self, prompt_id, option_id, interaction):
        row = self.ledger.prompt_owner(prompt_id)
        await self.authorized_audience(row, row["source"])
        self.ledger.consume_prompt(prompt_id, actor=str(interaction.user.id),
                                   channel_id=str(interaction.channel_id), message_id=str(interaction.message.id),
                                   option=str(option_id), event_id=f"discord-control:{interaction.id}")
        self.wake.set()

    async def cluster_command(self, owner, event):
        if not owner:
            return  # No worker or state to control yet.
        if str(event.source.user_id) != owner["actor"]:
            raise OwnershipError("this control belongs to another audience or owner")
        await self.authorized_audience(owner, event)
        parts = event.text.strip().split(maxsplit=2)
        command = parts[1].lower() if len(parts) > 1 else ""
        if command == "status":
            summary = self.ledger.summary()
            await self.connector.notice(owner, f'Worker {owner["id"]}, generation {owner["generation"]}: {owner["status"]}. '
                                        f'Queue: {summary["queued_messages"]} messages; worker limit: {summary["max_workers"]}. {owner["detail"] or ""}')
        elif command == "park":
            await self.park(owner["id"])
        elif command == "resume":
            note = parts[2] if len(parts) > 2 else ""
            try:
                await self.recover(owner["id"], note)
            except (ValueError, OwnershipError) as exc:
                await self.connector.notice(owner, str(exc))
        else:
            await self.connector.notice(owner, "Use /cluster status, /cluster park, or /cluster resume followed by how you checked any uncertain actions.")

    async def park(self, cid):
        owner = self.ledger.get(cid)
        connection = self.connections.get(cid)
        if not connection or owner["status"] != "ready":
            raise OwnershipError("a connected worker is required for graceful park")
        self.ledger.transition(cid, owner["generation"], "draining", detail="owner requested graceful park")
        await connection.send({"type": "inbound", "event": {"text": "/cluster-stop-worker", "message_type": "command",
                              "source": owner["source"], "message_id": f'park:{cid}:{owner["generation"]}'}})
        self.ledger.record(cid, "park_requested", {"generation": owner["generation"]})

    async def recover(self, cid, note):
        async with self.operations:
            owner = self.ledger.get(cid)
            if not owner or not owner["identity"]:
                raise OwnershipError("no retained Pod identity is available for recovery")
            if cid in self.connections:
                raise OwnershipError("old owner is still connected")
            if await self.connector.audience(owner["source"]) != owner["audience"]:
                raise OwnershipError("audience changed; restore its permissions or start a fresh conversation")
            status = await self.backend.status(WorkerIdentity(**owner["identity"]))
            if not status.terminated:
                raise OwnershipError("old owner termination has not been proved; node absence is not a fence")
            self.ledger.prepare_recovery(cid, owner["generation"], note)
            self.knowledge.revoke_worker(cid, owner["generation"])
        self.wake.set()

    async def fence_owner(self, owner, reason):
        """Revoke shared access before permitting any later tool or delivery result.

        The shutdown request is best effort, never proof of process termination.
        Recovery still requires the recorded Pod's terminated state.
        """
        current = self.ledger.get(owner["id"])
        if not current or current["generation"] != owner["generation"]:
            return
        self.ledger.transition(owner["id"], owner["generation"], "recovery_required", detail=reason)
        self.knowledge.revoke_worker(owner["id"], owner["generation"])
        connection = self.connections.get(owner["id"])
        if connection:
            try:
                await connection.send({"type": "inbound", "event": {"text": "/cluster-stop-worker", "message_type": "command",
                                      "source": owner["source"], "message_id": f'fence:{owner["id"]}:{owner["generation"]}'}})
            except Exception:
                pass
        self.ledger.record(owner["id"], "owner_fenced", {"generation": owner["generation"], "reason": reason})

    def healthy(self):
        return not self.stopping and self.connector.connected and bool(self.task and not self.task.done()) and time.monotonic() - self.last_tick < 60
