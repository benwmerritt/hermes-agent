"""Authenticated Relay connections bound to one retained worker generation."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from .ledger import OwnershipError


@dataclass
class Connection:
    socket: WebSocket
    owner: dict
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def send(self, frame: dict):
        async with self.lock:
            await self.socket.send_text(json.dumps(frame, separators=(",", ":")) + "\n")


def create_relay_router(controller):
    router = APIRouter()

    @router.websocket("/relay")
    async def relay(socket: WebSocket):
        try:
            owner = controller.ledger.authenticate(socket.headers.get("authorization", ""))
        except OwnershipError:
            await socket.close(code=4401, reason="unauthorized")
            return
        cid = owner["id"]
        if cid in controller.connections:
            await socket.close(code=4409, reason="owner already connected")
            return
        await socket.accept()
        connection = Connection(socket, owner)
        registered = False
        try:
            text = await asyncio.wait_for(socket.receive_text(), 15)
            if len(text) > 262144:
                raise OwnershipError("oversized handshake")
            hello = json.loads(text.strip())
            if hello.get("type") != "hello" or hello.get("platform") != "discord" or str(hello.get("botId")) != str(controller.config["bot_id"]):
                raise OwnershipError("unexpected Relay identity")
            await controller.connect_owner(connection)
            registered = True
            await connection.send({"type": "descriptor", "descriptor": {
                "contract_version": 1, "platform": "discord", "label": "Discord",
                "max_message_length": 2000, "supports_draft_streaming": False,
                "supports_edit": True, "supports_threads": False, "markdown_dialect": "discord",
                "len_unit": "chars", "supports_context": False,
                "supported_ops": ["send", "edit", "typing", "get_chat_info", "send_media", "prompt", "react"],
            }})
            controller.wake.set()
            while True:
                text = await socket.receive_text()
                if len(text) > 1024 * 1024:
                    raise OwnershipError("oversized Relay frame")
                for line in text.splitlines():
                    if not line.strip():
                        continue
                    frame = json.loads(line)
                    current = controller.ledger.get(cid)
                    if current["generation"] != owner["generation"] or current["status"] == "stopped":
                        raise OwnershipError("worker generation revoked")
                    kind = frame.get("type")
                    if kind == "inbound_ack":
                        controller.ledger.acknowledge(str(frame.get("bufferId", "")), cid, owner["generation"])
                    elif kind == "going_idle":
                        controller.ledger.transition(cid, owner["generation"], "draining", detail="worker requested graceful drain")
                        await connection.send({"type": "going_idle_ack"})
                    elif kind == "outbound":
                        if frame.get("platform", "discord") != "discord" or str(frame.get("botId", controller.config["bot_id"])) != str(controller.config["bot_id"]):
                            raise OwnershipError("outbound identity mismatch")
                        request_id, action = frame.get("requestId"), frame.get("action")
                        if not isinstance(request_id, str) or len(request_id) > 100 or not isinstance(action, dict):
                            raise OwnershipError("invalid outbound request")
                        worker_id = f'{cid}:{owner["generation"]}'
                        result = controller.ledger.begin_outbound(worker_id, request_id, action)
                        if result is None:
                            try:
                                result = await controller.connector.dispatch(current, action)
                            except OwnershipError:
                                result = {"success": False, "error": "destination authorization refused", "code": "forbidden"}
                            except Exception:
                                # The platform may have accepted the action before a timeout.
                                # Never translate uncertainty into a safe-to-retry failure.
                                result = {"success": False, "error": "platform result uncertain; inspect before retry", "ambiguous": True}
                            controller.ledger.finish_outbound(worker_id, request_id, result)
                        await connection.send({"type": "outbound_result", "requestId": request_id, "result": result})
                    elif kind == "interrupt":
                        if frame.get("session_key") != owner["native_key"]:
                            raise OwnershipError("interrupt belongs to another conversation")
                        controller.ledger.record(cid, "native_interrupt", {"generation": owner["generation"]})
                    else:
                        raise OwnershipError("unsupported Relay frame")
        except (WebSocketDisconnect, asyncio.TimeoutError):
            pass
        except (OwnershipError, ValueError, TypeError):
            await socket.close(code=4403, reason="protocol or ownership rejected")
        finally:
            if registered:
                await controller.disconnect_owner(connection)

    return router
