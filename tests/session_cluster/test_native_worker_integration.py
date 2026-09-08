"""Real worker process, Relay socket, authority HTTP and native Discord boundary.

Only Discord's network and Kubernetes scheduling are replaced. The model speaks
OpenAI HTTP locally; Hermes executes its real terminal/approval/history paths.
"""
import asyncio
import json
import os
from pathlib import Path
import socket
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

from fastapi import Request
from fastapi.responses import StreamingResponse
import pytest
import uvicorn
import yaml

from hermes_cluster.app import create_app
from hermes_cluster.controller import Controller
from hermes_cluster.knowledge import KnowledgeStore
from hermes_cluster.kubernetes import WorkerIdentity, WorkerStatus
from hermes_cluster.discord_connector import PromptView
from test_discord_connector import fixture as discord_fixture


class ProcessBackend:
    """A Pod stand-in that preserves the production process/config boundary."""
    def __init__(self, root, native_config):
        self.root, self.native_config = root, native_config
        self.credentials, self.processes, self.logs, self.configs = {}, {}, {}, {}

    async def create_credentials(self, name, values):
        self.credentials[name] = values

    async def create_worker(self, cid, generation, config, secret_name, **kwargs):
        root = self.root / cid
        root.mkdir(parents=True, exist_ok=True)
        native = root / "hermes.yaml"
        native.write_text(yaml.safe_dump(self.native_config))
        config = {**config, "hermes_home": str(root / "home"), "workspace": str(root / "workspace"),
                  "config_source": str(native), "secret_files": {}}
        for name, value in self.credentials[secret_name].items():
            path = root / name
            path.write_text(value)
            path.chmod(0o600)
            config["secret_files"][name] = str(path)
        path = root / "worker.json"
        path.write_text(json.dumps(config))
        self.configs[cid] = config
        env = {k: v for k, v in os.environ.items() if not k.startswith(("HERMES_", "GATEWAY_", "DISCORD_"))}
        # Private OS home prevents unrelated CLI OAuth/profile discovery.
        env["HOME"] = str(root)
        repo = Path(__file__).resolve().parents[2]
        env["PYTHONPATH"] = os.pathsep.join([str(repo), str(repo / "contrib/session-cluster")])
        log = (root / "worker.log").open("w")
        self.logs[cid] = log
        self.processes[cid] = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "hermes_cluster.worker", "--config", str(path),
            cwd=repo, env=env, stdout=log, stderr=asyncio.subprocess.STDOUT)
        return WorkerIdentity(cid, generation, "pod", "pod-uid", "claim", "config", secret_name, "claim-uid")

    async def status(self, identity):
        proc = self.processes[identity.conversation_id]
        done = proc.returncode is not None
        return WorkerStatus("Succeeded" if proc.returncode == 0 else "Failed" if done else "Running", terminated=done)

    async def aclose(self):
        for proc in self.processes.values():
            if proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), 15)
                except asyncio.TimeoutError:
                    proc.kill()
                    await proc.wait()
        for stream in self.logs.values():
            stream.close()

    def diagnostics(self):
        return "\n".join(path.read_text()[-12000:] for path in self.root.glob("*/worker.log")) + str(getattr(self, "trace", lambda: "")())


async def eventually(predicate, backend, timeout=35):
    end = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < end:
        result = predicate()
        if result:
            return result
        await asyncio.sleep(0.05)
    raise AssertionError("integration condition timed out\n" + backend.diagnostics())


@pytest.mark.asyncio
async def test_native_worker_approval_reply_history_and_park(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "local-fixture-provider-key")
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    native = {"model": {"default": "local-fixture", "provider": "custom", "base_url": base + "/model/v1"},
              "streaming": {"enabled": False}, "approvals": {"mode": "manual"},
              "agent": {"max_turns": 4}, "terminal": {"timeout": 10}}
    backend = ProcessBackend(tmp_path / "workers", native)
    knowledge = KnowledgeStore(tmp_path / "knowledge.sqlite")
    connector, source, parent, thread, actor, everyone = discord_fixture(tmp_path / "discord")
    sent = []
    backend.trace = lambda: [{"id": m["id"], "content": m.get("content")} for m in sent]
    async def send(**kwargs):
        ident = 100 + len(sent)
        sent.append({"id": ident, **kwargs})
        return SimpleNamespace(id=ident, attachments=[])
    thread.send.side_effect = send
    thread.typing = AsyncMock()
    connector.connect = AsyncMock(return_value=True)
    connector.disconnect = AsyncMock()
    # Keep real connector logic, replacing only its constructed Discord socket.
    def factory(config, route, prompt_response, *, ledger, media, relay_url, fence_owner=None):
        connector.route, connector.prompt_response = route, prompt_response
        connector.ledger, connector.media, connector.media_base = ledger, media, relay_url.rstrip("/")
        connector.fence_owner = fence_owner
        return connector
    config = {"guild_id": "1", "bot_id": "2", "allowed_user_ids": ["3"], "allowed_channel_ids": ["10"],
              "agent_id": "timmy", "data_dir": str(tmp_path / "controller"), "relay_url": base,
              "native_config": native, "max_workers": 1, "personality": "Run the requested fixture tool then report its result."}
    controller = Controller(config, backend, knowledge, connector_factory=factory)
    app = create_app(controller)
    model_requests = []
    @app.post("/model/v1/chat/completions")
    async def model(request: Request):
        body = await request.json()
        model_requests.append(body)
        last_user = max(i for i, m in enumerate(body["messages"]) if m.get("role") == "user")
        user_text = str(body["messages"][last_user].get("content", ""))
        tools_done = any(m.get("role") == "tool" for m in body["messages"][last_user + 1:])
        if "[IMPORTANT:" in user_text and "async-fixture-complete" in user_text:
            message = {"role": "assistant", "content": "Autonomous fixture completion received in its owning conversation."}
            finish = "stop"
        elif "Run cancellation fixture" in user_text and not tools_done:
            message = {"role": "assistant", "content": None, "tool_calls": [
                {"id": "cancel-fixture-call", "type": "function", "function": {"name": "terminal",
                 "arguments": json.dumps({"command": "mkdir -p cancelled-fixture && rm -rf cancelled-fixture && printf cancellation-ran > cancellation-ran.txt"})}}]}
            finish = "tool_calls"
        elif "Start the background fixture" in user_text and tools_done:
            message = {"role": "assistant", "content": "Background fixture started; completion will arrive here."}
            finish = "stop"
        elif "Start the background fixture" in user_text:
            message = {"role": "assistant", "content": None, "tool_calls": [
                {"id": "async-fixture-call", "type": "function", "function": {"name": "terminal",
                 "arguments": json.dumps({"command": "sleep 0.2; printf async-fixture-complete", "background": True, "notify": True})}}]}
            finish = "tool_calls"
        elif tools_done:
            message = {"role": "assistant", "content": "Fixture approved and executed; native history is retained."}
            finish = "stop"
        else:
            message = {"role": "assistant", "content": None, "tool_calls": [
                {"id": "fixture-call", "type": "function", "function": {"name": "terminal",
                 "arguments": json.dumps({"command": "mkdir -p approval-fixture && rm -rf approval-fixture && printf fixture-complete"})}}]}
            finish = "tool_calls"
        result = {"id": "fixture-response", "object": "chat.completion", "created": 1, "model": "local-fixture",
                "choices": [{"index": 0, "message": message, "finish_reason": finish}],
                "usage": {"prompt_tokens": 100, "completion_tokens": 20, "total_tokens": 120}}
        if body.get("stream"):
            delta = dict(message)
            if delta.get("tool_calls"):
                delta["tool_calls"] = [{"index": i, **call} for i, call in enumerate(delta["tool_calls"])]
            result["object"] = "chat.completion.chunk"
            result["choices"] = [{"index": 0, "delta": delta, "finish_reason": finish}]
            return StreamingResponse(iter(["data: " + json.dumps(result) + "\n\n", "data: [DONE]\n\n"]),
                                     media_type="text/event-stream")
        return result
    server = uvicorn.Server(uvicorn.Config(app, log_level="warning", lifespan="on"))
    serving = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        await eventually(lambda: server.started, backend)
        interaction = SimpleNamespace(channel=thread, channel_id=11, guild_id=1, guild=thread.guild,
            user=actor, id=90, response=SimpleNamespace(send_message=AsyncMock(), defer=AsyncMock()),
            followup=SimpleNamespace(send=AsyncMock()))
        event = connector.adapter._build_slash_event(interaction, "Run the approved local fixture.")
        await connector.adapter.handle_message(event)
        owner = controller.ledger.conversations()[0]
        assert owner["source"]["thread_id"] == "11"
        assert owner["status"] == "queued"
        prompt = await eventually(lambda: next(iter(controller.ledger.pending_prompts()), None), backend)
        assert model_requests and len(backend.processes) == 1
        assert not any("Fixture approved and executed" in m.get("content", "") for m in sent)
        interaction.id = 91
        steer = connector.adapter._build_slash_event(interaction, "/steer Include steer-fixture-marker after the tool.")
        await connector.adapter.handle_message(steer)
        view = PromptView(connector, prompt["id"], prompt["options"])
        interaction.message = SimpleNamespace(id=int(prompt["message_id"]))
        interaction.id = 92
        # Choose native "Allow once", preserving the real callback round trip.
        choice = next(child for child in view.children if "once" in child.label.lower())
        await choice.callback(interaction)
        interaction.response.defer.assert_awaited_once()
        await eventually(lambda: any("Fixture approved and executed" in m.get("content", "") for m in sent), backend)
        assert "fixture-complete" in json.dumps(model_requests[-1]["messages"])
        assert "steer-fixture-marker" in json.dumps(model_requests[-1]["messages"])
        interaction.id = 93
        event = connector.adapter._build_slash_event(interaction, "Start the background fixture.")
        await connector.adapter.handle_message(event)
        await eventually(lambda: any("Autonomous fixture completion received" in m.get("content", "") for m in sent), backend)
        assert len(backend.processes) == 1
        interaction.id = 94
        event = connector.adapter._build_slash_event(interaction, "Run cancellation fixture.")
        await connector.adapter.handle_message(event)
        await eventually(lambda: next(iter(controller.ledger.pending_prompts()), None), backend)
        sent_before_stop = len(sent)
        interaction.id = 95
        event = connector.adapter._build_slash_event(interaction, "/stop")
        await connector.adapter.handle_message(event)
        await eventually(lambda: any("stopped" in m.get("content", "").lower() for m in sent[sent_before_stop:]), backend)
        assert controller.ledger.get(owner["id"])["status"] == "ready"
        assert not Path(backend.configs[owner["id"]]["workspace"], "cancellation-ran.txt").exists()
        event = connector.adapter._build_slash_event(interaction, "/cluster park")
        interaction.id = 96
        await connector.adapter.handle_message(event)
        proc = backend.processes[owner["id"]]
        await eventually(lambda: proc.returncode is not None, backend)
        assert proc.returncode == 0, backend.diagnostics()
        await controller.inspect_workers()
        assert controller.ledger.get(owner["id"])["status"] == "stopped"
        with knowledge.transaction() as db:
            rows = [json.loads(row[0]) for row in db.execute("SELECT payload FROM history")]
        assert any("Fixture approved and executed" in str(row) for row in rows)
        assert any("fixture-complete" in str(row) for row in rows)
        assert any("Autonomous fixture completion received" in str(row) for row in rows)
        assert not any("No home channel is set" in m.get("content", "") for m in sent)
    finally:
        await backend.aclose()
        server.should_exit = True
        await asyncio.wait_for(serving, 20)
        sock.close()
