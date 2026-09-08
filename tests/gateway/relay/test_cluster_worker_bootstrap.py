"""Retained-worker ownership and native imports use the prepared private home."""
import json
import os
from pathlib import Path
import subprocess
import sys


def test_private_bootstrap_and_native_conversation_guard(tmp_path):
    home = tmp_path / "home"
    workspace = tmp_path / "workspace"
    yaml_path = tmp_path / "hermes.yaml"
    yaml_path.write_text("model:\n  default: test-model\n  provider: custom\n  base_url: http://127.0.0.1:1/v1\n")
    source = {"platform": "discord", "chat_id": "thread", "chat_type": "thread",
              "thread_id": "thread", "scope_id": "guild", "parent_chat_id": "channel",
              "user_id": "owner"}
    config = {"schema_version": 1, "worker_id": "worker", "generation": 1,
              "conversation_key": "agent:main:discord:thread:thread:thread", "source": source,
              "allowed_user_ids": ["owner"], "hermes_home": str(home), "workspace": str(workspace),
              "config_source": str(yaml_path), "relay": {"url": "ws://127.0.0.1:1/relay", "bot_id": "bot"},
              "knowledge": {"url": "http://127.0.0.1:1"}}
    path = tmp_path / "worker.json"
    path.write_text(json.dumps(config))
    env = dict(os.environ, HERMES_HOME=str(home), GATEWAY_RELAY_SECRET="test-relay-secret",
               HERMES_CLUSTER_KNOWLEDGE_TOKEN="test-knowledge-secret", OPENAI_API_KEY="test-provider-key")
    package_root = Path(__file__).resolve().parents[3] / "contrib" / "session-cluster"
    env["PYTHONPATH"] = str(package_root) + os.pathsep + env.get("PYTHONPATH", "")
    code = r'''
import asyncio
from pathlib import Path
import sys
from hermes_cluster.worker_config import read_config, claim_home, prepare_environment, install_runtime_config
config = read_config(sys.argv[1])
with claim_home(config):
    try:
        with claim_home(config):
            raise AssertionError("duplicate owner acquired lock")
    except RuntimeError:
        pass
    prepare_environment(config)
    install_runtime_config(config)
    from hermes_constants import get_hermes_home
    assert get_hermes_home() == Path(config["hermes_home"])
    from hermes_cluster.worker_runtime import ConversationPolicy, ConversationGateway, ConversationRelayAdapter
    from gateway.config import Platform, PlatformConfig
    from gateway.session import SessionSource
    from gateway.relay.descriptor import CapabilityDescriptor
    from gateway.platforms.event import MessageEvent
    policy = ConversationPolicy(config)
    assert policy.accepts(SessionSource.from_dict(config["source"]))
    for key, value in (("scope_id", "foreign-guild"), ("thread_id", "foreign-thread"), ("user_id", "stranger")):
        source = SessionSource.from_dict({**config["source"], key: value})
        assert not policy.accepts(source)
    runner = ConversationGateway(policy)
    assert list(runner.config.platforms) == [Platform.RELAY]
    assert runner._schedule_resume_pending_sessions() == 0
    assert not runner.request_restart()
    assert Path(config["hermes_home"], "state.db").exists()
    from unittest.mock import AsyncMock, patch
    async def check_first_contact_notes():
        notes = []
        with patch.object(runner, "_deliver_platform_notice", new_callable=AsyncMock) as notice:
            await runner._hmwa_first_contact_notes(policy.source, [], notes)
            notice.assert_not_awaited()
        assert notes, "native first-contact onboarding should remain available"
        assert policy.source.platform == Platform.DISCORD
    asyncio.run(check_first_contact_notes())
    descriptor = CapabilityDescriptor(contract_version=1, platform="discord", label="Discord",
        max_message_length=2000, supports_draft_streaming=False, supports_edit=True,
        supports_threads=True, markdown_dialect="discord", len_unit="chars")
    adapter = ConversationRelayAdapter(PlatformConfig(), descriptor, None, policy)
    prompt_id = adapter._mint_prompt("exec_approval", {"session_key": policy.key})
    foreign = SessionSource.from_dict({**config["source"], "user_id": "stranger"})
    event = MessageEvent(text="", source=foreign, prompt_response={"prompt_id": prompt_id, "option_id": "once"})
    try:
        asyncio.run(adapter._on_inbound(event))
        raise AssertionError("foreign actor reached approval resolution")
    except PermissionError:
        pass
    assert prompt_id in adapter._pending_prompts
    assert runner._session_db is not None
    from gateway import delivery_ledger as dl
    dl.record_obligation(obligation_id="uncertain-final", session_key=policy.key,
        platform="relay", chat_id="thread", thread_id="thread", content="retained complete answer")
    dl.mark_attempting("uncertain-final")
    with dl._connect() as db:
        db.execute("UPDATE delivery_obligations SET owner_pid=999999999, owner_started_at=1")
    import json
    import os
    import websockets
    async def exercise_native_start():
        async def connector(ws):
            async for raw in ws:
                for line in raw.splitlines():
                    frame = json.loads(line)
                    if frame["type"] == "hello":
                        await ws.send(json.dumps({"type": "descriptor", "descriptor": json.loads(descriptor.to_json())}) + "\n")
                    elif frame["type"] == "outbound":
                        await ws.send(json.dumps({"type": "outbound_result", "requestId": frame["requestId"],
                            "result": {"success": True, "message_id": "reply"}}) + "\n")
        async with websockets.serve(connector, "127.0.0.1", 0) as server:
            config["relay"]["url"] = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}/relay"
            os.environ["GATEWAY_RELAY_URL"] = config["relay"]["url"]
            install_runtime_config(config)
            runner.config.platforms[Platform.RELAY].extra["relay_url"] = config["relay"]["url"]
            try:
                assert await asyncio.wait_for(runner.start(), timeout=20)
                assert runner._running
                assert isinstance(runner.adapters[Platform.RELAY], ConversationRelayAdapter)
                assert runner.adapters[Platform.RELAY].is_connected
                assert await runner._redeliver_failed_obligations_for_platform(Platform.RELAY) == 0
                with dl._connect() as db:
                    row = db.execute("SELECT state, attempts, content FROM delivery_obligations WHERE obligation_id='uncertain-final'").fetchone()
                assert tuple(row) == ("attempting", 0, "retained complete answer"), row
            finally:
                await asyncio.wait_for(runner.stop(), timeout=20)
    asyncio.run(exercise_native_start())
'''
    result = subprocess.run([sys.executable, "-c", code, str(path)], env=env, capture_output=True, text=True, timeout=45)
    assert result.returncode == 0, result.stdout + result.stderr
    saved = json.loads((home / "cluster-worker-identity.json").read_text())
    assert saved["conversation_key"] == config["conversation_key"]


def test_retained_home_rejects_changed_owner_and_stale_generation(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[3] / "contrib" / "session-cluster"))
    from hermes_cluster.worker_config import claim_home
    import pytest
    config = {"hermes_home": str(tmp_path), "worker_id": "a", "generation": 2,
              "conversation_key": "thread-a", "source": {"chat_id": "a"}}
    with claim_home(config):
        pass
    for changes in ({"worker_id": "b"}, {"generation": 1}, {"source": {"chat_id": "b"}}):
        with pytest.raises(ValueError):
            with claim_home({**config, **changes}):
                pass
