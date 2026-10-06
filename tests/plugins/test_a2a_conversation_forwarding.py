"""Served profiles use authenticated peer policy before reaching hermes chat."""

from types import SimpleNamespace
from unittest.mock import Mock

from gateway.config import GatewayConfig, PlatformConfig
from plugins.platforms.a2a import adapter as a2a, protocol


def test_restricted_profile_forwarding_rejects_before_subprocess(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("A2A_PEER_TOKENS", "alfred:test-a,gromit:test-g")
    monkeypatch.delenv("A2A_BEARER_TOKEN", raising=False)
    adapter = a2a.A2AAdapter(PlatformConfig(enabled=True))
    config = GatewayConfig.from_dict({"gateway": {"a2a_conversation_only_peers": ["alfred"]}})
    adapter.set_session_store(SimpleNamespace(config=config))
    process = Mock(return_value=SimpleNamespace(returncode=0, stdout="hello", stderr=""))
    monkeypatch.setattr(a2a.subprocess, "run", process)
    monkeypatch.setattr(a2a, "_state_db", lambda *args, **kwargs: "")
    monkeypatch.setattr(a2a, "_profile_home", lambda _: str(tmp_path / "other"))
    route = {"profile": "other", "slug": "other", "local": False}
    peer = adapter._security_context.authenticate("Bearer test-a", "127.0.0.1")
    reply, state = adapter._forward_to_profile(route, peer, "gromit-context", "hi")
    assert state == protocol.STATE_REJECTED
    process.assert_not_called()
    # A caller's metadata cannot claim Gromit's identity or opt out of the policy.
    task, pending = adapter._prepare_task(
        {"message": {"role": "user", "parts": [{"kind": "text", "text": "hi"}],
                     "contextId": "gromit-context"},
         "metadata": {"peer": "gromit", "conversation_only": False}}, peer, route,
    )
    assert pending is None and task["status"]["state"] == protocol.STATE_REJECTED
    process.assert_not_called()

    peer = adapter._security_context.authenticate("Bearer test-g", "127.0.0.1")
    assert adapter._forward_to_profile(route, peer, "ctx/a", "hi")[1] == protocol.STATE_COMPLETED
    assert process.call_count == 1
    adapter._forward_to_profile(route, peer, "ctx-a", "hi")
    adapter._forward_to_profile(route, "another", "ctx/a", "hi")
    assert len(adapter._profile_session_locks) == 3
    assert all("--resume" not in call.args[0] for call in process.call_args_list)


def test_authenticated_context_collision_cannot_misdeliver_replies(tmp_path, monkeypatch):
    import asyncio
    from gateway.session import SessionStore

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("A2A_PEER_TOKENS", "alfred:test-a,gromit:test-g")
    monkeypatch.delenv("A2A_BEARER_TOKEN", raising=False)
    config = GatewayConfig.from_dict({"gateway": {"a2a_conversation_only_peers": ["alfred"]}})
    store = SessionStore(tmp_path / "sessions", config)
    adapter = a2a.A2AAdapter(PlatformConfig(enabled=True))
    adapter.set_session_store(store)
    adapter._loop = object()
    adapter._message_handler = object()
    events = []

    async def receive(event):
        events.append(event)

    monkeypatch.setattr(adapter, "handle_message", receive)
    monkeypatch.setattr(a2a.asyncio, "run_coroutine_threadsafe", lambda coro, loop: asyncio.run(coro))
    pending = []
    for token in ("test-a", "test-g"):
        peer = adapter._security_context.authenticate(f"Bearer {token}", "127.0.0.1")
        task, waiter = adapter._prepare_task(
            {"message": {"role": "user", "contextId": "same/context",
                         "parts": [{"text": "hi"}]}, "metadata": {"peer": "gromit"}}, peer,
        )
        assert task is None
        pending.append(waiter)
    assert events[0].source.chat_id != events[1].source.chat_id
    assert store.get_or_create_session(events[0].source).session_id != store.get_or_create_session(events[1].source).session_id
    assert all(waiter["context_id"] == "same/context" for waiter in pending)
    asyncio.run(adapter.send(events[1].source.chat_id, "GROMIT_PRIVATE", metadata={"notify": True}))
    assert not pending[0]["future"].done()
    assert pending[1]["future"].result()[1] == "GROMIT_PRIVATE"
    asyncio.run(adapter.send(events[0].source.chat_id, "Hello Alfred", metadata={"notify": True}))
    assert pending[0]["future"].result()[1] == "Hello Alfred"
    # A policy change also starts a distinct delivery and audit history context.
    old_context = adapter._history_context_id("gromit", "same/context")
    config.a2a_conversation_only_peers.append("gromit")
    assert adapter._history_context_id("gromit", "same/context") != old_context
    store.close_all_db_handles()
