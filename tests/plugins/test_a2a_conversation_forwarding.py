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


def test_forwarded_context_key_has_fixed_size_and_preserves_identity(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    adapter = a2a.A2AAdapter(PlatformConfig(enabled=True))
    adapter.set_session_store(SimpleNamespace(config=GatewayConfig()))
    monkeypatch.setattr(a2a, "_profile_home", lambda _: str(tmp_path))
    titles = []
    def state(profile, sql, params, *args, **kwargs):
        if sql.startswith("SELECT id FROM sessions WHERE title"):
            titles.append(params[0])
        return ""
    monkeypatch.setattr(a2a, "_state_db", state)
    monkeypatch.setattr(a2a.subprocess, "run", Mock(return_value=SimpleNamespace(returncode=0, stdout="ok")))
    route = {"profile": "other", "slug": "other", "local": False}
    for peer, context in (("gromit", "x" * 900_000), ("gromit", "x" * 900_000 + "y"), ("other", "x" * 900_000)):
        assert adapter._forward_to_profile(route, peer, context, "hello")[1] == protocol.STATE_COMPLETED
    assert len(set(titles)) == 3
    assert all(len(title) < 100 for title in titles)
    assert all(len(key[2]) == 64 for key in adapter._profile_session_locks)


def test_local_history_uses_protocol_context_id_and_refuses_ambiguous_scopes(tmp_path, monkeypatch):
    from plugins.platforms.a2a.tools import a2a_history, a2a_list
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    adapter = a2a.A2AAdapter(PlatformConfig(enabled=True))
    config = GatewayConfig(a2a_conversation_only_peers=["alfred"])
    adapter.set_session_store(SimpleNamespace(config=config))
    context_id = "shared:context/☃"
    protocol.persist_message(adapter._history_context_id("alfred", context_id), "user", "ALFRED")
    assert "ALFRED" in a2a_history({"context_id": context_id})
    assert context_id in a2a_list()
    protocol.persist_message(adapter._history_context_id("gromit", context_id), "user", "GROMIT")
    assert "Ambiguous" in a2a_history({"context_id": context_id})
    assert "GROMIT" not in a2a_history({"context_id": context_id, "peer": "alfred"})
    config.a2a_conversation_only_peers.clear()
    protocol.persist_message(adapter._history_context_id("alfred", context_id), "user", "NEW_POLICY")
    assert "Ambiguous" in a2a_history({"context_id": context_id, "peer": "alfred"})
    assert "NEW_POLICY" not in a2a_history({"context_id": context_id, "peer": "alfred", "policy_mode": "restricted"})
