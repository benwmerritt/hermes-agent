"""Authenticated peer and policy routes must survive cache loss without sharing history."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.session import SessionSource, SessionStore


def test_authenticated_peers_and_mode_changes_never_recover_other_history(tmp_path, monkeypatch):
    from plugins.platforms.a2a.adapter import A2AAdapter
    from gateway.platforms.event import MessageEvent, MessageType
    from gateway.run import GatewayRunner
    from agent.conversation_loop import _restore_or_build_system_prompt

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("A2A_PEER_TOKENS", "gromit:test-g,alfred:test-a")
    monkeypatch.delenv("A2A_BEARER_TOKEN", raising=False)
    config = GatewayConfig.from_dict({"gateway": {"a2a_conversation_only_peers": []}})
    store = SessionStore(tmp_path / "sessions", config)
    adapter = A2AAdapter(PlatformConfig(enabled=True))
    adapter.set_session_store(store)
    runner = object.__new__(GatewayRunner)
    runner.config, runner.session_store = config, store

    def source(token, context="shared:context/☃"):
        peer = adapter._security_context.authenticate(f"Bearer {token}", "127.0.0.1")
        assert peer is not None
        return adapter.build_source(chat_id=context, chat_type="dm", user_id=peer)

    gromit, alfred = source("test-g"), source("test-a")
    legacy_key = f"agent:main:a2a:dm:{gromit.chat_id}"
    db = store._db
    db.create_session(session_id="legacy-gromit", source="a2a", user_id="gromit")
    db.record_gateway_session_peer("legacy-gromit", source="a2a", user_id="gromit",
                                   session_key=legacy_key, chat_id=gromit.chat_id, chat_type="dm")
    store.append_to_transcript("legacy-gromit", {"role": "user", "content": "LEGACY_PRIVATE"})
    g = store.get_or_create_session(gromit)
    a = store.get_or_create_session(alfred)
    assert len({g.session_id, a.session_id, "legacy-gromit"}) == 3
    store.append_to_transcript(g.session_id, {"role": "user", "content": "GROMIT_PRIVATE"})
    store.append_to_transcript(a.session_id, {"role": "user", "content": "Old hello", "api_content": "PRIVATE_API"})
    db.update_system_prompt(a.session_id, "PRIVATE_STORED_PROMPT")
    config.a2a_conversation_only_peers = ["alfred"]
    restricted = store.get_or_create_session(alfred)
    assert restricted.session_id != a.session_id
    assert store.load_transcript(restricted.session_id) == []
    assert store.get_or_create_session(gromit).session_id == g.session_id
    assert store.load_transcript("legacy-gromit")[0]["content"] == "LEGACY_PRIVATE"
    assert db.get_session(a.session_id)["system_prompt"] == "PRIVATE_STORED_PROMPT"
    assert store.switch_session(restricted.session_key, a.session_id) is None
    assert store.switch_session(restricted.session_key, g.session_id) is None

    event = MessageEvent(text="Hi", message_type=MessageType.TEXT, source=alfred)
    assert adapter._event_session_key(event) == runner._session_key_for_source(alfred) == restricted.session_key
    # The runner's no-store path and the store's alternate-key path obey the same policy.
    runner.session_store = None
    assert runner._session_key_for_source(alfred) == restricted.session_key
    assert store._generate_session_key(alfred, gromit) == restricted.session_key
    # Force the real durable recovery path after discarding only in-memory routing.
    recovered, _ = store._query_recoverable_row(session_key=restricted.session_key, source=alfred, now=restricted.updated_at)
    assert recovered.session_id == restricted.session_id
    assert store._query_recoverable_row(session_key=restricted.session_key + "00", source=alfred,
                                      now=restricted.updated_at) == (None, False)

    agent = SimpleNamespace(conversation_only=True, _cached_system_prompt=None,
                            _session_db=db, session_id=a.session_id,
                            _build_system_prompt=lambda _: "safe prompt")
    _restore_or_build_system_prompt(agent, None, store.load_transcript(a.session_id))
    assert agent._cached_system_prompt == "safe prompt"
    store.close_all_db_handles()


def test_a2a_encoding_is_unambiguous_and_discord_keys_unchanged(tmp_path):
    config = GatewayConfig.from_dict({"gateway": {"a2a_conversation_only_peers": ["alfred"]}})
    store = SessionStore(tmp_path / "sessions", config)
    sources = [SessionSource(platform=Platform("a2a"), chat_type="dm", user_id=peer, chat_id=context)
               for peer, context in [("a:b", "c"), ("a", "b:c"), ("a/b", "☃"), ("a-b", "☃")]]
    assert len({store._generate_session_key(s) for s in sources}) == len(sources)
    discord = SessionSource(platform=Platform.DISCORD, chat_type="dm", user_id="alfred", chat_id="123")
    assert store._generate_session_key(discord) == "agent:main:discord:dm:123"
    store.close_all_db_handles()


def test_restricted_hygiene_skips_before_settings_or_agent_build():
    from gateway.run_turn import GatewayTurnMixin

    runner = object.__new__(GatewayTurnMixin)
    runner.config = GatewayConfig.from_dict({"gateway": {"a2a_conversation_only_peers": ["alfred"]}})
    runner._hmwa_hygiene_settings = AsyncMock(side_effect=AssertionError("hygiene must not start"))
    source = SessionSource(platform=Platform("a2a"), chat_type="dm", user_id="alfred", chat_id="ctx")
    history = [{"role": "user", "content": "large" * 10000}] * 10
    assert asyncio.run(runner._hmwa_run_session_hygiene(None, source, None, "key", history, None, None)) is history
    runner._hmwa_hygiene_settings.assert_not_called()
    runner._hmwa_hygiene_settings = AsyncMock(return_value=SimpleNamespace(compression_enabled=False, hard_msg_limit=1000))
    source.user_id = "gromit"
    assert asyncio.run(runner._hmwa_run_session_hygiene(None, source, None, "key", history, None, None)) is history
    runner._hmwa_hygiene_settings.assert_awaited_once()


def test_lifecycle_finalize_and_reset_skip_restricted_registered_hooks(monkeypatch):
    from hermes_cli import plugins
    from hermes_cli.plugins import PluginContext
    from hermes_cli.plugins_manifest import PluginManifest
    from gateway.run import GatewayRunner

    manager = plugins.PluginManager()
    monkeypatch.setattr(plugins, "_plugin_manager", manager)
    fired = []
    PluginContext(PluginManifest(name="finalize-sentinel"), manager).register_hook(
        "on_session_finalize", lambda **kwargs: fired.append(kwargs["session_id"]),
    )
    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig.from_dict({"gateway": {"a2a_conversation_only_peers": ["alfred"]}})
    runner._FINALIZE_TIMEOUT_S = 5
    runner._flush_agent_transcript_at_shutdown = Mock()
    runner._cleanup_agent_resources_off_loop = AsyncMock()
    runner.hooks = SimpleNamespace(emit=AsyncMock())

    async def execute(fn, *args):
        return fn(*args)

    runner._run_housekeeping_in_executor = execute
    asyncio.run(runner._finalize_shutdown_agents({
        "restricted": SimpleNamespace(conversation_only=True, session_id="restricted"),
        "ordinary": SimpleNamespace(conversation_only=False, session_id="ordinary"),
    }))
    assert fired == ["ordinary"]
    source = SessionSource(platform=Platform("a2a"), user_id="alfred", chat_id="ctx")
    asyncio.run(runner._fire_session_reset_hooks(source, "key", "old", "new"))
    assert fired == ["ordinary"]
    runner.hooks.emit.assert_not_awaited()
    source.user_id = "gromit"
    asyncio.run(runner._fire_session_reset_hooks(source, "key", "gromit-old", "gromit-new"))
    assert fired == ["ordinary", "gromit-old"]
    assert runner.hooks.emit.await_count == 2


def test_background_turn_does_not_adopt_gateway_prefill(tmp_path, monkeypatch):
    from gateway.run_turn import GatewayTurnMixin
    from gateway import run
    from run_agent import AIAgent

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(run, "_load_gateway_config", lambda: {})
    monkeypatch.setattr(run, "_current_max_iterations", lambda: 2)
    seen = []

    def reply(agent, **kwargs):
        seen.append((agent.conversation_only, agent.prefill_messages))
        return {"final_response": "hello", "messages": []}

    monkeypatch.setattr(AIAgent, "run_conversation", reply)
    runner = object.__new__(GatewayTurnMixin)
    runner.config = GatewayConfig.from_dict({"gateway": {"a2a_conversation_only_peers": ["alfred"]}})
    runner._prefill_messages = [{"role": "user", "content": "PRIVATE_PREFILL"}]
    adapter = SimpleNamespace(send=AsyncMock(), extract_media=lambda text: ([], text),
                              extract_images=lambda text: ([], text))
    runner._delivery_adapter_for = lambda _: adapter
    runner._thread_metadata_for_source = lambda *args: {}
    runtime = {"provider": "custom", "base_url": "https://example.invalid/v1", "api_key": "test-key"}
    runner._resolve_session_agent_runtime = lambda **kwargs: ("test-model", runtime)
    runner._resolve_turn_toolsets = lambda *args: ([], [])
    runner._provider_routing = {}
    runner._resolve_session_reasoning_config = lambda **kwargs: None
    runner._resolve_session_service_tier = lambda **kwargs: None
    runner._resolve_turn_agent_config = lambda *args: {"model": "test-model", "runtime": runtime}
    runner._session_db = None
    runner._refresh_fallback_model = lambda: None
    runner._cleanup_agent_resources = lambda agent: agent.close()

    async def execute(fn):
        return fn()

    runner._run_in_executor_with_context = execute
    for platform, peer in [(Platform.DISCORD, "ben"), (Platform("a2a"), "gromit"), (Platform("a2a"), "alfred")]:
        source = SessionSource(platform=platform, user_id=peer, chat_id="ctx")
        asyncio.run(runner._run_background_task_inner("hi", source, f"task-{peer}"))
    assert seen == [(False, []), (False, []), (True, [])]
