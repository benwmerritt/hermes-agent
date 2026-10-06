"""Restricted agents reject native execution before transport construction."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest


@pytest.mark.parametrize("route", [
    {"provider": "openai-codex", "api_mode": "codex_app_server"},
    {"provider": "copilot-acp"},
    {"provider": "custom", "base_url": "acp://copilot"},
    {"provider": "custom", "base_url": "acp+tcp://localhost:9000"},
])
def test_constructor_rejects_before_build_client(tmp_path, monkeypatch, route):
    from run_agent import AIAgent
    from agent import agent_init

    monkeypatch.chdir(tmp_path)
    build = Mock(side_effect=AssertionError("client initialization must not run"))
    monkeypatch.setattr(agent_init, "_build_client", build)
    with pytest.raises(ValueError, match="Execution-capable"):
        AIAgent(**route, model="test-model", api_key="test-key", conversation_only=True)
    build.assert_not_called()


@pytest.mark.parametrize("operation", ["restore", "switch", "adopt", "env_adopt", "rebuild", "turn", "codex"])
def test_runtime_reentry_rejects_native_transport(tmp_path, monkeypatch, operation):
    from run_agent import AIAgent
    from agent.agent_runtime_helpers import restore_primary_runtime, create_openai_client
    from agent.conversation_loop import run_conversation
    from agent.codex_runtime import _ensure_codex_session

    monkeypatch.chdir(tmp_path)
    agent = AIAgent(provider="custom", base_url="https://example.invalid/v1", api_key="test-key",
                    model="test-model", conversation_only=True, quiet_mode=True)
    original_client = agent.client
    with pytest.raises(ValueError, match="Execution-capable"):
        if operation == "restore":
            agent._fallback_activated = True
            agent._primary_runtime = {**agent._primary_runtime, "api_mode": "codex_app_server"}
            restore_primary_runtime(agent)
        elif operation == "switch":
            agent.switch_model("native", "copilot-acp", "test-key", "acp://copilot")
        elif operation == "adopt":
            agent._adopt_openai_credentials("test-key", "acp://copilot", reason="test")
        elif operation == "env_adopt":
            monkeypatch.setattr(agent, "_resolve_env_credentials", lambda: ("test-key", "acp://copilot", "https://example.invalid/v1"))
            monkeypatch.setattr(agent, "_should_adopt_env_credentials", lambda *args: True)
            agent._try_refresh_env_client_credentials()
        elif operation == "rebuild":
            create_openai_client(agent, {"base_url": "acp://copilot", "api_key": "test-key"}, reason="test", shared=True)
        elif operation == "turn":
            agent.api_mode = "codex_app_server"
            run_conversation(agent, "hello")
        else:
            _ensure_codex_session(agent)
    assert agent.client is original_client
    agent.close()


def test_fallback_skips_native_candidates_without_initializing(tmp_path, monkeypatch):
    from run_agent import AIAgent
    from agent.chat_completion_helpers import try_activate_fallback
    from agent.conversation_policy import resolve_agent_client
    from providers import get_provider_profile

    monkeypatch.chdir(tmp_path)
    profile = get_provider_profile("copilot-acp")
    create = Mock(side_effect=AssertionError("ACP must not initialize"))
    agent = AIAgent(provider="custom", base_url="https://example.invalid/v1", api_key="test-key",
                    model="test-model", conversation_only=True, quiet_mode=True)
    original_client = agent.client
    monkeypatch.setattr(type(profile), "create_client", create)
    agent._fallback_chain = [
        {"provider": "copilot-acp", "model": "native"},
        {"provider": "custom", "model": "native", "api_mode": "codex_app_server",
         "base_url": "https://example.invalid/v1", "api_key": "test-key"},
    ]
    assert try_activate_fallback(agent) is False
    with pytest.raises(ValueError, match="Execution-capable"):
        resolve_agent_client(agent, "github-copilot-acp", model="native")
    create.assert_not_called()
    assert agent.client is original_client
    agent.close()


@pytest.mark.parametrize("provider,mode,url", [
    ("custom", "chat_completions", "https://example.invalid/v1"),
    ("anthropic", "anthropic_messages", "https://api.anthropic.com"),
    ("openai-codex", "codex_responses", "https://chatgpt.com/backend-api/codex"),
    ("gemini", "chat_completions", "https://generativelanguage.googleapis.com"),
])
def test_http_transports_remain_available(provider, mode, url):
    from agent.conversation_policy import check_agent_transport

    check_agent_transport(SimpleNamespace(conversation_only=True, provider=provider, api_mode=mode, base_url=url))


def test_initial_fallback_and_recursive_auto_resolution_reject_execution(tmp_path, monkeypatch):
    from agent import auxiliary_client
    from agent.agent_init import _routed_client_kwargs
    from agent.conversation_policy import resolve_agent_client
    from providers import get_provider_profile

    monkeypatch.chdir(tmp_path)
    agent = SimpleNamespace(conversation_only=True, provider="custom", model="test-model")
    create = Mock(side_effect=AssertionError("native client must not initialize"))
    monkeypatch.setattr(type(get_provider_profile("copilot-acp")), "create_client", create)
    monkeypatch.setitem(auxiliary_client._EXPLICIT_PROVIDER_BRANCHES, "custom", lambda req: (None, None))
    with pytest.raises(RuntimeError, match="No LLM provider configured"):
        _routed_client_kwargs(agent, [{"provider": "copilot-acp", "model": "native"}], None)
    create.assert_not_called()

    def auto_route(**kwargs):
        client, model = auxiliary_client.resolve_provider_client("copilot-acp", "native")
        return client, model, "copilot-acp"

    monkeypatch.setattr(auxiliary_client, "_resolve_auto_route", auto_route)
    with pytest.raises(ValueError, match="Execution-capable"):
        resolve_agent_client(agent, "auto", model="native")
    create.assert_not_called()
    # Resolution scope never bleeds into another agent's call.
    assert auxiliary_client.resolve_provider_client("custom", "ordinary") == (None, None)


def test_restricted_title_never_spawns_daemon_or_auxiliary_client(tmp_path, monkeypatch):
    import subprocess
    from agent import title_generator, auxiliary_client
    from agent.turn_context import _maybe_title_session_at_turn_start
    from hermes_state import SessionDB

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    (tmp_path / "config.yaml").write_text("auxiliary:\n  title_generation:\n    enabled: true\n    provider: copilot-acp\n")
    db = SessionDB(tmp_path / "titles.db")
    db.create_session(session_id="title-test", source="a2a")
    agent = SimpleNamespace(conversation_only=True, _session_db=db, session_id="title-test",
                            _session_db_created=True, platform="a2a", provider="custom", model="test")
    forbidden = Mock(side_effect=AssertionError("restricted title execution"))
    monkeypatch.setattr(title_generator.threading, "Thread", forbidden)
    monkeypatch.setattr(auxiliary_client, "resolve_provider_client", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    try:
        _maybe_title_session_at_turn_start(agent, [{"role": "user", "content": "Help plan my garden"}])
        forbidden.assert_not_called()
        assert db.get_session_title("title-test") is None
        # The ordinary path still reaches the actual daemon creation site.
        thread = Mock()
        monkeypatch.setattr(title_generator.threading, "Thread", thread)
        agent.conversation_only = False
        _maybe_title_session_at_turn_start(agent, [{"role": "user", "content": "Help plan my garden"}])
        thread.assert_called_once()
        thread.return_value.start.assert_called_once()
        assert db.get_session_title("title-test")
        forbidden.assert_not_called()
    finally:
        db.close()


def test_moa_rejected_before_facade_or_constituent_routing(tmp_path, monkeypatch):
    import base64
    import json
    import subprocess
    from run_agent import AIAgent
    from agent import agent_init, moa_loop, conversation_loop
    from agent.conversation_policy import resolve_agent_client
    from hermes_cli.moa_config import MOA_MARKER_PREFIX

    monkeypatch.chdir(tmp_path)
    forbidden = Mock(side_effect=AssertionError("MoA constituent construction"))
    monkeypatch.setattr(moa_loop, "build_moa_facade", forbidden)
    monkeypatch.setattr(moa_loop, "call_llm", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    with pytest.raises(ValueError, match="MoA"):
        AIAgent(provider="moa", model="test", conversation_only=True)
    with pytest.raises(ValueError, match="MoA"):
        resolve_agent_client(SimpleNamespace(conversation_only=True), "moa", model="test")
    agent = AIAgent(provider="custom", base_url="https://example.invalid/v1", api_key="test-key",
                    model="test", conversation_only=True, quiet_mode=True)
    config = {"reference_models": [{"provider": "copilot-acp", "model": "native"}],
              "aggregator": {"provider": "copilot-acp", "model": "native"}}
    inline = MOA_MARKER_PREFIX + base64.urlsafe_b64encode(json.dumps({"prompt": "hello", "config": config}).encode()).decode()
    try:
        for text, options in ((inline, {}), ("hello", {"moa_config": config})):
            with pytest.raises(ValueError, match="MoA"):
                conversation_loop.run_conversation(agent, text, **options)
        forbidden.assert_not_called()
        # Normal inline requests pass the policy gate and reach turn preparation.
        class Prepared(Exception):
            pass
        prepare = Mock(side_effect=Prepared)
        monkeypatch.setattr(conversation_loop, "begin_fast_mode_turn", prepare)
        agent.conversation_only = False
        with pytest.raises(Prepared):
            conversation_loop.run_conversation(agent, inline)
        prepare.assert_called_once()
        facade = Mock(return_value=object())
        monkeypatch.setattr(moa_loop, "build_moa_facade", facade)
        agent.provider = "moa"
        agent_init._init_moa_client(agent, "")
        facade.assert_called_once()
        forbidden.assert_not_called()
    finally:
        agent.close()
