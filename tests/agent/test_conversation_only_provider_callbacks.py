"""Registered provider callbacks cannot replace restricted HTTP or inject context."""

from unittest.mock import Mock

import pytest


@pytest.mark.parametrize("restricted", [True, False])
def test_registered_provider_callbacks_preserve_conversation_boundary(tmp_path, monkeypatch, restricted):
    import httpx
    import providers
    from providers.base import ProviderProfile
    from run_agent import AIAgent
    from agent import process_bootstrap, served_model
    from agent.agent_runtime_helpers import create_openai_client
    from agent.chat_completion_helpers import build_api_kwargs

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    agent = AIAgent(provider="custom", base_url="https://example.invalid/v1", api_key="test-key",
                    model="test", conversation_only=True, quiet_mode=True)
    agent.conversation_only = restricted
    callbacks = []
    sentinel_http = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200)))

    class ProbeProfile(ProviderProfile):
        def build_client_kwargs_extras(self, **kwargs):
            callbacks.append("client")
            return {"http_client": sentinel_http}

        def prepare_messages(self, messages):
            callbacks.append("messages")
            return [*messages, {"role": "system", "content": "PRIVATE_PROVIDER_SENTINEL"}]

    providers._discover_providers()
    monkeypatch.setattr(providers, "_REGISTRY", dict(providers._REGISTRY))
    monkeypatch.setattr(providers, "_ALIASES", dict(providers._ALIASES))
    monkeypatch.setattr(providers, "_PROVIDER_LIST_CACHE", None)
    providers.register_provider(ProbeProfile(name="conversation-probe", auth_type="api_key",
                                             base_url="https://example.invalid/v1"))
    agent.provider = "conversation-probe"
    constructor = Mock(return_value=object())
    monkeypatch.setattr(process_bootstrap, "OpenAI", constructor)
    monkeypatch.setattr(served_model, "install_served_model_capture", lambda *args: None)
    try:
        create_openai_client(agent, {"api_key": "test-key", "base_url": agent.base_url},
                             reason="regression", shared=False)
        built_http = constructor.call_args.kwargs.get("http_client")
        assert (built_http is sentinel_http) is not restricted
        request = build_api_kwargs(agent, [{"role": "user", "content": "hello"}])
        assert ("PRIVATE_PROVIDER_SENTINEL" in str(request["messages"])) is not restricted
        assert callbacks == ([] if restricted else ["client", "messages"])
    finally:
        sentinel_http.close()
        agent.close()


@pytest.mark.parametrize("restricted", [True, False])
def test_registered_profiles_are_not_consulted_by_restricted_request_builders(tmp_path, monkeypatch, restricted):
    """Anthropic (Nous Portal merge), Responses (effort vocabulary) and unset reasoning defaults."""
    import providers
    from providers.base import ProviderProfile
    from run_agent import AIAgent
    from agent.chat_completion_helpers import build_api_kwargs

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    callbacks = []

    class ProbeProfile(ProviderProfile):
        def build_extra_body(self, **kwargs):
            callbacks.append("extra_body")
            return {"system": "PRIVATE_PROVIDER_SENTINEL"}

        def supported_reasoning_efforts(self, model):
            callbacks.append("efforts")
            return ("low",)

        def default_reasoning_config(self, model):
            callbacks.append("default_reasoning")
            return {"enabled": True, "effort": "low"}

    providers._discover_providers()
    monkeypatch.setattr(providers, "_REGISTRY", dict(providers._REGISTRY))
    monkeypatch.setattr(providers, "_ALIASES", dict(providers._ALIASES))
    monkeypatch.setattr(providers, "_PROVIDER_LIST_CACHE", None)
    for name in ("nous", "conversation-probe"):
        providers.register_provider(ProbeProfile(name=name, auth_type="api_key",
                                                 base_url="https://example.invalid/v1"))
    messages = [{"role": "user", "content": "hello"}]
    routes = (("anthropic_messages", "nous", "extra_body"),
              ("codex_responses", "conversation-probe", "efforts"),
              ("chat_completions", "conversation-probe", "default_reasoning"))
    for api_mode, provider, callback in routes:
        agent = AIAgent(provider="custom", base_url="https://example.invalid/v1", api_key="test-key",
                        model="test", conversation_only=True, quiet_mode=True)
        try:
            agent.conversation_only = restricted
            agent.provider, agent.api_mode = provider, api_mode
            agent.reasoning_config = {"enabled": True, "effort": "high"} if api_mode == "codex_responses" else None
            callbacks.clear()
            request = build_api_kwargs(agent, messages)
            if restricted:
                assert "PRIVATE_PROVIDER_SENTINEL" not in str(request), api_mode
                assert callbacks == [], api_mode
            else:
                assert callback in callbacks, api_mode
        finally:
            agent.close()
