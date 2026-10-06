"""Transport restrictions for conversation-only agents."""

from contextvars import ContextVar
from urllib.parse import urlsplit

_restricted_resolution = ContextVar("restricted_provider_resolution", default=False)


def require_conversation_transport(restricted, provider=None, api_mode=None, base_url=None):
    if not restricted:
        return
    provider = str(provider or "").strip().lower()
    if provider == "moa":
        raise ValueError("MoA is disabled for conversation-only agents")
    mode = str(api_mode or "").strip().lower()
    scheme = urlsplit(str(base_url or "")).scheme.lower()
    if (mode not in {"", "chat_completions", "codex_responses", "anthropic_messages", "bedrock_converse"}
            or provider in {"copilot-acp", "github-copilot-acp", "copilot-acp-agent"}
            or scheme in {"acp", "acp+tcp"}):
        raise ValueError("Execution-capable transports are disabled for conversation-only agents")
    # Covers registered external-process providers, including third-party ACP adapters.
    from hermes_cli.auth import PROVIDER_REGISTRY
    profile = PROVIDER_REGISTRY.get(provider)
    if profile is not None and getattr(profile, "auth_type", None) == "external_process":
        raise ValueError("Execution-capable providers are disabled for conversation-only agents")


def check_agent_transport(agent, **route):
    require_conversation_transport(
        getattr(agent, "conversation_only", False),
        **{key: route.get(key, getattr(agent, key, None)) for key in ("provider", "api_mode", "base_url")},
    )


def resolve_agent_client(agent, *args, **kwargs):
    """Carry the restriction through automatic and recursive provider resolution."""
    from agent.auxiliary_client import resolve_provider_client
    token = _restricted_resolution.set(
        _restricted_resolution.get() or bool(getattr(agent, "conversation_only", False))
    )
    try:
        return resolve_provider_client(*args, **kwargs)
    finally:
        _restricted_resolution.reset(token)
