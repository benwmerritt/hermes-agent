"""Real registered observers cannot see restricted ingress, streams, or errors."""

import asyncio
import threading

import pytest


@pytest.fixture
def sentinel_plugin(monkeypatch):
    from hermes_cli import plugins
    from hermes_cli.plugins import PluginContext
    from hermes_cli.plugins_manifest import PluginManifest
    from agent.plugin_stream_hooks import shutdown_plugin_stream_hook_dispatcher

    manager = plugins.PluginManager()
    monkeypatch.setattr(plugins, "_plugin_manager", manager)
    ctx = PluginContext(PluginManifest(name="restricted-path-sentinel"), manager)
    calls = []
    def register(name, result=None):
        def sentinel(**kwargs):
            calls.append((name, kwargs))
            return result
        ctx.register_hook(name, sentinel)
    register("pre_gateway_dispatch", {"action": "rewrite", "text": "PRIVATE_PLUGIN_CONTEXT"})
    register("transform_api_error_classification", {"reason": "content_policy_blocked", "retryable": False})
    for name in ("on_stream_start", "on_stream_delta", "on_stream_end", "on_interim_message"):
        register(name)
    try:
        yield calls
    finally:
        shutdown_plugin_stream_hook_dispatcher(timeout=5)


def test_ingress_uses_authenticated_source_before_plugin_rewrite(sentinel_plugin):
    from gateway.config import GatewayConfig, Platform
    from gateway.run_inbound import GatewayInboundMixin
    from gateway.platforms.event import MessageEvent, MessageType
    from gateway.session import SessionSource

    runner = GatewayInboundMixin()
    runner.config = GatewayConfig.from_dict({"gateway": {"a2a_conversation_only_peers": ["alfred"]}})
    runner._scale_to_zero_note_real_inbound = lambda: None
    runner._is_user_authorized_for_source = lambda source: True
    runner._admit_bot_message_for_source = lambda source: True
    for peer, expected in (("alfred", "hello"), ("gromit", "PRIVATE_PLUGIN_CONTEXT")):
        source = SessionSource(platform=Platform("a2a"), user_id=peer, chat_id="shared", chat_type="dm")
        event = MessageEvent(text="hello", source=source, message_type=MessageType.TEXT)
        admitted, _, _ = asyncio.run(runner._hm_admit_event(event))
        assert admitted.text == expected
        if peer == "alfred":
            assert sentinel_plugin == []
    assert [name for name, _ in sentinel_plugin] == ["pre_gateway_dispatch"]


@pytest.mark.parametrize("restricted", [True, False])
def test_stream_callbacks_and_all_error_classifiers_preserve_policy(tmp_path, monkeypatch, sentinel_plugin, restricted):
    from run_agent import AIAgent
    from agent.chat_completion_helpers import _StreamingCall, ProviderStreamError
    from agent.error_classifier import classify_api_error, FailoverReason
    from agent.error_surface import build_error_surface_from_exception
    from agent.context_compressor import _is_summary_access_or_quota_error
    from agent import plugin_stream_hooks

    monkeypatch.chdir(tmp_path)
    # Construct the real restricted agent to keep unrelated lifecycle hooks out of this test.
    agent = AIAgent(provider="custom", base_url="https://example.invalid/v1", api_key="test-key",
                    model="test", conversation_only=True, quiet_mode=True)
    agent.conversation_only = restricted
    display, reasoning, interim = [], [], []
    agent.stream_delta_callback = display.append
    agent._stream_callback = None
    agent.reasoning_callback = reasoning.append
    agent.interim_assistant_callback = lambda text, **kwargs: interim.append(text)
    monkeypatch.setattr(plugin_stream_hooks, "stream_reasoning_deltas_enabled", lambda: True)
    error = ProviderStreamError(status_code=401, body={"error": {"message": "invalid API key"}}, raw_text="invalid API key")
    try:
        # Real callbacks on a fresh thread: no inherited policy ContextVar is needed.
        failures = []
        def live_callbacks():
            try:
                agent._emit_stream_start()
                agent._fire_stream_delta("hello")
                agent._fire_reasoning_delta("reasoning")
                agent._emit_interim_assistant_message({"role": "assistant", "content": "interim"})
                agent._emit_stream_end(final_text="hello", finished=False, error="provider failed")
                call = _StreamingCall(agent, {}, None)
                call.result["error"] = error
                stub = call._partial_stream_stub()
                assert bool(getattr(stub, "_content_filter_terminated", False)) is (not restricted)
            except BaseException as exc:
                failures.append(exc)
        thread = threading.Thread(target=live_callbacks)
        thread.start()
        thread.join(timeout=5)
        assert not thread.is_alive() and not failures
        # Drain real observer queues deterministically before asserting absence/presence.
        plugin_stream_hooks.shutdown_plugin_stream_hook_dispatcher(timeout=5)
        assert display == ["hello"] and reasoning == ["reasoning"] and interim == ["interim"]
        classified = classify_api_error(error, conversation_only=restricted)
        assert classified.reason == (FailoverReason.auth if restricted else FailoverReason.content_policy_blocked)
        surface = build_error_surface_from_exception(error, conversation_only=restricted)
        assert surface["code"] == classified.reason.value
        _is_summary_access_or_quota_error(error, conversation_only=restricted)
        from types import SimpleNamespace
        from agent.turn_recovery import classify_codex_soft_failure
        agent.api_mode = "codex_responses"
        soft, _ = classify_codex_soft_failure(agent, SimpleNamespace(
            status="failed", error={"code": "invalid_api_key", "message": "invalid API key"},
        ))
        assert soft.reason == (FailoverReason.auth if restricted else FailoverReason.content_policy_blocked)
        if restricted:
            assert sentinel_plugin == []
            agent.stream_delta_callback = None
            assert not agent._has_stream_consumers()
        else:
            assert {name for name, _ in sentinel_plugin} == {
                "on_stream_start", "on_stream_delta", "on_stream_end", "on_interim_message",
                "transform_api_error_classification",
            }
            assert agent._has_stream_consumers()
    finally:
        agent.conversation_only = True
        agent.close()


def test_real_http_turn_error_keeps_builtin_classification(tmp_path, monkeypatch, sentinel_plugin):
    import json
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from run_agent import AIAgent

    received = []
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            body = json.dumps({"error": {"message": "invalid API key", "type": "authentication_error"}}).encode()
            self.send_response(401)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        def log_message(self, *args):
            pass

    monkeypatch.chdir(tmp_path)
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    agent = None
    try:
        agent = AIAgent(provider="custom", base_url=f"http://127.0.0.1:{server.server_port}/v1",
                        api_key="test-key", model="test", conversation_only=True, quiet_mode=True,
                        max_iterations=1, save_trajectories=False)
        result = agent.run_conversation("hello")
        assert received
        assert result["failed"]
        assert "401" in result["error"]
        assert sentinel_plugin == []
        assert agent.context_compressor.conversation_only is True
    finally:
        if agent is not None:
            agent.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.parametrize("restricted", [True, False])
def test_early_output_transform_and_gateway_callbacks_obey_peer_policy(monkeypatch, restricted):
    from types import SimpleNamespace
    from unittest.mock import Mock
    from agent.turn_finalizer import apply_llm_output_transform, _apply_output_hooks
    from agent import turn_finalizer
    from gateway.config import GatewayConfig, Platform
    from gateway.run_turn import GatewayTurnMixin
    from gateway.run_turn_runner import TurnRunner
    from gateway.session import SessionSource

    hook = Mock(return_value=["transformed"])
    monkeypatch.setattr(turn_finalizer, "_invoke_hook_safely", hook)
    agent = SimpleNamespace(conversation_only=restricted, session_id="s", model="test", platform="a2a")
    early = apply_llm_output_transform(agent, "original", turn_id="t")
    final = _apply_output_hooks(
        agent, early[0], None, platform="a2a", effective_task_id="task", turn_id="t",
        original_user_message="hello", messages=[],
    )
    assert early == final
    assert final[0] == ("original" if restricted else "transformed")
    assert [call.args[0] for call in hook.call_args_list] == (
        [] if restricted else ["transform_llm_output", "post_llm_call"]
    )

    runner = GatewayTurnMixin()
    runner.config = GatewayConfig(a2a_conversation_only_peers=["alfred"])
    source = SessionSource(platform=Platform("a2a"), user_id="alfred" if restricted else "gromit", chat_id="c")
    emitted = Mock(return_value=object())
    ctx = SimpleNamespace(source=source, _run_still_current=lambda: True, session_id="s",
                          _hooks_ref=SimpleNamespace(emit=emitted), _loop_for_step=None)
    turn = TurnRunner(runner, ctx)
    monkeypatch.setattr(turn, "_schedule", Mock())
    monkeypatch.setattr(asyncio, "run_coroutine_threadsafe", Mock())
    turn._step_callback_sync(1, [])
    turn._event_callback_sync("agent:custom", {})
    assert emitted.call_count == (0 if restricted else 2)
