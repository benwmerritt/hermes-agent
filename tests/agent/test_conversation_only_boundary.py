from types import SimpleNamespace
from unittest.mock import Mock

import pytest


def test_conversation_only_agent_has_safe_prompt_and_denies_fabricated_tools(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "AGENTS.md").write_text("LOCAL_CONTEXT_SENTINEL")
    from agent.system_prompt import build_system_prompt_parts
    from agent.agent_runtime_helpers import invoke_tool

    executor = Mock(side_effect=AssertionError("tool executor must not run"))
    agent = SimpleNamespace(
        conversation_only=True, tools=[], valid_tool_names=set(),
        _cached_system_prompt=None, _emit_status=Mock(), _memory_store=None,
        _memory_manager=None, _background_review_run=None, session_id="s", quiet_mode=True,
        _invoke_tool=executor, _interrupt_requested=False,
    )
    prompt = build_system_prompt_parts(agent, "CALLER_CONTEXT_SENTINEL")
    assert prompt == {"stable": "You are Wallace, a helpful conversational assistant.", "context": "", "volatile": ""}
    assert "LOCAL_CONTEXT_SENTINEL" not in str(prompt)
    assert "CALLER_CONTEXT_SENTINEL" not in str(prompt)
    assert "MEMORY_SENTINEL" not in str(prompt)
    assert "SKILLS_SENTINEL" not in str(prompt)
    assert "SOUL_SENTINEL" not in str(prompt)
    for tool_name in ("terminal", "read_file", "recall", "retain"):
        result = invoke_tool(agent, tool_name, {"command": "id"}, "task")
        assert "disabled" in result.lower()
    executor.assert_not_called()


@pytest.mark.parametrize("dispatch", ["sequential", "concurrent"])
@pytest.mark.parametrize("tool_name", ["terminal", "read_file", "recall", "retain"])
def test_conversation_only_rejects_fabricated_tool_calls_before_dispatch(dispatch, tool_name):
    from agent.tool_executor import execute_tool_calls_sequential, execute_tool_calls_concurrent

    invoke = Mock(side_effect=AssertionError("tool executor must not run"))
    call = SimpleNamespace(id="c1", function=SimpleNamespace(name=tool_name, arguments='{"command":"id"}'))
    message = SimpleNamespace(tool_calls=[call])
    agent = SimpleNamespace(conversation_only=True, tools=[], valid_tool_names=set(), _invoke_tool=invoke,
                            _interrupt_requested=False, _incremental_persistence_failed=False,
                            quiet_mode=True, session_id="s", log_prefix="", verbose_logging=False,
                            _safe_print=Mock(), _persist_session=Mock())
    messages = []
    fn = execute_tool_calls_sequential if dispatch == "sequential" else execute_tool_calls_concurrent
    fn(agent, message, messages, "task")
    invoke.assert_not_called()
    assert any(m.get("role") == "tool" and "disabled" in m.get("content", "").lower() for m in messages)


def test_gateway_conversation_only_config_is_exact_peer_allowlist():
    from gateway.config import GatewayConfig

    assert GatewayConfig.from_dict({}).a2a_conversation_only_peers == []
    assert GatewayConfig.from_dict({"gateway": {"a2a_conversation_only_peers": ["alfred"]}}).a2a_conversation_only_peers == ["alfred"]
    assert GatewayConfig.from_dict({"gateway": {"a2a_conversation_only_peers": "alfred"}}).a2a_conversation_only_peers == ["*"]
    assert GatewayConfig.from_dict({"gateway": {"a2a_conversation_only_peers": ["", None, "alfred"]}}).a2a_conversation_only_peers == ["*"]
    source_a2a = SimpleNamespace(platform="a2a", user_id="alfred")
    source_other = SimpleNamespace(platform="telegram", user_id="alfred")
    from gateway.run_turn import GatewayTurnMixin
    runner = object.__new__(GatewayTurnMixin)
    runner.config = GatewayConfig.from_dict({"gateway": {"a2a_conversation_only_peers": ["alfred"]}})
    assert runner._is_conversation_only_peer(source_a2a)
    assert not runner._is_conversation_only_peer(source_other)
    assert not runner._is_conversation_only_peer(SimpleNamespace(platform="a2a", user_id="other"))
    runner.config = GatewayConfig.from_dict({"gateway": {"a2a_conversation_only_peers": "alfred"}})
    assert runner._is_conversation_only_peer(SimpleNamespace(platform="a2a", user_id="unknown"))


def test_agent_cache_signature_separates_conversation_only_mode():
    from gateway.run_agent_cache import GatewayAgentCacheMixin

    args = ("model", {}, [], "")
    unrestricted = GatewayAgentCacheMixin._agent_config_signature(*args, conversation_only=False)
    restricted = GatewayAgentCacheMixin._agent_config_signature(*args, conversation_only=True)
    assert unrestricted != restricted


@pytest.mark.parametrize("peer,restricted", [("alfred", True), ("gromit", False)])
def test_real_platform_and_turn_runner_use_configured_peer(peer, restricted):
    from gateway.config import GatewayConfig, Platform
    from gateway.run_turn import GatewayTurnMixin
    from gateway.run_turn_runner import TurnRunner
    from gateway.turn_context import TurnContext

    runner = object.__new__(GatewayTurnMixin)
    runner.config = GatewayConfig.from_dict({"gateway": {"a2a_conversation_only_peers": ["alfred"]}})
    source = SimpleNamespace(platform=Platform("a2a"), user_id=peer)
    turn = TurnRunner(runner, TurnContext(source=source, user_config={"gateway": {}}))
    assert runner._is_conversation_only_peer(source) is restricted
    assert turn._conversation_only_peer(source) is restricted
    source.platform = Platform.DISCORD
    assert not turn._conversation_only_peer(source)


@pytest.mark.parametrize("restricted", [False, True])
def test_real_agent_constructor_preserves_normal_context_and_isolates_peer(tmp_path, monkeypatch, restricted):
    from run_agent import AIAgent

    monkeypatch.chdir(tmp_path)
    (tmp_path / "AGENTS.md").write_text("LOCAL_CONTEXT_SENTINEL")
    agent = AIAgent(
        provider="custom", base_url="https://example.invalid/v1", api_key="test-key",
        model="test-model", quiet_mode=True, enabled_toolsets=[],
        conversation_only=restricted, skip_memory=True,
        skip_context_files=False, load_soul_identity=True,
        prefill_messages=[{"role": "user", "content": "PREFILL_SENTINEL"}],
    )
    assert agent.skip_context_files is restricted
    assert agent.load_soul_identity is (not restricted)
    prompt = agent._build_system_prompt(None)
    assert ("LOCAL_CONTEXT_SENTINEL" in prompt) is (not restricted)
    if restricted:
        assert agent.tools == []
        assert agent.valid_tool_names == set()
        assert agent._memory_store is None
        assert agent._memory_manager is None
        assert not agent.prefill_messages
        assert agent.skip_background_review


def test_real_conversation_loop_denies_provider_tool_call(tmp_path, monkeypatch):
    import json
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from run_agent import AIAgent

    from hermes_cli import plugins
    from hermes_cli.plugins import PluginContext
    from hermes_cli.plugins_manifest import PluginManifest

    manager = plugins.PluginManager()
    monkeypatch.setattr(plugins, "_plugin_manager", manager)
    ctx = PluginContext(PluginManifest(name="boundary-sentinel"), manager)
    fired = []

    def sentinel(**kwargs):
        fired.append(kwargs)
        return "PRIVATE_PLUGIN_SENTINEL"

    for hook in ("on_session_start", "on_session_end", "on_session_finalize", "pre_llm_call",
                 "transform_llm_output", "post_llm_call", "pre_api_request", "post_api_request",
                 "api_request_error"):
        ctx.register_hook(hook, sentinel)
    for kind in ("llm_request", "llm_execution"):
        ctx.register_middleware(kind, sentinel)

    captured = []
    marker = tmp_path / "must-not-exist"
    responses = [
        {"role": "assistant", "content": None, "tool_calls": [
            {"id": "forged", "type": "function", "function": {
                "name": "terminal", "arguments": json.dumps({"command": f"touch {marker}"}),
            }},
        ]},
        {"role": "assistant", "content": "Hello Alfred."},
    ]

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if not self.path.endswith("/chat/completions"):
                self.send_error(404)
                return
            captured.append(request)
            message = responses.pop(0) if responses else {"role": "assistant", "content": "Hello Alfred."}
            if request.get("stream"):
                delta = dict(message)
                for index, call in enumerate(delta.get("tool_calls", [])):
                    call["index"] = index
                chunks = [
                    {"id": "test", "choices": [{"index": 0, "delta": delta, "finish_reason": None}]},
                    {"id": "test", "choices": [{"index": 0, "delta": {},
                    "finish_reason": "tool_calls" if message.get("tool_calls") else "stop"}]},
                ]
                body = ("".join(f"data: {json.dumps(chunk)}\n\n" for chunk in chunks) + "data: [DONE]\n\n").encode()
                content_type = "text/event-stream"
            else:
                body = json.dumps({
                    "id": "test", "choices": [{"index": 0, "message": message,
                    "finish_reason": "tool_calls" if message.get("tool_calls") else "stop"}],
                    "usage": {"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20},
                }).encode()
                content_type = "application/json"
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    monkeypatch.chdir(tmp_path)
    (tmp_path / "AGENTS.md").write_text("PRIVATE_FILE_SENTINEL")
    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        agent = AIAgent(
            provider="custom", base_url=f"http://127.0.0.1:{server.server_port}/v1",
            api_key="test-key", model="test-model", quiet_mode=True,
            conversation_only=True, max_iterations=3, save_trajectories=False,
        )
        agent._invoke_tool = Mock(side_effect=AssertionError("executor must not run"))
        result = agent.run_conversation(
            "Hello Wallace.", system_message="PRIVATE_PROMPT_SENTINEL",
            conversation_history=[
                {"role": "user", "content": "Earlier hello", "api_content": "PRIVATE_API_SENTINEL"},
                {"role": "assistant", "content": "Hello", "api_content": "PRIVATE_ASSISTANT_SENTINEL"},
            ],
        )
        assert result["final_response"] == "Hello Alfred."
        agent._invoke_api_request_error_hook(
            task_id="task", turn_id="turn", api_request_id="request", api_call_count=1,
            api_start_time=0, api_kwargs={}, error_type="test", error_message="test",
        )
        agent.close()
        assert fired == []
        assert "PRIVATE_API_SENTINEL" not in json.dumps(captured)
        assert "PRIVATE_ASSISTANT_SENTINEL" not in json.dumps(captured)
        assert "PRIVATE_PLUGIN_SENTINEL" not in json.dumps(captured)
        # The same registered hook still applies to an ordinary agent.
        import logging
        from agent.turn_finalizer import _apply_output_hooks
        agent.conversation_only = False
        output, transformed, _ = _apply_output_hooks(
            agent, "ordinary reply", logging.getLogger(__name__), platform="discord",
            effective_task_id="ordinary", turn_id="ordinary", original_user_message="Hi", messages=[],
        )
        assert transformed and output == "PRIVATE_PLUGIN_SENTINEL"
        assert len(fired) == 2
        agent._invoke_tool.assert_not_called()
        assert not marker.exists()
        assert len(captured) >= 2
        assert all(not request.get("tools") for request in captured)
        assert "PRIVATE_FILE_SENTINEL" not in json.dumps(captured)
        assert "PRIVATE_PROMPT_SENTINEL" not in json.dumps(captured)
        assert any(message.get("role") == "tool" and "disabled" in message.get("content", "").lower()
                   for request in captured for message in request.get("messages", []))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
