"""Exercise the human command and real gateway prompt delivery, not a grant setter."""
import asyncio
import concurrent.futures
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from gateway.slash_commands import GatewaySlashCommandsMixin
from tools import approval
from tools.approval_gateway_wait import _ApprovalEntry


@pytest.mark.asyncio
async def test_explicit_scope_command_is_request_bound():
    runner = GatewaySlashCommandsMixin()
    runner._pending_approvals = {}
    runner._session_key_for_source = lambda source: "scope-chat"
    runner._deliver_approval_confirmation = AsyncMock(side_effect=lambda event, text, kind: text)
    event = SimpleNamespace(source=SimpleNamespace(is_bot=False, user_id="human"), get_command_args=lambda: "instruction-15m request-1")
    entry = _ApprovalEntry({"request_id": "request-1", "instruction_scope": {
        "paths": ["/review/AGENTS.md"], "seconds": 900, "worktree": "/review"}})
    with approval._lock:
        approval._gateway_queues["scope-chat"] = [entry]
    try:
        reply = await runner._handle_approve_command(event)
        assert entry.result == "instruction_15m"
        assert "15 minutes" in reply
        assert "session" in reply
    finally:
        approval.clear_session("scope-chat")


def test_real_notify_renders_exact_files_duration_and_command():
    from gateway.run_turn_runner import TurnRunner
    from gateway.platforms.base import SendResult
    runner = TurnRunner.__new__(TurnRunner)
    adapter = SimpleNamespace(pause_typing_for_chat=Mock(), typed_command_prefix="!",
                              send=AsyncMock(return_value=SendResult(success=True)))
    runner._ctx = SimpleNamespace(_status_adapter=adapter, _status_chat_id="chat",
                                  _status_thread_metadata=None, session_key="scope-chat")
    runner._close_native_stream_boundary = Mock()

    def schedule(coro, label):
        future = concurrent.futures.Future()
        future.set_result(asyncio.run(coro))
        return future

    runner._schedule = schedule
    data = {"command": "<write>", "description": "protected instruction write",
            "request_id": "request-1", "allow_session": False, "allow_permanent": False,
            "instruction_scope": {"paths": ["/review/a/AGENTS.md", "/review/b/AGENTS.md"],
                                  "seconds": 900, "worktree": "/review"}}
    runner._approval_notify_sync(data)
    message = adapter.send.call_args.args[1]
    assert "/review/a/AGENTS.md" in message and "/review/b/AGENTS.md" in message
    assert "15 minutes" in message
    assert "!approve instruction-15m request-1" in message
    assert "!approve session" not in message
    assert "child sessions" in message

    adapter.send.return_value = SendResult(success=False, error="transport refused")
    with pytest.raises(RuntimeError):
        runner._approval_notify_sync(data)



@pytest.mark.parametrize("args", ["instruction-15m", "instruction-15m wrong-id",
                                   "instruction-15m request-1 all"])
@pytest.mark.asyncio
async def test_invalid_explicit_command_leaves_request_pending(args):
    runner = GatewaySlashCommandsMixin()
    runner._pending_approvals = {}
    runner._session_key_for_source = lambda source: "scope-chat"
    event = SimpleNamespace(source=SimpleNamespace(is_bot=False, user_id="human"), get_command_args=lambda: args)
    entry = _ApprovalEntry({"request_id": "request-1", "instruction_scope": {
        "paths": ["/review/AGENTS.md"], "seconds": 900, "worktree": "/review"}})
    with approval._lock:
        approval._gateway_queues["scope-chat"] = [entry]
    try:
        await runner._handle_approve_command(event)
        assert not entry.event.is_set()
        assert not approval.resolve_gateway_approval("scope-chat", "instruction_15m", resolve_all=True)
        assert not approval.resolve_gateway_approval("scope-chat", "instruction_15m")
        assert not approval.resolve_gateway_approval("different-chat", "instruction_15m", request_id="request-1")
    finally:
        approval.clear_session("scope-chat")


@pytest.mark.parametrize("actor", ["bot", "internal", "no-user", "no-control"])
@pytest.mark.asyncio
async def test_nonhuman_scope_approval_keeps_request_pending(actor, monkeypatch):
    from gateway.session import SessionSource
    from gateway.config import Platform
    from gateway.platforms.event import MessageEvent
    from gateway.authz_mixin import GatewayAuthorizationMixin

    source = SessionSource(platform=Platform.DISCORD, chat_id="thread", user_id="actor",
                           chat_type="thread", is_bot=actor == "bot")
    event = MessageEvent(text="/approve instruction-15m request-1", source=source,
                         internal=actor == "internal", allow_gateway_control=actor != "no-control")
    if actor == "no-user":
        source.user_id = None
    if actor == "bot":
        # Admission to a shared chat is not permission to approve writes.
        monkeypatch.setattr("gateway.authz_mixin._platform_gate_env", lambda *a: "mentions")
        auth = GatewayAuthorizationMixin()
        assert auth._chat_scoped_grant(source, None, False, False)
    runner = GatewaySlashCommandsMixin()
    runner._pending_approvals = {}
    runner._session_key_for_source = lambda source: "scope-chat"
    runner._deliver_approval_confirmation = AsyncMock()
    entry = _ApprovalEntry({"request_id": "request-1", "instruction_scope": {
        "paths": ["/review/AGENTS.md"], "seconds": 900, "worktree": "/review"}})
    with approval._lock:
        approval._gateway_queues["scope-chat"] = [entry]
    try:
        await runner._handle_approve_command(event)
        assert not entry.event.is_set()
        assert entry.result is None
        runner._deliver_approval_confirmation.assert_not_called()
    finally:
        approval.clear_session("scope-chat")


def test_temporary_choice_cannot_resolve_unrelated_request():
    entry = _ApprovalEntry({"request_id": "other", "command": "rm -rf example"})
    with approval._lock:
        approval._gateway_queues["scope-chat"] = [entry]
    try:
        assert approval.resolve_gateway_approval("scope-chat", "instruction_15m", request_id="other") == 0
        assert not entry.event.is_set()
    finally:
        approval.clear_session("scope-chat")
