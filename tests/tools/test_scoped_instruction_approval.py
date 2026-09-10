"""Exact, human-selected review-worktree permission, through the real file guard."""
import json
import subprocess
from pathlib import Path

import pytest

from tools import approval, approval_context
from tools.file_tools_write_guards import _check_protected_instruction_write


@pytest.fixture
def review(tmp_path, monkeypatch):
    main = tmp_path / "main"
    tree = tmp_path / "review"
    subprocess.run(["git", "init", str(main)], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(main), "-c", "user.name=Test", "-c",
                    "user.email=test@example.invalid", "commit", "--allow-empty", "-m", "fixture"],
                   check=True, capture_output=True)
    subprocess.run(["git", "-C", str(main), "worktree", "add", "-b", "review", str(tree)],
                   check=True, capture_output=True)
    monkeypatch.setenv("TERMINAL_ENV", "local")
    monkeypatch.setenv("TERMINAL_CWD", str(tree))
    monkeypatch.chdir(tree)
    token = approval_context.set_current_session_key("review-chat")
    ids = approval_context.set_current_observability_context(session_id="review-session")
    state = {"calls": [], "choice": "once"}

    def notify(data):
        state["calls"].append(data)
        approval.resolve_gateway_approval("review-chat", state["choice"], request_id=data["request_id"])

    notify.supports_instruction_scope = True
    approval.register_gateway_notify("review-chat", notify)
    yield tree, main, state
    approval.unregister_gateway_notify("review-chat")
    approval.clear_session("review-chat")
    approval_context.reset_current_observability_context(ids)
    approval_context.reset_current_session_key(token)


def test_once_still_reprompts_in_linked_worktree(review):
    tree, _, state = review
    target = str(tree / "AGENTS.md")
    assert _check_protected_instruction_write([target], "review-session") is None
    assert _check_protected_instruction_write([target], "review-session") is None
    assert len(state["calls"]) == 2


def test_explicit_temporary_choice_covers_repeated_exact_edits(review):
    tree, _, state = review
    state["choice"] = "instruction_15m"
    target = str(tree / "AGENTS.md")
    assert _check_protected_instruction_write([target], "review-session") is None
    assert _check_protected_instruction_write([target], "review-session") is None
    assert len(state["calls"]) == 1
    request = state["calls"][0]
    assert request["instruction_scope"]["paths"] == [target]
    assert request["instruction_scope"]["seconds"] == 900
    assert request["allow_session"] is False
    assert request["allow_permanent"] is False



@pytest.mark.parametrize("boundary", ["file", "worktree", "session", "child", "expiry", "cleanup", "pid"])
def test_grants_do_not_cross_boundaries(review, monkeypatch, boundary):
    from tools import approval_instruction_scope as temporary
    tree, main, state = review
    target = str(tree / "AGENTS.md")
    state["choice"] = "instruction_15m"
    assert _check_protected_instruction_write([target], "review-session") is None
    state["choice"] = "deny"
    task = "review-session"
    ids = None
    if boundary == "file":
        target = str(tree / "other" / "AGENTS.md")
    elif boundary == "worktree":
        other = tree.parent / "other-review"
        subprocess.run(["git", "-C", str(main), "worktree", "add", "-b", "other", str(other)],
                       check=True, capture_output=True)
        monkeypatch.setenv("TERMINAL_CWD", str(other))
        target = str(other / "AGENTS.md")
    elif boundary == "session":
        ids = approval_context.set_current_observability_context(session_id="new-session")
    elif boundary == "child":
        task = "child-task"
        ids = approval_context.set_current_observability_context(session_id="child-session")
    elif boundary == "expiry":
        now = temporary.time.monotonic()
        monkeypatch.setattr(temporary.time, "monotonic", lambda: now + 901)
    elif boundary == "cleanup":
        approval.clear_session("review-chat")
    elif boundary == "pid":
        monkeypatch.setattr(temporary.os, "getpid", lambda: -1)
    try:
        assert "BLOCKED" in _check_protected_instruction_write([target], task)
        assert len(state["calls"]) == 2
    finally:
        if ids is not None:
            approval_context.reset_current_observability_context(ids)


@pytest.mark.parametrize("choice", ["once", "session", "always", "deny"])
def test_broad_or_once_choices_never_mint_temporary_scope(review, monkeypatch, choice):
    from tools import approval_instruction_scope as temporary
    tree, _, state = review
    monkeypatch.setattr(approval, "_YOLO_MODE_FROZEN", True)
    monkeypatch.setattr(approval_context, "_get_approval_mode", lambda: "off")
    approval.approve_session("review-chat", "protected_instruction_file")
    state["choice"] = choice
    target = str(tree / "AGENTS.md")
    _check_protected_instruction_write([target], "review-session")
    assert not temporary.is_approved(temporary.candidate([target], "review-session"))
    state["choice"] = "deny"
    assert "BLOCKED" in _check_protected_instruction_write([target], "review-session")
    assert len(state["calls"]) == 2


@pytest.mark.parametrize("failure", ["timeout", "unavailable", "delivery", "cleanup-during-answer"])
def test_missing_consent_fails_closed(review, monkeypatch, failure):
    from tools import approval_instruction_scope as temporary
    from tools.terminal_tool import set_approval_callback
    tree, _, state = review
    target = str(tree / "AGENTS.md")
    if failure == "timeout":
        monkeypatch.setattr(approval_context, "_get_approval_timeout", lambda: 0)
        approval.register_gateway_notify("review-chat", lambda data: None)
    elif failure == "unavailable":
        approval.unregister_gateway_notify("review-chat")
        set_approval_callback(None)
    elif failure == "delivery":
        def notify(data):
            raise RuntimeError("undeliverable")
        approval.register_gateway_notify("review-chat", notify)
    else:
        def notify(data):
            assert approval.resolve_gateway_approval("review-chat", "instruction_15m", request_id=data["request_id"]) == 1
            approval.clear_session("review-chat")
        notify.supports_instruction_scope = True
        approval.register_gateway_notify("review-chat", notify)
    result = _check_protected_instruction_write([target], "review-session")
    assert "BLOCKED" in result
    assert not temporary.is_approved(temporary.candidate([target], "review-session"))
    assert not approval.has_blocking_approval("review-chat")


@pytest.mark.parametrize("invalid", ["main", "outside", "live-home", "symlink", "remote", "unknown-session"])
def test_ineligible_targets_never_offer_temporary_scope(review, monkeypatch, invalid):
    from tools import approval_instruction_scope as temporary
    tree, main, state = review
    target = tree / "AGENTS.md"
    ids = None
    if invalid == "main":
        monkeypatch.setenv("TERMINAL_CWD", str(main))
        target = main / "AGENTS.md"
    elif invalid == "outside":
        target = tree.parent / "AGENTS.md"
    elif invalid == "live-home":
        monkeypatch.setenv("HERMES_HOME", str(tree))
    elif invalid == "symlink":
        real = tree.parent / "target"
        real.mkdir()
        (tree / "alias").symlink_to(real, target_is_directory=True)
        target = tree / "alias" / "AGENTS.md"
    elif invalid == "remote":
        monkeypatch.setattr(temporary, "_terminal_env_type_for_task", lambda task: "ssh")
    else:
        ids = approval_context.set_current_observability_context()
    try:
        assert temporary.candidate([str(target)], "review-session") is None
        state["choice"] = "once"
        _check_protected_instruction_write([str(target)], "review-session")
        assert all("instruction_scope" not in call for call in state["calls"])
    finally:
        if ids is not None:
            approval_context.reset_current_observability_context(ids)


def test_symlink_retarget_during_and_after_consent(review):
    tree, _, state = review
    target = tree / "AGENTS.md"
    outside = tree.parent / "outside" / "AGENTS.md"
    outside.parent.mkdir()
    outside.write_text("unchanged")
    state["choice"] = "instruction_15m"
    assert _check_protected_instruction_write([str(target)], "review-session") is None
    target.symlink_to(outside)
    state["choice"] = "deny"
    assert "BLOCKED" in _check_protected_instruction_write([str(target)], "review-session")
    assert "instruction_scope" not in state["calls"][-1]
    target.unlink()
    approval.clear_session("review-chat")

    def notify(data):
        target.symlink_to(outside)
        approval.resolve_gateway_approval("review-chat", "instruction_15m", request_id=data["request_id"])

    notify.supports_instruction_scope = True
    approval.register_gateway_notify("review-chat", notify)
    assert "scope changed" in _check_protected_instruction_write([str(target)], "review-session")
    assert outside.read_text() == "unchanged"


def test_real_tool_dispatch_write_replace_and_atomic_multifile_patch(review):
    from model_tools import handle_function_call
    tree, _, state = review
    first = tree / "AGENTS.md"
    second = tree / "sub" / "AGENTS.md"
    second.parent.mkdir()
    first.write_text("before\n")
    second.write_text("before\n")
    state["choice"] = "instruction_15m"

    def call(name, args):
        result = json.loads(handle_function_call(name, args, task_id="review-session", session_id="review-session"))
        assert not result.get("error"), result
        return result

    patch = "*** Begin Patch\n" + "".join(
        f"*** Update File: {p}\n@@\n-before\n+after\n" for p in (first, second)) + "*** End Patch"
    call("patch", {"mode": "patch", "patch": patch})
    call("write_file", {"path": str(first), "content": "again\n"})
    call("patch", {"mode": "replace", "path": str(second), "old_string": "after", "new_string": "again"})
    assert first.read_text() == second.read_text() == "again\n"
    assert len(state["calls"]) == 1
    assert set(state["calls"][0]["instruction_scope"]["paths"]) == {str(first), str(second)}

    third = tree / "SOUL.md"
    third.write_text("before\n")
    state["choice"] = "deny"
    patch = (f"*** Begin Patch\n*** Update File: {first}\n@@\n-again\n+denied\n"
             f"*** Update File: {third}\n@@\n-before\n+denied\n*** End Patch")
    result = json.loads(handle_function_call("patch", {"mode": "patch", "patch": patch},
                                            task_id="review-session", session_id="review-session"))
    assert "BLOCKED" in result["error"]
    assert first.read_text() == "again\n" and third.read_text() == "before\n"



def test_unaware_gateway_cannot_offer_new_scope(review):
    tree, _, state = review
    def legacy_notify(data):
        state["calls"].append(data)
        approval.resolve_gateway_approval("review-chat", "once", request_id=data["request_id"])
    approval.register_gateway_notify("review-chat", legacy_notify)
    assert _check_protected_instruction_write([str(tree / "AGENTS.md")], "review-session") is None
    assert "instruction_scope" not in state["calls"][0]



def test_nested_worktree_is_not_part_of_outer_scope(review):
    from tools import approval_instruction_scope as temporary
    tree, main, state = review
    nested = tree / "nested"
    subprocess.run(["git", "-C", str(main), "worktree", "add", "-b", "nested", str(nested)],
                   check=True, capture_output=True)
    assert temporary.candidate([str(nested / "AGENTS.md")], "review-session") is None


def test_gateway_guard_ui_command_and_tools_end_to_end(review):
    import asyncio
    import concurrent.futures
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock
    from gateway.run_turn_runner import TurnRunner
    from gateway.slash_commands import GatewaySlashCommandsMixin
    from gateway.platforms.base import SendResult
    from model_tools import handle_function_call

    tree, _, state = review
    slash = GatewaySlashCommandsMixin()
    slash._pending_approvals = {}
    slash._session_key_for_source = lambda source: "review-chat"
    slash._deliver_approval_confirmation = AsyncMock(side_effect=lambda event, text, kind: text)
    messages = []

    async def send(chat, text, **kwargs):
        messages.append(text)
        command = text.split("`/approve instruction-15m ")[1].split("`")[0]
        event = SimpleNamespace(source=SimpleNamespace(is_bot=False, user_id="human"),
                                get_command_args=lambda: "instruction-15m " + command)
        reply = await slash._handle_approve_command(event)
        assert "15 minutes" in reply
        return SendResult(success=True)

    runner = TurnRunner.__new__(TurnRunner)
    runner._ctx = SimpleNamespace(_status_adapter=SimpleNamespace(
        pause_typing_for_chat=Mock(), typed_command_prefix="/", send=send),
        _status_chat_id="test-chat", _status_thread_metadata=None, session_key="review-chat")
    runner._close_native_stream_boundary = Mock()
    def schedule(coro, log_message):
        future = concurrent.futures.Future()
        future.set_result(asyncio.run(coro))
        return future
    runner._schedule = schedule
    approval.register_gateway_notify("review-chat", runner._approval_notify_sync)
    target = str(tree / "AGENTS.md")
    for content in ("first", "second"):
        result = json.loads(handle_function_call("write_file", {"path": target, "content": content},
                                                task_id="review-session", session_id="review-session"))
        assert not result.get("error"), result
    assert Path(target).read_text() == "second"
    assert len(messages) == 1
    assert target in messages[0]



def test_worktree_replacement_invalidates_prior_grant(review):
    tree, main, state = review
    target = str(tree / "AGENTS.md")
    state["choice"] = "instruction_15m"
    assert _check_protected_instruction_write([target], "review-session") is None
    moved = tree.parent / "old-review"
    subprocess.run(["git", "-C", str(main), "worktree", "move", str(tree), str(moved)],
                   check=True, capture_output=True)
    subprocess.run(["git", "-C", str(main), "worktree", "add", "-b", "replacement", str(tree)],
                   check=True, capture_output=True)
    state["choice"] = "deny"
    assert "BLOCKED" in _check_protected_instruction_write([target], "review-session")
    assert len(state["calls"]) == 2


def test_cli_autoapprove_callback_cannot_mint_scope(review):
    from tools.terminal_tool import set_approval_callback
    from tools import approval_instruction_scope as temporary
    tree, _, state = review
    approval.unregister_gateway_notify("review-chat")
    set_approval_callback(lambda *args, **kwargs: "instruction_15m")
    target = str(tree / "AGENTS.md")
    try:
        assert "BLOCKED" in _check_protected_instruction_write([target], "review-session")
        assert not temporary.is_approved(temporary.candidate([target], "review-session"))
    finally:
        set_approval_callback(None)
