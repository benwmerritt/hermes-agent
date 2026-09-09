"""A Relay button may authorize only the native request that created it."""

import asyncio
from types import SimpleNamespace

import pytest

from gateway.run_turn_runner import TurnRunner
from tests.gateway.relay.test_relay_interactive import _adapter, _event
from tools import approval
from tools.approval_gateway_wait import _await_gateway_decision


@pytest.mark.asyncio
async def test_stale_button_cannot_resolve_new_approval_in_same_session():
    adapter, stub = _adapter()
    metadata = {"thread_id": "thread-1"}
    runner = object.__new__(TurnRunner)
    runner._ctx = SimpleNamespace(
        _status_adapter=adapter,
        _status_chat_id="c1",
        _status_thread_metadata=metadata,
        session_key="binding-session",
    )
    runner._close_native_stream_boundary = lambda reason: None
    loop = asyncio.get_running_loop()
    runner._schedule = lambda coroutine, label: asyncio.run_coroutine_threadsafe(
        coroutine, loop
    )
    notified = asyncio.Queue()

    def notify(data):
        runner._approval_notify_sync(data)
        loop.call_soon_threadsafe(notified.put_nowait, data)

    async def start(command):
        task = asyncio.create_task(
            asyncio.to_thread(
                _await_gateway_decision,
                "binding-session",
                notify,
                {"command": command, "description": "fixture"},
            )
        )
        data = await asyncio.wait_for(notified.get(), 5)
        prompt = next(
            action for action in reversed(stub.sent) if action["op"] == "prompt"
        )
        return task, data, prompt["prompt_id"]

    tasks = []
    try:
        old, old_data, old_prompt = await start("old fixture command")
        tasks.append(old)
        assert (
            approval.resolve_gateway_approval(
                "binding-session", "deny", request_id=old_data["request_id"]
            )
            == 1
        )
        assert (await old)["choice"] == "deny"
        new, new_data, new_prompt = await start("new different fixture command")
        tasks.append(new)
        assert old_data["request_id"] != new_data["request_id"]
        await adapter._consume_prompt_response(
            _event({"prompt_id": old_prompt, "option_id": "once"})
        )
        pending = approval.list_gateway_approvals("binding-session")
        assert [row["request_id"] for row in pending] == [new_data["request_id"]]
        assert metadata == {"thread_id": "thread-1"}, (
            "notify must not modify the turn's shared metadata"
        )
        await adapter._consume_prompt_response(
            _event({"prompt_id": new_prompt, "option_id": "once"})
        )
        assert (await asyncio.wait_for(new, 5))["choice"] == "once"
    finally:
        approval.resolve_gateway_approval("binding-session", "deny", resolve_all=True)
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_legacy_prompt_without_request_id_never_falls_back_to_fifo():
    adapter, stub = _adapter()
    notified = asyncio.Event()
    loop = asyncio.get_running_loop()
    task = asyncio.create_task(
        asyncio.to_thread(
            _await_gateway_decision,
            "legacy-session",
            lambda data: loop.call_soon_threadsafe(notified.set),
            {"command": "new waiting command"},
        )
    )
    try:
        await asyncio.wait_for(notified.wait(), 5)
        await adapter.send_exec_approval("c1", "old unbound command", "legacy-session")
        prompt_id = stub.sent[-1]["prompt_id"]
        await adapter._consume_prompt_response(
            _event({"prompt_id": prompt_id, "option_id": "always"})
        )
        assert len(approval.list_gateway_approvals("legacy-session")) == 1
    finally:
        approval.resolve_gateway_approval("legacy-session", "deny", resolve_all=True)
        await asyncio.wait_for(task, 5)
