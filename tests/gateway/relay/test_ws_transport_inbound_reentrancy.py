"""Inbound controls must not prevent the socket from receiving their own replies."""
import asyncio
import json

import pytest
import websockets

from gateway.config import PlatformConfig
from gateway.relay.adapter import RelayAdapter
from gateway.relay.descriptor import CapabilityDescriptor
from gateway.relay.ws_transport import WebSocketRelayTransport
from gateway.session import build_session_key


@pytest.mark.asyncio
async def test_busy_control_reply_and_following_inbound_share_live_socket():
    descriptor = CapabilityDescriptor(
        contract_version=1, platform="discord", label="Discord", max_message_length=2000,
        supports_draft_streaming=False, supports_edit=True, supports_threads=True,
        markdown_dialect="discord", len_unit="chars",
    )
    source = {"platform": "discord", "chat_id": "thread", "chat_type": "thread",
              "thread_id": "thread", "user_id": "owner", "scope_id": "guild"}
    from gateway.relay.ws_transport import _event_from_wire
    event = _event_from_wire({"text": "/status", "source": source})
    completed = asyncio.Event()
    acknowledgments = []
    received = []

    async def serve(ws):
        async for raw in ws:
            for line in raw.splitlines():
                frame = json.loads(line)
                if frame["type"] == "hello":
                    await ws.send(json.dumps({"type": "descriptor", "descriptor": json.loads(descriptor.to_json())}) + "\n")
                    for index in range(2):
                        await ws.send(json.dumps({"type": "inbound", "bufferId": str(index),
                            "event": {"text": "/status", "message_type": "command", "message_id": str(index), "source": source}}) + "\n")
                elif frame["type"] == "outbound":
                    received.append(frame["action"])
                    await ws.send(json.dumps({"type": "outbound_result", "requestId": frame["requestId"],
                        "result": {"success": True, "message_id": "reply"}}) + "\n")
                elif frame["type"] == "inbound_ack":
                    acknowledgments.append(frame["bufferId"])
                    if len(acknowledgments) == 2:
                        completed.set()

    async with websockets.serve(serve, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        transport = WebSocketRelayTransport(f"ws://127.0.0.1:{port}", "discord", "bot", outbound_timeout_s=30)
        adapter = RelayAdapter(PlatformConfig(), descriptor, transport)
        async def handler(inbound):
            return "Still working"
        adapter.set_message_handler(handler)
        adapter._active_sessions[build_session_key(event.source)] = asyncio.Event()
        try:
            assert await adapter.connect()
            await asyncio.wait_for(completed.wait(), timeout=3)
            assert acknowledgments == ["0", "1"]
            assert [action["content"] for action in received] == ["Still working", "Still working"]
        finally:
            await transport.disconnect(budget_s=0)
