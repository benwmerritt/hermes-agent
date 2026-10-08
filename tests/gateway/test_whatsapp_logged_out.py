"""The WhatsApp bridge's logged-out exit is terminal, not a crash.

Field incident: after WhatsApp removed the linked device, the bridge exited 1 on
``DisconnectReason.loggedOut`` and the gateway respawned it against the dead session on every
reconnect tick (~250 restarts a day, 8,837 "Logged out" log lines) while scheduled sends failed
silently for weeks. The bridge now exits ``_BRIDGE_EXIT_LOGGED_OUT``; the adapter maps it to a
non-retryable ``whatsapp_logged_out`` fatal (the runner drops non-retryable platforms instead of
queueing them) and posts one re-pair notice to the profile's other home channels.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter
from gateway.run import GatewayRunner
from hermes_constants import get_routing_process_hermes_home
from plugins.platforms.whatsapp.adapter import _BRIDGE_EXIT_LOGGED_OUT, WhatsAppAdapter


def _home(chat_id):
    return SimpleNamespace(chat_id=chat_id, thread_id=None, user_id=None, scope_id=None)


def _runner(transports, served_homes=None):
    """The runner surface the notice path reads: served home channels and the shared send helper."""
    return SimpleNamespace(
        _served_home_channel_transports=lambda: iter(transports),
        _send_home_channel_message=AsyncMock(return_value=True),
        _served_profile_homes=served_homes or {},
        _stop_requested_by_signal=False,
    )


def _make_adapter(runner):
    """A WhatsAppAdapter bound to *runner* with just the state the bridge-exit paths touch (bypass __init__)."""
    adapter = WhatsAppAdapter.__new__(WhatsAppAdapter)
    adapter.platform = Platform.WHATSAPP
    adapter.config = MagicMock()
    adapter.gateway_runner = runner
    adapter._profile_home = get_routing_process_hermes_home()
    adapter._bridge_log = "/tmp/test-wa-bridge.log"
    adapter._bridge_log_fh = MagicMock()
    adapter._shutting_down = False
    adapter._running = True
    adapter._http_session = None
    adapter._fatal_error_code = adapter._fatal_error_message = None
    adapter._fatal_error_retryable = True
    adapter._fatal_error_handler = None
    adapter._message_queue = asyncio.Queue()
    return adapter


def _exited(returncode):
    return MagicMock(**{"poll.return_value": returncode, "returncode": returncode})


@pytest.mark.asyncio
async def test_logged_out_exit_at_runtime_is_terminal_and_alerts_other_home_channels_once(tmp_path):
    slack_home = _home("C-OPS")
    runner = _runner([
        (None, Platform.SLACK, None, slack_home, object()),
        (None, Platform.WHATSAPP, None, _home("wa-group"), object()),  # the platform that is down
        ("other", Platform.TELEGRAM, None, _home("t-other"), object()),  # another profile's home
    ], served_homes={"other": tmp_path / "other"})
    adapter = _make_adapter(runner)
    fatal_handler = AsyncMock()
    adapter.set_fatal_error_handler(fatal_handler)
    adapter._bridge_process = _exited(_BRIDGE_EXIT_LOGGED_OUT)

    message = await adapter._check_managed_bridge_exit()

    assert adapter.fatal_error_code == "whatsapp_logged_out"
    assert adapter.fatal_error_retryable is False
    assert "hermes whatsapp" in message
    fatal_handler.assert_awaited_once()
    send = runner._send_home_channel_message
    send.assert_awaited_once()
    platform, home, _transport, text, _failure_fmt = send.await_args.args
    assert platform is Platform.SLACK and home is slack_home
    assert "hermes whatsapp" in text and "gateway restart" in text

    # The poll loop and queued sends observe the same exit again: no second notice, no second teardown.
    assert await adapter._check_managed_bridge_exit() == message
    send.assert_awaited_once()
    fatal_handler.assert_awaited_once()


@pytest.mark.asyncio
async def test_generic_bridge_exit_stays_retryable_with_no_notice():
    runner = _runner([(None, Platform.SLACK, None, _home("C-OPS"), object())])
    adapter = _make_adapter(runner)
    adapter.set_fatal_error_handler(AsyncMock())
    adapter._bridge_process = _exited(1)

    await adapter._check_managed_bridge_exit()

    assert adapter.fatal_error_code == "whatsapp_bridge_exited"
    assert adapter.fatal_error_retryable is True
    runner._send_home_channel_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_logged_out_exit_while_waiting_for_the_bridge_is_not_retried():
    """A gateway (re)started on a revoked session: the fresh bridge exits before /health answers."""
    runner = _runner([(None, Platform.SLACK, None, _home("C-OPS"), object())])
    adapter = _make_adapter(runner)
    adapter._bridge_process = _exited(_BRIDGE_EXIT_LOGGED_OUT)

    with patch("plugins.platforms.whatsapp.adapter.asyncio.sleep", new_callable=AsyncMock):
        assert await adapter._wait_for_bridge() is False

    assert adapter.fatal_error_code == "whatsapp_logged_out"
    assert adapter.fatal_error_retryable is False
    runner._send_home_channel_message.assert_awaited_once()
    assert adapter._bridge_log_fh is None


class _LoggedOutAdapter(BasePlatformAdapter):
    def __init__(self):
        super().__init__(PlatformConfig(enabled=True, token="token"), Platform.WHATSAPP)

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        return True

    async def disconnect(self) -> None:
        self._mark_disconnected()

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        raise NotImplementedError

    async def get_chat_info(self, chat_id):
        return {"id": chat_id}


@pytest.mark.asyncio
async def test_runner_drops_logged_out_whatsapp_instead_of_queueing_it(tmp_path):
    """The respawn loop was the reconnect watcher retrying a retryable fatal; logged out never enters its queue."""
    config = GatewayConfig(
        platforms={Platform.WHATSAPP: PlatformConfig(enabled=True, token="token")},
        sessions_dir=tmp_path / "sessions",
    )
    runner = GatewayRunner(config)
    adapter = _LoggedOutAdapter()
    adapter._set_fatal_error("whatsapp_logged_out", "logged out", retryable=False)
    other = MagicMock(name="slack")  # another platform keeps the gateway alive
    runner.adapters = {Platform.WHATSAPP: adapter, Platform.SLACK: other}
    runner.delivery_router.adapters = runner.adapters
    runner.stop = AsyncMock()

    await runner._handle_adapter_fatal_error(adapter)

    assert Platform.WHATSAPP not in runner._failed_platforms
    assert runner.adapters == {Platform.SLACK: other}
    runner.stop.assert_not_awaited()
