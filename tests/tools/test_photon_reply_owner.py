import json
from types import SimpleNamespace
from unittest.mock import patch, AsyncMock
import pytest

from gateway.session_context import _CRON_AUTO_DELIVER_PLATFORM, _CRON_AUTO_DELIVER_CHAT_ID, _CRON_AUTO_DELIVER_THREAD_ID
from tools.send_message_tool import send_message_tool, _maybe_skip_cron_duplicate_send

@pytest.fixture
def owner():
    fields = [(_CRON_AUTO_DELIVER_PLATFORM, 'photon'), (_CRON_AUTO_DELIVER_CHAT_ID, '+15551234567'), (_CRON_AUTO_DELIVER_THREAD_ID, '')]
    tokens = [(v, v.set(x)) for v,x in fields]
    yield
    for v,t in reversed(tokens): v.reset(t)

@pytest.mark.parametrize('chat', ['+15551234567', 'any;-;+15551234567'])
def test_same_photon_recipient_never_reaches_transport(owner, chat):
    with patch('tools.send_message_tool._resolve_tool_target', return_value=('photon', chat, None, None)), patch('gateway.config.load_gateway_config'), patch('tools.send_message_tool._resolve_platform_config', return_value=('photon', SimpleNamespace(token=None), None, None)), patch('tools.send_message_tool._send_to_platform', new_callable=AsyncMock) as send:
        result=json.loads(send_message_tool({'action':'send','target':'photon:'+chat,'message':'fixture'}))
    assert result.get('skipped') is True
    send.assert_not_called()

@pytest.mark.parametrize('chat', ['+15557654321', 'any;-;+15557654321', 'any;-;+15551234567junk', 'any;-;15551234567'])
def test_different_or_malformed_targets_not_reinterpreted(owner, chat):
    assert _maybe_skip_cron_duplicate_send('photon',chat,None) is None


def test_other_platform_and_thread_are_not_suppressed(owner):
    assert _maybe_skip_cron_duplicate_send('signal','+15551234567',None) is None
    assert _maybe_skip_cron_duplicate_send('photon','+15551234567','other') is None
