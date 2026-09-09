import argparse
import contextvars
import json
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from gateway.session_context import _CRON_AUTO_DELIVER_PLATFORM, _CRON_AUTO_DELIVER_CHAT_ID, _CRON_AUTO_DELIVER_THREAD_ID
from hermes_cli.send_cmd import cmd_send
from tools.environments.local import _inject_session_context_env
from tools.send_message_tool import _maybe_skip_cron_duplicate_send


def test_cli_owner_then_bridge_delivery(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    monkeypatch.setenv('HERMES_CRON_AUTO_DELIVER_PLATFORM', 'photon')
    monkeypatch.setenv('HERMES_CRON_AUTO_DELIVER_CHAT_ID', '+15551234567')
    monkeypatch.setenv('HERMES_CRON_AUTO_DELIVER_THREAD_ID', '')
    # Real target parser, CLI env loader, send handler and ownership guard.
    # Only platform configuration and the network boundary are fixtures.
    with patch('gateway.config.load_gateway_config'), patch('tools.send_message_tool._resolve_platform_config', return_value=('photon', SimpleNamespace(token=None), None, None)), patch('tools.send_message_tool._home_chat_id', return_value=('+15551234567',None)), patch('tools.send_message_tool._mirror_sent_message',return_value=False), patch('tools.send_message_tool._send_to_platform', new_callable=AsyncMock, return_value={'success':True,'message_id':'fixture'}) as transport:
        for target in ['photon', 'photon:+15551234567', 'photon:any;-;+15551234567']:
            try:
                cmd_send(argparse.Namespace(to=target,message='fixture',json=True))
            except SystemExit as result:
                assert result.code == 0
            assert json.loads(capsys.readouterr().out)['skipped'] is True
        transport.assert_not_called()
        monkeypatch.delenv('HERMES_CRON_AUTO_DELIVER_PLATFORM')
        monkeypatch.delenv('HERMES_CRON_AUTO_DELIVER_CHAT_ID')
        try:
            cmd_send(argparse.Namespace(to='photon:+15551234567',message='final fixture',json=True))
        except SystemExit as result:
            assert result.code == 0
        assert json.loads(capsys.readouterr().out)['message_id'] == 'fixture'
        transport.assert_awaited_once()


def test_scoped_owner_survives_worker_context_without_leaking():
    def owner_work():
        _CRON_AUTO_DELIVER_PLATFORM.set('photon')
        _CRON_AUTO_DELIVER_CHAT_ID.set('+15551234567')
        _CRON_AUTO_DELIVER_THREAD_ID.set('')
        def child():
            env={}
            _inject_session_context_env(env)
            return env, _maybe_skip_cron_duplicate_send('photon','any;-;+15551234567',None)
        with ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(contextvars.copy_context().run, child).result()
    env, skipped=contextvars.Context().run(owner_work)
    assert env['HERMES_CRON_AUTO_DELIVER_CHAT_ID']=='+15551234567'
    assert skipped['skipped'] is True
    assert contextvars.Context().run(_maybe_skip_cron_duplicate_send,'photon','+15551234567',None) is None
