"""A parked worker settles native delivery receipts before disconnecting Relay."""
import asyncio
from types import SimpleNamespace

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platforms.event import MessageEvent
from gateway.relay.descriptor import CapabilityDescriptor
from gateway.session import SessionSource, build_session_key
from hermes_cluster.worker_runtime import ConversationGateway, ConversationPolicy, ConversationRelayAdapter


@pytest.mark.asyncio
@pytest.mark.parametrize('acknowledged', [True, False])
async def test_park_drains_delivery_or_preserves_uncertainty(tmp_path, monkeypatch, acknowledged):
    from gateway import delivery_ledger as ledger

    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    source = SessionSource(platform=Platform.DISCORD, chat_id='thread', chat_type='thread',
                           thread_id='thread', user_id='owner')
    policy = ConversationPolicy({'source': source.to_dict(), 'conversation_key': build_session_key(source),
                                 'allowed_user_ids': ['owner']})
    sent, release, waiting = asyncio.Event(), asyncio.Event(), asyncio.Event()
    disconnect_states = []
    sends = []

    def states():
        with ledger._connect() as db:
            return [row[0] for row in db.execute('SELECT state FROM delivery_obligations')]

    class Transport:
        async def send_outbound(self, action, **kwargs):
            if action['op'] == 'send':
                sends.append(action['content'])
                sent.set()
                await release.wait()
            return {'success': True, 'message_id': 'delivered-1'}

        async def disconnect(self, **kwargs):
            disconnect_states.append(states())

    descriptor = CapabilityDescriptor(contract_version=1, platform='discord', label='Discord',
        max_message_length=2000, supports_draft_streaming=False, supports_edit=True,
        supports_threads=True, markdown_dialect='discord', len_unit='chars')
    adapter = ConversationRelayAdapter(PlatformConfig(), descriptor, Transport(), policy)
    adapter.config.typing_indicator = False
    adapter.set_message_handler(lambda event: asyncio.sleep(0, result='Gateway is shutting down.'))
    runner = object.__new__(ConversationGateway)
    runner._exit_code = 0
    runner._exit_reason = None
    runner.adapters = {Platform.RELAY: adapter}

    def cleanup_budget():
        waiting.set()
        return 2.0

    runner._adapter_disconnect_timeout_secs = cleanup_budget
    event = MessageEvent(text='an already admitted message', source=source, message_id='input-1')
    await adapter.handle_message(event)
    await asyncio.wait_for(sent.wait(), 5)
    assert states() == ['attempting']
    teardown = asyncio.create_task(runner._bounded_adapter_teardown(adapter, Platform.RELAY))
    await asyncio.wait_for(waiting.wait(), 5)
    if acknowledged:
        release.set()
    await asyncio.wait_for(teardown, 10)
    assert len(sends) == 1, 'shutdown must never replay the external send'
    expected = 'delivered' if acknowledged else 'attempting'
    assert states() == [expected]
    assert disconnect_states == [[expected]]
    assert runner.exit_code == (0 if acknowledged else 1)


@pytest.mark.asyncio
async def test_worker_exit_includes_teardown_failure(tmp_path, monkeypatch):
    from hermes_cluster import worker, worker_runtime

    class Runner:
        _running = True
        exit_code = 0
        adapters = {Platform.RELAY: SimpleNamespace(is_connected=True)}

        def __init__(self, policy):
            pass

        async def start(self):
            return True

        async def wait_for_shutdown(self):
            return None

        async def stop(self):
            self.exit_code = 1

    monkeypatch.setattr(worker_runtime, 'ConversationGateway', Runner)
    monkeypatch.setattr(worker_runtime, 'ConversationPolicy', lambda config: config)
    config = {'hermes_home': str(tmp_path), 'worker_id': 'one', 'generation': 1,
              'conversation_key': 'thread'}
    knowledge = SimpleNamespace(snapshot_id='snapshot', flush_history=lambda path: None)
    assert await worker._run(config, knowledge) == 1
