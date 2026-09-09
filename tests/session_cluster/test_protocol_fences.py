"""Protocol edge cases must not widen a revoked conversation's authority."""

import asyncio
import json
import threading
from types import SimpleNamespace

import httpx
import pytest
from fastapi import WebSocketDisconnect

from gateway.relay.auth import make_upgrade_token
from hermes_cluster.app import create_app
from hermes_cluster.controller import Controller
from hermes_cluster.ledger import Ledger
from hermes_cluster.media import MediaStore
from hermes_cluster.relay_server import create_relay_router


@pytest.mark.asyncio
async def test_shutdown_ack_preserves_revoked_owner(tmp_path):
    ledger = Ledger(tmp_path / 'router.sqlite')
    owner, _ = ledger.admit(agent_id='test', native_key='one',
                            source={'scope_id': 'guild', 'chat_id': 'one', 'user_id': 'allowed'},
                            audience='private', event_id='first', payload={'text': 'hello'}, actor='allowed')
    ledger.transition(owner['id'], 1, 'ready')
    token = make_upgrade_token(f"{owner['id']}:1", owner['relay_secret'])
    sent = []

    class Socket:
        headers = {'authorization': 'Bearer ' + token}
        receives = 0

        async def accept(self):
            pass

        async def send_text(self, text):
            sent.append(json.loads(text))

        async def receive_text(self):
            self.receives += 1
            if self.receives == 1:
                return json.dumps({'type': 'hello', 'platform': 'discord', 'botId': '1'})
            if self.receives == 2:
                ledger.transition(owner['id'], 1, 'recovery_required', detail='audience revoked')
                return json.dumps({'type': 'going_idle'})
            raise WebSocketDisconnect()

    async def connect(connection):
        controller.connections[owner['id']] = connection

    async def disconnect(connection):
        controller.connections.pop(owner['id'])

    controller = SimpleNamespace(ledger=ledger, config={'bot_id': '1'}, connections={},
                                 connect_owner=connect, disconnect_owner=disconnect,
                                 wake=SimpleNamespace(set=lambda: None))
    await create_relay_router(controller).routes[0].endpoint(Socket())
    assert sent[-1]['type'] == 'going_idle_ack'
    assert ledger.get(owner['id'])['status'] == 'recovery_required'
    assert ledger.get(owner['id'])['detail'] == 'audience revoked'


@pytest.mark.asyncio
async def test_non_ascii_operator_header_is_forbidden_not_server_error(tmp_path, monkeypatch):
    ledger = Ledger(tmp_path / 'router.sqlite')
    controller = SimpleNamespace(ledger=ledger, media=MediaStore(tmp_path / 'media', ledger), knowledge=None)
    monkeypatch.setenv('HERMES_CLUSTER_OPERATOR_TOKEN', 'valid-operator-token-long-enough')
    app = create_app(controller)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
        response = await client.get('/status', headers=[(b'authorization', b'Bearer \xff')])
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_cluster_command_without_subcommand_returns_usage():
    notices = []

    async def audience(event):
        return 'private'

    async def notice(owner, text):
        notices.append(text)

    controller = object.__new__(Controller)
    controller.connector = SimpleNamespace(audience=audience, notice=notice)
    owner = {'actor': 'allowed', 'audience': 'private'}
    event = SimpleNamespace(source=SimpleNamespace(user_id='allowed'), text=' /cluster  ')
    await controller.cluster_command(owner, event)
    assert notices == ['Use /cluster status, /cluster park, or /cluster resume followed by how you checked any uncertain actions.']


@pytest.mark.asyncio
@pytest.mark.parametrize('fence_stage', ['credentials', 'pod'])
async def test_fence_during_provisioning_preserves_reconciliation_and_identity(tmp_path, monkeypatch, fence_stage):
    from hermes_cluster.knowledge.store import KnowledgeError, KnowledgeStore
    from hermes_cluster.kubernetes import WorkerIdentity

    class Connector:
        def __init__(self, *args, **kwargs):
            pass

        async def notice(self, owner, text):
            pass

    class Backend:
        created = False

        async def create_credentials(self, *args):
            if fence_stage == 'credentials':
                await controller.fence_owner(owner, 'audience revoked')

        async def create_worker(self, cid, generation, config, secret, **kwargs):
            self.created = True
            await controller.fence_owner(owner, 'audience revoked')
            return WorkerIdentity(cid, generation, 'pod', 'pod-uid', 'claim', 'config', secret, 'claim-uid')

    monkeypatch.setenv('OPENAI_API_KEY', 'test-only-key')
    backend = Backend()
    knowledge = KnowledgeStore(tmp_path / 'knowledge.sqlite')
    controller = Controller({'data_dir': str(tmp_path / 'controller'), 'relay_url': 'http://test',
                             'bot_id': '1', 'native_config': {}}, backend, knowledge, connector_factory=Connector)
    controller.ledger.admit(agent_id='test', native_key='one',
                           source={'scope_id': 'guild', 'chat_id': 'one', 'user_id': 'allowed'},
                           audience='private', event_id='first', payload={'text': 'hello'}, actor='allowed')
    owner = controller.ledger.reserve_next()
    await controller.provision_or_fence(owner)
    current = controller.ledger.get(owner['id'])
    assert current['status'] == 'recovery_required'
    assert backend.created == (fence_stage == 'pod')
    if backend.created:
        assert current['identity']['pod_uid'] == 'pod-uid'
    with pytest.raises(KnowledgeError, match='revoked'):
        knowledge.snapshot(owner['knowledge_token'])


@pytest.mark.asyncio
async def test_upload_checks_revocation_after_file_write_without_blocking_event_loop(tmp_path, monkeypatch):
    import hermes_cluster.media as media_module
    from fastapi import FastAPI

    ledger = Ledger(tmp_path / 'router.sqlite')
    owner, _ = ledger.admit(agent_id='test', native_key='one',
                            source={'scope_id': 'guild', 'chat_id': 'one', 'user_id': 'allowed'},
                            audience='private', event_id='first', payload={'text': 'hello'}, actor='allowed')
    ledger.transition(owner['id'], 1, 'ready')
    media = MediaStore(tmp_path / 'media', ledger)
    app = FastAPI()
    app.include_router(media.router())
    event_loop_thread = threading.get_ident()
    original = media_module.os.fsync
    threads = []

    def write_finished(fd):
        threads.append(threading.get_ident())
        original(fd)
        ledger.transition(owner['id'], 1, 'recovery_required')

    monkeypatch.setattr(media_module.os, 'fsync', write_finished)
    token = make_upgrade_token(f"{owner['id']}:1", owner['relay_secret'])
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
        response = await client.post('/relay/media', headers={'authorization': 'Bearer ' + token}, content=b'private')
    assert response.status_code == 403
    assert threads and all(t != event_loop_thread for t in threads)
    assert list(media.root.iterdir()) == []


@pytest.mark.asyncio
async def test_outbound_timeout_is_persisted_and_not_retried(tmp_path, monkeypatch):
    import hermes_cluster.relay_server as relay_module

    ledger = Ledger(tmp_path / 'router.sqlite')
    owner, _ = ledger.admit(agent_id='test', native_key='one',
                            source={'scope_id': 'guild', 'chat_id': 'one', 'user_id': 'allowed'},
                            audience='private', event_id='first', payload={'text': 'hello'}, actor='allowed')
    ledger.transition(owner['id'], 1, 'ready')
    token = make_upgrade_token(f"{owner['id']}:1", owner['relay_secret'])
    action = {'type': 'send', 'chat_id': 'one', 'text': 'one result'}
    frames = iter([{'type': 'hello', 'platform': 'discord', 'botId': '1'}] +
                  [{'type': 'outbound', 'requestId': 'same', 'action': action}] * 2)
    sent, attempts = [], []

    class Socket:
        headers = {'authorization': 'Bearer ' + token}

        async def accept(self):
            pass

        async def send_text(self, text):
            sent.append(json.loads(text))

        async def receive_text(self):
            frame = next(frames, None)
            if frame is None:
                raise WebSocketDisconnect()
            return json.dumps(frame)

    async def lifecycle(connection):
        pass

    async def dispatch(owner, action):
        attempts.append(action)
        await asyncio.Event().wait()

    controller = SimpleNamespace(ledger=ledger, config={'bot_id': '1'}, connections={},
                                 connect_owner=lifecycle, disconnect_owner=lifecycle,
                                 connector=SimpleNamespace(dispatch=dispatch), wake=SimpleNamespace(set=lambda: None))
    monkeypatch.setattr(relay_module, 'OUTBOUND_TIMEOUT_S', 0.01)
    await create_relay_router(controller).routes[0].endpoint(Socket())
    results = [f['result'] for f in sent if f['type'] == 'outbound_result']
    assert len(attempts) == 1
    assert len(results) == 2 and all(r['ambiguous'] for r in results)
    assert results[0] == results[1] == ledger.begin_outbound(f"{owner['id']}:1", 'same', action)
