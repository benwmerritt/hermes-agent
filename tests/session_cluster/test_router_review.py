"""Independent regression checks for private media storage and authorization."""
import asyncio
import sqlite3
from types import SimpleNamespace
from dataclasses import asdict

import httpx
import pytest
from fastapi import FastAPI

from gateway.relay.auth import make_upgrade_token
from hermes_cluster.ledger import Ledger, AdmissionError, OwnershipError
from hermes_cluster.media import MediaStore


def owner(ledger, thread):
    source = {'scope_id': 'guild', 'chat_id': thread, 'user_id': 'allowed'}
    row, _ = ledger.admit(agent_id='test', native_key=thread, source=source,
                          audience='private', event_id=thread, payload={'text': 'test'}, actor='allowed')
    ledger.transition(row['id'], row['generation'], 'ready')
    return row


def authorization(row):
    token = make_upgrade_token(f"{row['id']}:{row['generation']}", row['relay_secret'])
    return {'Authorization': 'Bearer ' + token}


def test_failed_media_metadata_transaction_does_not_bypass_disk_accounting(tmp_path):
    ledger = Ledger(tmp_path / 'ledger.sqlite')
    media = MediaStore(tmp_path / 'media', ledger, total_bytes=4, per_conversation_bytes=4)
    row = owner(ledger, 'one')
    with ledger.transaction() as db:
        db.execute("CREATE TRIGGER reject_media BEFORE INSERT ON media BEGIN SELECT RAISE(ABORT, 'injected storage failure'); END")
    with pytest.raises(sqlite3.IntegrityError, match='injected storage failure'):
        media.put(row['id'], b'abcd')
    # A failed upload must leave neither a visible attachment nor unaccounted bytes.
    assert list(media.root.iterdir()) == []


@pytest.mark.asyncio
async def test_media_requires_live_owner_credential_and_conversation_match(tmp_path):
    ledger = Ledger(tmp_path / 'ledger.sqlite')
    media = MediaStore(tmp_path / 'media', ledger)
    a, b = owner(ledger, 'one'), owner(ledger, 'two')
    media_id = media.put(a['id'], b'private content', 'private.txt', 'text/plain')
    app = FastAPI()
    app.include_router(media.router())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='https://companion.test') as client:
        path = f'/relay/media/{media_id}'
        assert (await client.get(path)).status_code == 403
        assert (await client.get(path, headers=authorization(b))).status_code == 404
        response = await client.get(path, headers=authorization(a))
        assert response.status_code == 200 and response.content == b'private content'
        assert response.headers['cache-control'] == 'no-store'
        with ledger.transaction() as db:
            db.execute('UPDATE conversations SET generation=generation+1 WHERE id=?', (a['id'],))
        assert (await client.get(path, headers=authorization(a))).status_code == 403


def test_completed_ingress_stops_at_retained_budget_and_keeps_dedupe(tmp_path):
    ledger = Ledger(tmp_path / 'ledger.sqlite', max_queued=1, max_ledger_bytes=1024 * 1024)
    source = {'scope_id': 'guild', 'chat_id': 'one', 'user_id': 'allowed'}
    arguments = dict(agent_id='test', native_key='one', source=source, audience='private',
                     payload={'text': 'x' * 100000}, actor='allowed')
    completed = 0
    for index in range(30):
        try:
            row, _ = ledger.admit(event_id=str(index), **arguments)
        except AdmissionError as exc:
            assert 'retained ledger budget' in str(exc)
            break
        ledger.transition(row['id'], row['generation'], 'ready')
        ledger.dispatch(str(index), row['id'], row['generation'])
        ledger.acknowledge(str(index), row['id'], row['generation'])
        completed += 1
    else:
        pytest.fail('Completed messages bypassed the retained storage cap')
    assert completed > ledger.max_queued
    assert ledger.summary()['queued_messages'] == 0
    reopened = Ledger(tmp_path / 'ledger.sqlite', max_ledger_bytes=1024 * 1024)
    assert reopened.admit(event_id='0', **arguments)[1] is False


@pytest.mark.asyncio
async def test_revocation_while_upload_body_arrives_leaves_no_media(tmp_path):
    ledger = Ledger(tmp_path / 'ledger.sqlite')
    media = MediaStore(tmp_path / 'media', ledger)
    row = owner(ledger, 'one')
    app = FastAPI()
    app.include_router(media.router())
    async def body():
        yield b'first'
        ledger.transition(row['id'], row['generation'], 'stopped')
        yield b'last'
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='https://companion.test') as client:
        result = await client.post('/relay/media', headers=authorization(row), content=body())
    assert result.status_code == 403
    assert list(media.root.iterdir()) == []


@pytest.mark.asyncio
async def test_uploads_bound_concurrent_body_consumption(tmp_path):
    ledger = Ledger(tmp_path / 'ledger.sqlite')
    media = MediaStore(tmp_path / 'media', ledger)
    row = owner(ledger, 'one')
    app = FastAPI()
    app.include_router(media.router())
    started = [asyncio.Event() for _ in range(3)]
    release = asyncio.Event()
    async def body(index):
        started[index].set()
        yield str(index).encode()
        await release.wait()
        yield b'done'
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='https://companion.test') as client:
        tasks = [asyncio.create_task(client.post('/relay/media', headers=authorization(row), content=body(i))) for i in range(3)]
        try:
            await asyncio.wait_for(asyncio.gather(started[0].wait(), started[1].wait()), 2)
            await asyncio.sleep(0)
            assert not started[2].is_set()
        finally:
            release.set()
            results = await asyncio.gather(*tasks)
    assert all(result.status_code == 200 for result in results)
    assert started[2].is_set()


class ReviewConnector:
    connected = True
    def __init__(self, *args, **kwargs):
        self.audience_hook = None
    async def audience(self, source):
        if self.audience_hook:
            await self.audience_hook()
        return 'private'
    async def connect(self):
        return True
    async def disconnect(self):
        pass
    async def notice(self, owner, text):
        return {'success': True}


def review_controller(tmp_path):
    from hermes_cluster.controller import Controller
    from hermes_cluster.kubernetes import WorkerIdentity, WorkerStatus
    class Backend:
        async def status(self, identity):
            return WorkerStatus('Running')
    knowledge = SimpleNamespace(register_worker=lambda **kw: None, revoke_worker=lambda *a, **kw: None)
    controller = Controller({'data_dir': str(tmp_path), 'relay_url': 'http://test',
                             'bot_id': '1', 'native_config': {}}, Backend(), knowledge,
                             connector_factory=ReviewConnector)
    row = owner(controller.ledger, 'one')
    identity = WorkerIdentity(row['id'], 1, 'pod', 'pod-uid', 'claim', 'config', 'secret', 'claim-uid')
    controller.ledger.transition(row['id'], 1, 'ready', identity=asdict(identity))
    return controller, controller.ledger.get(row['id'])


@pytest.mark.asyncio
async def test_reconnecting_drain_does_not_reopen_message_admission(tmp_path):
    controller, row = review_controller(tmp_path)
    controller.ledger.transition(row['id'], 1, 'draining')
    connection = SimpleNamespace(owner=row)
    try:
        await controller.connect_owner(connection)
    except OwnershipError:
        pass  # Rejecting the reconnect also preserves the admission boundary.
    assert controller.ledger.get(row['id'])['status'] == 'draining'


@pytest.mark.asyncio
async def test_simultaneous_handshakes_cannot_both_register(tmp_path):
    controller, row = review_controller(tmp_path)
    arrived = asyncio.Event()
    release = asyncio.Event()
    calls = 0
    async def audience_hook():
        nonlocal calls
        calls += 1
        arrived.set()
        await release.wait()
    controller.connector.audience_hook = audience_hook
    first = asyncio.create_task(controller.connect_owner(SimpleNamespace(owner=row)))
    await asyncio.wait_for(arrived.wait(), 2)
    second = asyncio.create_task(controller.connect_owner(SimpleNamespace(owner=row)))
    await asyncio.sleep(0)
    release.set()
    results = await asyncio.gather(first, second, return_exceptions=True)
    assert sum(not isinstance(result, Exception) for result in results) == 1
    assert any(isinstance(result, OwnershipError) for result in results)


@pytest.mark.asyncio
async def test_restart_does_not_reactivate_terminated_owner_knowledge_grant(tmp_path):
    from hermes_cluster.knowledge.store import KnowledgeError, KnowledgeStore
    controller, row = review_controller(tmp_path)
    knowledge = KnowledgeStore(tmp_path / 'knowledge.sqlite')
    controller.knowledge = knowledge
    controller.register_knowledge(row)
    knowledge.revoke_worker(row['id'], row['generation'])
    controller.ledger.transition(row['id'], row['generation'], 'recovery_required')
    async def no_background_work():
        pass
    controller.run = no_background_work
    await controller.start()
    await controller.task
    with pytest.raises(KnowledgeError, match='revoked'):
        knowledge.snapshot(row['knowledge_token'])


@pytest.mark.asyncio
async def test_pre_recovery_handshake_cannot_claim_current_generation(tmp_path):
    controller, row = review_controller(tmp_path)
    controller.ledger.transition(row['id'], row['generation'], 'stopped')
    recovered = controller.ledger.prepare_recovery(row['id'], row['generation'], 'confirmed clean exit and inspected history')
    current_identity = dict(recovered['identity'], generation=recovered['generation'])
    controller.ledger.transition(row['id'], recovered['generation'], 'provisioning', identity=current_identity)
    with pytest.raises(OwnershipError):
        await controller.connect_owner(SimpleNamespace(owner=row))
    assert row['id'] not in controller.connections


@pytest.mark.asyncio
async def test_approval_is_not_dispatchable_until_audience_check_finishes(tmp_path):
    controller, row = review_controller(tmp_path)
    controller.ledger.create_prompt('prompt', row, 'one', [{'id': 'once'}])
    controller.ledger.attach_prompt('prompt', 'prompt-message')
    checking, release = asyncio.Event(), asyncio.Event()
    async def audience_hook():
        checking.set()
        await release.wait()
    controller.connector.audience_hook = audience_hook
    interaction = SimpleNamespace(user=SimpleNamespace(id='allowed'), channel_id='one',
                                  message=SimpleNamespace(id='prompt-message'), id='click')
    response = asyncio.create_task(controller.prompt_response('prompt', 'once', interaction))
    try:
        await asyncio.wait_for(checking.wait(), 2)
        assert not any(event['payload'].get('prompt_response') for event in controller.ledger.queued(row['id']))
    finally:
        release.set()
        await response
    assert sum(bool(event['payload'].get('prompt_response')) for event in controller.ledger.queued(row['id'])) == 1


def real_knowledge(controller, row, tmp_path):
    from hermes_cluster.knowledge import KnowledgeStore
    controller.knowledge = KnowledgeStore(tmp_path / 'review-knowledge.sqlite')
    controller.register_knowledge(row)
    return controller.knowledge


@pytest.mark.asyncio
async def test_fence_during_reconnect_audience_await_cannot_restore_ready(tmp_path):
    from hermes_cluster.knowledge import KnowledgeError
    controller, row = review_controller(tmp_path)
    knowledge = real_knowledge(controller, row, tmp_path)
    arrived, release = asyncio.Event(), asyncio.Event()
    async def audience_hook():
        arrived.set()
        await release.wait()
    controller.connector.audience_hook = audience_hook
    handshake = asyncio.create_task(controller.connect_owner(SimpleNamespace(owner=row)))
    await asyncio.wait_for(arrived.wait(), 2)
    await controller.fence_owner(row, 'concurrent operator fence')
    release.set()
    await asyncio.gather(handshake, return_exceptions=True)
    assert controller.ledger.get(row['id'])['status'] == 'recovery_required'
    assert row['id'] not in controller.connections
    with pytest.raises(KnowledgeError, match='revoked'):
        knowledge.snapshot(row['knowledge_token'])


@pytest.mark.asyncio
@pytest.mark.parametrize('operation', ['reconnect', 'approval'])
async def test_explicit_audience_denial_revokes_capability_not_just_delivery(tmp_path, operation):
    from hermes_cluster.knowledge import KnowledgeError
    controller, row = review_controller(tmp_path)
    knowledge = real_knowledge(controller, row, tmp_path)
    async def audience_denied():
        raise OwnershipError('conversation owner no longer has Discord channel visibility')
    controller.connector.audience_hook = audience_denied
    if operation == 'reconnect':
        action = controller.connect_owner(SimpleNamespace(owner=row))
    else:
        controller.ledger.create_prompt('denied-prompt', row, 'one', [{'id': 'once'}])
        controller.ledger.attach_prompt('denied-prompt', 'prompt-message')
        interaction = SimpleNamespace(user=SimpleNamespace(id='allowed'), channel_id='one',
                                      message=SimpleNamespace(id='prompt-message'), id='click')
        action = controller.prompt_response('denied-prompt', 'once', interaction)
    with pytest.raises(OwnershipError):
        await action
    assert controller.ledger.get(row['id'])['status'] == 'recovery_required'
    with pytest.raises(KnowledgeError, match='revoked'):
        knowledge.snapshot(row['knowledge_token'])


@pytest.mark.asyncio
async def test_interrupted_provisioning_retry_failure_revokes_issued_knowledge_grant(tmp_path, monkeypatch):
    from hermes_cluster.knowledge import KnowledgeError
    controller, existing = review_controller(tmp_path)
    # This represents a controller restart after reserve_next but before recording a Pod identity.
    with controller.ledger.transaction() as db:
        db.execute("UPDATE conversations SET status='provisioning',identity=NULL WHERE id=?", (existing['id'],))
    row = controller.ledger.get(existing['id'])
    knowledge = real_knowledge(controller, row, tmp_path)
    monkeypatch.setenv('OPENAI_API_KEY', 'test-provider-credential')
    async def fail_credentials(*args, **kwargs):
        controller.stopping = True
        controller.wake.set()
        raise RuntimeError('injected credential provisioning failure')
    controller.backend.create_credentials = fail_credentials
    await controller.run()
    assert controller.ledger.get(row['id'])['status'] == 'recovery_required'
    with pytest.raises(KnowledgeError, match='revoked'):
        knowledge.snapshot(row['knowledge_token'])


@pytest.mark.asyncio
async def test_late_ingress_ack_cannot_reopen_fenced_or_recovered_owner(tmp_path):
    from hermes_cluster.knowledge import KnowledgeError
    controller, row = review_controller(tmp_path)
    knowledge = real_knowledge(controller, row, tmp_path)
    controller.ledger.dispatch('one', row['id'], row['generation'])
    await controller.fence_owner(row, 'explicit reconciliation required')
    controller.ledger.acknowledge('one', row['id'], row['generation'])
    assert controller.ledger.get(row['id'])['status'] == 'recovery_required'
    with pytest.raises(KnowledgeError):
        knowledge.snapshot(row['knowledge_token'])
    recovered = controller.ledger.prepare_recovery(row['id'], row['generation'], 'checked uncertain effects and retained history')
    controller.ledger.acknowledge('one', row['id'], row['generation'])
    current = controller.ledger.get(row['id'])
    assert current['generation'] == recovered['generation']
    assert current['status'] == 'queued'


@pytest.mark.asyncio
@pytest.mark.parametrize('phase,terminated', [('Unknown', False), ('Succeeded', True)])
async def test_status_inspection_cannot_overwrite_a_concurrent_fence(tmp_path, phase, terminated):
    from hermes_cluster.knowledge import KnowledgeError
    controller, row = review_controller(tmp_path)
    knowledge = real_knowledge(controller, row, tmp_path)
    controller.ledger.transition(row['id'], row['generation'], 'draining')
    arrived, release = asyncio.Event(), asyncio.Event()
    async def delayed_status(identity):
        arrived.set()
        await release.wait()
        return SimpleNamespace(phase=phase, terminated=terminated)
    controller.backend.status = delayed_status
    inspection = asyncio.create_task(controller.inspect_workers())
    await asyncio.wait_for(arrived.wait(), 2)
    await controller.fence_owner(row, 'audience revoked during status inspection')
    release.set()
    await inspection
    assert controller.ledger.get(row['id'])['status'] == 'recovery_required'
    with pytest.raises(KnowledgeError, match='revoked'):
        knowledge.snapshot(row['knowledge_token'])
