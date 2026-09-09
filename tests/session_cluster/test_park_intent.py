"""A successful process exit is a clean park only with this owner's durable intent."""
from dataclasses import asdict
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

from fastapi import WebSocketDisconnect
import pytest

from gateway.relay.auth import make_upgrade_token
from hermes_cluster.controller import Controller
from hermes_cluster.kubernetes import WorkerIdentity, WorkerStatus
from hermes_cluster.ledger import Ledger
from hermes_cluster.relay_server import create_relay_router


def controller_fixture(tmp_path):
    controller = object.__new__(Controller)
    controller.ledger = Ledger(tmp_path / 'router.sqlite')
    row, _ = controller.ledger.admit(agent_id='test', native_key='one',
        source={'scope_id': 'guild', 'chat_id': 'one', 'user_id': 'allowed'},
        audience='private', event_id='first', payload={'text': 'hello'}, actor='allowed')
    identity = WorkerIdentity(row['id'], 2, 'pod', 'uid', 'pvc', 'config', 'secret', 'claim-uid')
    with controller.ledger.transaction() as db:
        db.execute('UPDATE conversations SET generation=2 WHERE id=?', (row['id'],))
    controller.ledger.transition(row['id'], 2, 'ready', identity=asdict(identity))
    row = controller.ledger.get(row['id'])
    controller.config = {'bot_id': '1'}
    controller.connections = {}
    controller.wake = SimpleNamespace(set=lambda: None)
    controller.backend = SimpleNamespace(status=AsyncMock(return_value=WorkerStatus('Succeeded', terminated=True)))
    controller.knowledge = SimpleNamespace(revoke_worker=lambda cid, generation: None)
    controller.connector = SimpleNamespace(notice=AsyncMock())
    return controller, row


@pytest.mark.asyncio
@pytest.mark.parametrize('intent', ['none', 'previous-generation', 'other-conversation', 'current'])
async def test_going_idle_does_not_invent_clean_park(tmp_path, intent):
    controller, row = controller_fixture(tmp_path)
    if intent != 'none':
        controller.ledger.record('other' if intent == 'other-conversation' else row['id'],
            'park_requested', {'generation': 1 if intent == 'previous-generation' else 2})

    class Socket:
        headers = {'authorization': 'Bearer ' + make_upgrade_token(f"{row['id']}:2", row['relay_secret'])}
        receives = 0

        async def accept(self):
            pass

        async def send_text(self, text):
            pass

        async def receive_text(self):
            self.receives += 1
            if self.receives == 1:
                return json.dumps({'type': 'hello', 'platform': 'discord', 'botId': '1'})
            if self.receives == 2:
                return json.dumps({'type': 'going_idle'})
            raise WebSocketDisconnect()

    controller.connect_owner = AsyncMock()
    await create_relay_router(controller).routes[0].endpoint(Socket())
    assert controller.ledger.get(row['id'])['status'] == 'draining'
    await controller.inspect_workers()
    assert controller.ledger.get(row['id'])['status'] == (
        'stopped' if intent == 'current' else 'recovery_required')


@pytest.mark.asyncio
async def test_park_intent_survives_immediate_exit_and_controller_reopen(tmp_path):
    controller, row = controller_fixture(tmp_path)

    async def send(frame):
        assert frame['event']['text'] == '/cluster-stop-worker'
        # The worker can exit at this first await. Classification uses committed
        # state, including after a controller restart, rather than in-memory intent.
        controller.ledger.db.close()
        controller.ledger = Ledger(tmp_path / 'router.sqlite')
        with controller.ledger.lock:
            intent = controller.ledger.db.execute(
                "SELECT detail FROM audit WHERE conversation_id=? AND event='park_requested'",
                (row['id'],)).fetchone()
        assert intent is not None and json.loads(intent[0]) == {'generation': row['generation']}
        await controller.inspect_workers()
        assert controller.ledger.get(row['id'])['status'] == 'stopped'

    controller.connections[row['id']] = SimpleNamespace(send=send)
    await controller.park(row['id'])
    with controller.ledger.lock:
        rows = controller.ledger.db.execute(
            "SELECT detail FROM audit WHERE conversation_id=? AND event='park_requested'", (row['id'],)).fetchall()
    assert [json.loads(r[0]) for r in rows] == [{'generation': row['generation']}]
