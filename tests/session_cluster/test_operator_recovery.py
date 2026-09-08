"""Exercise privileged reboot fencing through HTTP, real SQLite and the Kube boundary."""
import asyncio
import copy
import hashlib
import json
import time
from types import SimpleNamespace

import httpx
import pytest

from hermes_cluster.app import create_app
from hermes_cluster.ledger import Ledger, OwnershipError
from hermes_cluster.media import MediaStore
from hermes_cluster.kubernetes import OwnershipConflict, _terminated
from test_kubernetes import FakeAPI
from test_retained_config import controller, admit


class API(FakeAPI):
    def request(self, request):
        if request.method == 'GET' and request.url.path.endswith('/pods') and 'labelSelector' not in request.url.params:
            return httpx.Response(200, json={'items': [v for (k, _), v in self.objects.items() if k == 'pods']})
        return super().request(request)


async def fixture(tmp_path, monkeypatch):
    api = API()
    ctrl = controller(tmp_path, api, monkeypatch)
    ctrl.operations = asyncio.Lock()
    ctrl.connections = {}
    ctrl.wake = asyncio.Event()
    ctrl.media = MediaStore(tmp_path / 'media', ctrl.ledger)
    ctrl.knowledge.revoke_worker = lambda *a: None
    async def audience(source):
        return 'public'
    ctrl.connector = SimpleNamespace(audience=audience)
    row = admit(ctrl, 'saved-thread')
    await ctrl.provision(row)
    row = ctrl.ledger.get(row['id'])
    ctrl.ledger.transition(row['id'], 1, 'ready')
    ctrl.ledger.dispatch('saved-thread', row['id'], 1)
    ctrl.ledger.acknowledge('saved-thread', row['id'], 1)
    ctrl.ledger.transition(row['id'], 1, 'stopped')
    row = ctrl.ledger.get(row['id'])
    old = row['identity']
    del api.objects['pods', old['pod_name']]
    claim = api.objects['persistentvolumeclaims', old['pvc_name']]
    claim['status'] = {'phase': 'Bound'}
    claim['spec']['volumeName'] = 'saved-pv'
    claim['metadata']['annotations'] = {'volume.kubernetes.io/selected-node': 'original-node'}
    config = api.objects['configmaps', old['config_name']]
    worker = json.loads(config['data']['worker.json'])
    home = {k: worker[k] for k in ('worker_id','generation','conversation_key','source','config_revision')}
    home['personality_digest'] = hashlib.sha256(worker['personality'].encode()).hexdigest()
    body = {'schema_version':1, 'method':'original-node-reboot', 'expected_generation':1,
            'prior_identity':old, 'binding':{'node_name':'original-node','node_uid':'node-uid','boot_id':'returned-boot',
                'pvc_uid':old['pvc_uid'],'volume_name':'saved-pv','config_uid':config['metadata']['uid']},
            'home':home, 'inspection':{'observed_at':time.time(),'shutdown_receipt_sha256':'a'*64,
                'start_receipt_sha256':'b'*64,'node_ready':True,'runtime_pod_uid_absent':True,
                'sandbox_pod_uid_absent':True,'pod_cgroup_absent':True,'no_other_claimant':True},
            'reconciliation_note':'Original guest rebooted, old runtime absent, saved effects reconciled. No replay.'}
    return ctrl, api, row, body


@pytest.mark.asyncio
async def test_operator_reboot_recovery_is_one_use_durable_and_preserves_home_config(tmp_path, monkeypatch):
    ctrl, api, row, body = await fixture(tmp_path, monkeypatch)
    monkeypatch.setenv('HERMES_CLUSTER_OPERATOR_TOKEN', 'operator-fixture-token-at-least-24')
    app = create_app(ctrl)
    headers = {'Authorization':'Bearer operator-fixture-token-at-least-24'}
    url = f'/conversations/{row["id"]}/recover-attested'
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
            assert (await client.post(url,json=body)).status_code == 403
            assert ctrl.ledger.get(row['id'])['generation'] == 1
            assert (await client.post(url,json=body,headers=headers)).status_code == 200
            assert (await client.post(url,json=body,headers=headers)).status_code == 409
            assert (await client.post(url,content=b'x'*32769,headers=headers)).status_code == 413
        ctrl.ledger = Ledger(tmp_path / 'ledger.sqlite')
        proof = ctrl.ledger.recovery_fence(row['id'], 2)
        assert proof['prior_identity'] == body['prior_identity']
        assert ctrl.ledger.db.execute("SELECT count(*) FROM audit WHERE event='operator_fence_consumed'").fetchone()[0] == 1
        assert ctrl.ledger.db.execute("SELECT status FROM ingress WHERE id='saved-thread'").fetchone()[0] == 'admitted'
        current = ctrl.ledger.reserve_next()
        assert current['generation'] == 2
        ctrl.config['native_config'] = {'agent':{'restart_drain_timeout':30}}
        ctrl.backend.hermes_config = ctrl.config['native_config']
        ctrl.config['personality'] = 'Latest personality for fresh homes'
        # A valid authorization remains usable after a controller restart/build delay.
        monkeypatch.setattr(time, 'time', lambda: body['inspection']['observed_at'] + 1000)
        api.post_response_lost = True
        with pytest.raises(Exception, match='reconcile before retry'):
            await ctrl.provision(current)
        api.post_response_lost = False
        await ctrl.provision(ctrl.ledger.get(row['id']))
        new = ctrl.ledger.get(row['id'])
        assert new['identity']['pvc_uid'] == row['identity']['pvc_uid']
        config = api.objects['configmaps', new['identity']['config_name']]['data']
        worker = json.loads(config['worker.json'])
        assert worker['config_revision'] == body['home']['config_revision']
        assert hashlib.sha256(worker['personality'].encode()).hexdigest() == body['home']['personality_digest']
        assert new['relay_secret'] != row['relay_secret']
        assert len([k for k in api.objects if k[0] == 'pods']) == 1
    finally:
        await ctrl.backend.client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize('damage', ['method','generation','boolean','uid','source','config','personality','stale','future',
    'nan','huge','unknown','runtime','sandbox','claim','node','pod','claimant','missing-config','changed-config','audience','reconnect','generation-race'])
async def test_operator_recovery_fails_closed(tmp_path, monkeypatch, damage):
    ctrl, api, row, original = await fixture(tmp_path, monkeypatch)
    body = copy.deepcopy(original)
    try:
        if damage == 'method': body['method'] = 'missing-node'
        elif damage == 'generation': body['expected_generation'] = 2
        elif damage == 'boolean': body['expected_generation'] = True
        elif damage == 'uid': body['prior_identity']['pod_uid'] = 'other'
        elif damage == 'source': body['home']['source']['chat_id'] = 'other'
        elif damage == 'config': body['home']['config_revision'] = 'c'*64
        elif damage == 'personality': body['home']['personality_digest'] = 'c'*64
        elif damage == 'stale': body['inspection']['observed_at'] -= 601
        elif damage == 'future': body['inspection']['observed_at'] += 31
        elif damage == 'nan': body['inspection']['observed_at'] = float('nan')
        elif damage == 'huge': body['inspection']['observed_at'] = 10**1000
        elif damage == 'unknown': body['trust_me'] = True
        elif damage == 'runtime': body['inspection']['runtime_pod_uid_absent'] = False
        elif damage == 'sandbox': body['inspection']['sandbox_pod_uid_absent'] = 1
        elif damage == 'claim': body['binding']['pvc_uid'] = 'other'
        elif damage == 'node': body['binding']['node_name'] = 'other'
        elif damage == 'pod': api.objects['pods',row['identity']['pod_name']] = {'metadata':{'uid':'substituted'}}
        elif damage == 'claimant': api.objects['pods','unrelated-label'] = {'metadata':{'name':'another'}, 'spec':{'volumes':[{'persistentVolumeClaim':{'claimName':row['identity']['pvc_name']}}]}}
        elif damage == 'missing-config': del api.objects['configmaps',row['identity']['config_name']]
        elif damage == 'changed-config': api.objects['configmaps',row['identity']['config_name']]['metadata']['uid'] = 'other'
        elif damage in ('audience','reconnect','generation-race'):
            async def changed(source):
                if damage == 'reconnect': ctrl.connections[row['id']] = object()
                if damage == 'generation-race': ctrl.ledger.prepare_recovery(row['id'],1,'Another explicit recovery superseded this one')
                return 'other' if damage == 'audience' else 'public'
            ctrl.connector.audience = changed
        with pytest.raises((ValueError,OwnershipError,OwnershipConflict)):
            await ctrl.recover_attested(row['id'],body)
        assert ctrl.ledger.recovery_fence(row['id'],2) is None
        assert not any(k[0]=='pods' and k[1].endswith('-g2') for k in api.objects)
    finally:
        await ctrl.backend.client.aclose()


@pytest.mark.asyncio
async def test_missing_pod_still_does_not_enable_ordinary_recovery(tmp_path, monkeypatch):
    ctrl, api, row, body = await fixture(tmp_path, monkeypatch)
    try:
        with pytest.raises(OwnershipError,match='termination has not been proved'):
            await ctrl.recover(row['id'],'The Pod was missing; no physical fence inspected')
    finally:
        await ctrl.backend.client.aclose()


def test_missing_or_duplicate_init_status_cannot_fence_claimant():
    pod = {'spec':{'containers':[{'name':'worker'}], 'initContainers':[{'name':'init'}]},
           'status':{'containerStatuses':[{'name':'worker','state':{'terminated':{}}}]}}
    assert not _terminated(pod)
    pod['status']['initContainerStatuses'] = [{'name':'init','state':{'terminated':{}}}]
    assert _terminated(pod)
    pod['status']['initContainerStatuses'] *= 2
    assert not _terminated(pod)


def test_live_ephemeral_container_is_not_a_terminated_owner():
    pod = {'spec':{'containers':[{'name':'worker'}], 'ephemeralContainers':[{'name':'debug'}]},
           'status':{'containerStatuses':[{'name':'worker','state':{'terminated':{}}}]}}
    assert not _terminated(pod)
    pod['status']['ephemeralContainerStatuses'] = [{'name':'debug','state':{'running':{}}}]
    assert not _terminated(pod)
    pod['status']['ephemeralContainerStatuses'][0]['state'] = {'terminated':{}}
    assert _terminated(pod)
