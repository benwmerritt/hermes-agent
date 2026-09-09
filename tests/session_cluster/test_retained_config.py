"""Config propagation through the real controller and Kubernetes request boundary."""
import copy
import hashlib
import json
from types import SimpleNamespace

import httpx
import pytest
import yaml

from hermes_cluster.controller import Controller
from hermes_cluster.kubernetes import KubernetesBackend, KubernetesError, OwnershipConflict
from hermes_cluster.ledger import Ledger, OwnershipError, canonical
from test_kubernetes import FakeAPI


def controller(tmp_path, api, monkeypatch):
    monkeypatch.setenv('OPENAI_API_KEY', 'fixture-provider')
    native = {'model': {'default': 'old-model'}, 'agent': {'restart_drain_timeout': 0}}
    result = object.__new__(Controller)
    result.config = {'native_config': native, 'personality': 'Original personality',
                     'relay_url': 'https://old.test', 'bot_id': '1'}
    result.ledger = Ledger(tmp_path / 'ledger.sqlite')
    result.backend = KubernetesBackend('pilot', 'example/worker@sha256:' + 'a' * 64,
        hermes_config=native, client=httpx.AsyncClient(transport=httpx.MockTransport(api.request), base_url='https://kube.test'))
    result.knowledge = SimpleNamespace(register_worker=lambda **kwargs: None)
    return result


def admit(ctrl, thread):
    row, _ = ctrl.ledger.admit(agent_id='test', native_key=thread,
        source={'platform': 'discord', 'scope_id': 'guild', 'chat_id': thread, 'user_id': 'allowed'},
        audience='public', event_id=thread, payload={'text': 'hello'}, actor='allowed')
    return ctrl.ledger.reserve_next()


def complete(ctrl, api, row):
    row = ctrl.ledger.get(row['id'])
    api.objects['pods', row['identity']['pod_name']]['status'] = {
        'phase': 'Succeeded', 'containerStatuses': [{'name': 'worker', 'state': {'terminated': {'exitCode': 0}}}]}
    ctrl.ledger.transition(row['id'], row['generation'], 'stopped')
    ctrl.ledger.prepare_recovery(row['id'], row['generation'], 'Verified termination and all uncertain effects')
    return ctrl.ledger.reserve_next()


@pytest.mark.asyncio
@pytest.mark.parametrize('legacy_identity', [False, True])
async def test_recovery_pins_config_while_fresh_workers_use_current_config(tmp_path, monkeypatch, legacy_identity):
    api = FakeAPI()
    ctrl = controller(tmp_path, api, monkeypatch)
    try:
        old = admit(ctrl, 'old-thread')
        await ctrl.provision(old)
        old = ctrl.ledger.get(old['id'])
        if legacy_identity:
            identity = dict(old['identity'])
            identity.pop('config_uid')
            ctrl.ledger.transition(old['id'], old['generation'], old['status'], identity=identity)
            old = ctrl.ledger.get(old['id'])
        original = copy.deepcopy(api.objects['configmaps', old['identity']['config_name']]['data'])
        original_worker = json.loads(original['worker.json'])
        recovering = complete(ctrl, api, old)
        ctrl.config.update(native_config={'model': {'default': 'new-model'}, 'agent': {'restart_drain_timeout': 30}},
                           personality='New personality', relay_url='https://current.test')
        ctrl.backend.hermes_config = ctrl.config['native_config']
        ctrl.backend.image = 'example/worker@sha256:' + 'b' * 64
        ctrl.backend.resources = {'requests': {'memory': '1Gi'}, 'limits': {'memory': '1Gi'}}
        api.post_response_lost = True
        with pytest.raises(KubernetesError, match='reconcile before retry'):
            await ctrl.provision(recovering)
        assert ctrl.ledger.get(old['id'])['identity'] == old['identity']
        api.post_response_lost = False
        await ctrl.provision(ctrl.ledger.get(old['id']))
        current = ctrl.ledger.get(old['id'])
        saved = api.objects['configmaps', current['identity']['config_name']]['data']
        worker = json.loads(saved['worker.json'])
        assert yaml.safe_load(saved['hermes.yaml']) == yaml.safe_load(original['hermes.yaml'])
        assert worker['config_revision'] == original_worker['config_revision']
        assert worker['personality'] == original_worker['personality']
        assert worker['generation'] == current['generation'] == old['generation'] + 1
        assert worker['source'] == current['source']
        assert worker['relay']['url'] == ctrl.config['relay_url']
        assert current['relay_secret'] != old['relay_secret']
        pod = api.objects['pods', current['identity']['pod_name']]
        assert pod['spec']['containers'][0]['image'] == ctrl.backend.image
        assert pod['spec']['containers'][0]['resources'] == ctrl.backend.resources
        assert current['identity']['pvc_uid'] == old['identity']['pvc_uid']
        # Exercise the actual saved-home contract, including native YAML digest
        # validation and personality digest calculation before claiming the home.
        from hermes_cluster.worker_config import claim_home, read_config
        for name, data in [('original', original), ('recovered', saved)]:
            native_path = tmp_path / f'{name}.yaml'
            native_path.write_text(data['hermes.yaml'])
            bootstrap = json.loads(data['worker.json'])
            bootstrap.update(hermes_home=str(tmp_path / 'retained-home'),
                             workspace=str(tmp_path / 'workspace'), config_source=str(native_path))
            bootstrap_path = tmp_path / f'{name}.json'
            bootstrap_path.write_text(json.dumps(bootstrap))
            with claim_home(read_config(bootstrap_path)) as retained:
                assert retained['config_revision'] == original_worker['config_revision']
        # A recorded current-generation identity is also a valid retry anchor.
        ctrl.config['native_config'] = {'model': {'default': 'third-model'}}
        ctrl.backend.hermes_config = ctrl.config['native_config']
        ctrl.config['personality'] = 'Third personality'
        before = copy.deepcopy(pod)
        saved_before_retry = copy.deepcopy(saved)
        await ctrl.provision(current)
        assert api.objects['pods', current['identity']['pod_name']] == before
        assert api.objects['configmaps', current['identity']['config_name']]['data'] == saved_before_retry
        fresh = admit(ctrl, 'fresh-thread')
        await ctrl.provision(fresh)
        fresh = ctrl.ledger.get(fresh['id'])
        fresh_saved = api.objects['configmaps', fresh['identity']['config_name']]['data']
        assert yaml.safe_load(fresh_saved['hermes.yaml']) == ctrl.config['native_config']
        assert json.loads(fresh_saved['worker.json'])['personality'] == ctrl.config['personality']
    finally:
        await ctrl.backend.client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize('damage', ['missing', 'mutable', 'label', 'uid', 'worker', 'revision', 'tampered',
                                   'pod', 'mount', 'fenced', 'generation'])
async def test_recovery_rejects_unproven_retained_config(tmp_path, monkeypatch, damage):
    api = FakeAPI()
    ctrl = controller(tmp_path, api, monkeypatch)
    try:
        old = admit(ctrl, 'thread')
        await ctrl.provision(old)
        old = ctrl.ledger.get(old['id'])
        recovering = complete(ctrl, api, old)
        config = api.objects['configmaps', old['identity']['config_name']]
        pod = api.objects['pods', old['identity']['pod_name']]
        if damage == 'missing':
            del api.objects['configmaps', old['identity']['config_name']]
        elif damage == 'mutable':
            config['immutable'] = False
        elif damage == 'label':
            config['metadata']['labels']['hermes-cluster/conversation'] = 'another'
        elif damage == 'uid':
            config['metadata']['uid'] = 'substituted-config'
        elif damage in {'worker', 'revision', 'tampered'}:
            worker = json.loads(config['data']['worker.json'])
            if damage == 'worker':
                worker['worker_id'] = 'another'
            elif damage == 'revision':
                worker['config_revision'] = 'wrong'
            else:
                native = {'model': {'default': 'tampered'}}
                config['data']['hermes.yaml'] = yaml.safe_dump(native)
                worker['config_revision'] = hashlib.sha256(canonical(native).encode()).hexdigest()
                worker['personality'] = 'Tampered personality'
            config['data']['worker.json'] = json.dumps(worker)
        elif damage == 'pod':
            pod['metadata']['uid'] = 'substituted'
        elif damage == 'mount':
            pod['spec']['volumes'][1]['configMap']['name'] = 'other-config'
        elif damage in {'fenced', 'generation'}:
            read = ctrl.backend.retained_configuration

            async def concurrent_change(identity):
                result = await read(identity)
                ctrl.ledger.transition(recovering['id'], recovering['generation'], 'recovery_required')
                if damage == 'generation':
                    ctrl.ledger.prepare_recovery(recovering['id'], recovering['generation'], 'Independent recovery superseded this request')
                return result

            ctrl.backend.retained_configuration = concurrent_change
        with pytest.raises((OwnershipConflict, KubernetesError, OwnershipError)):
            await ctrl.provision(recovering)
        assert len([key for key in api.objects if key[0] == 'pods']) == 1
        assert ctrl.ledger.get(old['id'])['identity'] == old['identity']
    finally:
        await ctrl.backend.client.aclose()
