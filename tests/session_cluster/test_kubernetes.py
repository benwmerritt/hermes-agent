"""Exercise the API contract without a cluster or credentials."""
import copy
import json

import httpx
import pytest
import pytest_asyncio

from hermes_cluster.kubernetes import KubernetesBackend, KubernetesError, OwnershipConflict


class FakeAPI:
    def __init__(self):
        self.objects = {}
        self.calls = []
        self.post_response_lost = False

    def request(self, request):
        parts = request.url.path.split('/')
        assert parts[:5] == ['', 'api', 'v1', 'namespaces', 'pilot']
        kind = parts[5]
        name = parts[6] if len(parts) > 6 else None
        self.calls.append((request.method, kind, name))
        if request.method == 'GET':
            if name is None:
                assert kind == 'pods', 'Secrets and claims must never be listed'
                key, value = request.url.params['labelSelector'].split('=', 1)
                items = [v for (k, _), v in self.objects.items()
                         if k == kind and v['metadata'].get('labels', {}).get(key) == value]
                return httpx.Response(200, json={'items': items})
            value = self.objects.get((kind, name))
            return httpx.Response(200 if value else 404, json=value or {})
        body = json.loads(request.content)
        if request.method == 'POST':
            name = body['metadata']['name']
            if (kind, name) in self.objects:
                return httpx.Response(409, json={})
            body['metadata']['uid'] = f'uid-{name}'
            self.objects[kind, name] = body
            if self.post_response_lost and kind == 'pods':
                raise httpx.ReadTimeout('lost response', request=request)
            return httpx.Response(201, json=body)
        assert request.method == 'DELETE'
        existing = self.objects[kind, name]
        if body['preconditions']['uid'] != existing['metadata']['uid']:
            return httpx.Response(409, json={})
        assert body['gracePeriodSeconds'] == 120
        existing['metadata']['deletionTimestamp'] = '2026-01-01T00:00:00Z'
        return httpx.Response(200, json=existing)


@pytest_asyncio.fixture
async def cluster():
    api = FakeAPI()
    async with httpx.AsyncClient(transport=httpx.MockTransport(api.request), base_url='https://kube.test') as client:
        backend = KubernetesBackend('pilot', 'example/worker@sha256:' + 'a' * 64,
                                    hermes_config={'model': 'test-model'}, client=client)
        yield backend, api


async def create(backend, generation=1):
    return await backend.create_worker('conversation/123', generation,
                                       {'config_source': '/config/hermes.yaml'}, 'worker-credential')


@pytest.mark.asyncio
async def test_reconcile_preserves_exact_generation_and_retained_data(cluster):
    backend, api = cluster
    first = await create(backend)
    assert await create(backend) == first
    assert len(api.objects) == 3
    pod = api.objects['pods', first.pod_name]
    claim = api.objects['persistentvolumeclaims', first.pvc_name]
    assert not claim['metadata'].get('ownerReferences')
    assert claim['spec']['storageClassName'] == 'hermes-session-local'
    assert pod['spec']['restartPolicy'] == 'Never'
    assert pod['spec']['automountServiceAccountToken'] is False
    assert pod['spec']['containers'][0]['envFrom'] == [{'secretRef': {'name': first.secret_name}}]
    config = api.objects['configmaps', first.config_name]
    assert config['immutable'] is True
    assert json.loads(config['data']['worker.json'])['config_source'] == '/config/hermes.yaml'
    assert 'test-model' in config['data']['hermes.yaml']
    assert not pod['metadata'].get('ownerReferences')
    assert 'livenessProbe' not in pod['spec']['containers'][0]


@pytest.mark.asyncio
async def test_prior_owner_in_unknown_failed_or_deleting_state_blocks_replacement(cluster):
    backend, api = cluster
    first = await create(backend)
    pod = api.objects['pods', first.pod_name]
    for status in ({}, {'phase': 'Unknown'}, {'phase': 'Failed', 'reason': 'NodeLost'}):
        pod['status'] = status
        with pytest.raises(OwnershipConflict, match='confirmed terminated'):
            await create(backend, 2)
    pod['metadata']['deletionTimestamp'] = 'now'
    with pytest.raises(OwnershipConflict):
        await create(backend, 2)
    assert len(api.objects) == 3


@pytest.mark.asyncio
async def test_confirmed_exit_allows_deliberate_generation_on_same_claim(cluster):
    backend, api = cluster
    first = await create(backend)
    api.objects['pods', first.pod_name]['status'] = {
        'phase': 'Succeeded', 'containerStatuses': [
            {'name': 'worker', 'state': {'terminated': {'exitCode': 0}}}]}
    assert (await backend.status(first)).terminated
    second = await create(backend, 2)
    assert second.pvc_name == first.pvc_name
    assert second.pod_uid != first.pod_uid
    assert len([k for k in api.objects if k[0] == 'persistentvolumeclaims']) == 1


@pytest.mark.asyncio
async def test_conflicting_config_and_lost_claim_fail_closed(cluster):
    backend, api = cluster
    first = await create(backend)
    with pytest.raises(OwnershipConflict, match='configuration differs'):
        await backend.create_worker(first.conversation_id, 1, {'different': True}, first.secret_name)
    api.objects['persistentvolumeclaims', first.pvc_name]['status'] = {'phase': 'Lost'}
    with pytest.raises(OwnershipConflict, match='lost'):
        await create(backend)


@pytest.mark.asyncio
async def test_response_loss_requires_reconcile_without_automatic_retry(cluster):
    backend, api = cluster
    api.post_response_lost = True
    with pytest.raises(KubernetesError, match='reconcile before retry'):
        await create(backend)
    assert len([c for c in api.calls if c[:2] == ('POST', 'pods')]) == 1
    api.post_response_lost = False
    first = await create(backend)
    assert len([k for k in api.objects if k[0] == 'pods']) == 1
    assert first.pod_uid


@pytest.mark.asyncio
async def test_stop_is_uid_scoped_and_keeps_claim_config_and_credentials(cluster):
    backend, api = cluster
    await backend.create_credentials('worker-credential', {'OPENAI_API_KEY': 'test-only'})
    first = await create(backend)
    api.objects['pods', first.pod_name]['status'] = {
        'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}]}
    assert (await backend.status(first)).ready
    state = await backend.stop_worker(first)
    assert not state.terminated
    assert not (await backend.status(first)).ready
    assert set(k[0] for k in api.objects) == {'pods', 'configmaps', 'secrets', 'persistentvolumeclaims'}
    assert [c[1] for c in api.calls if c[0] == 'DELETE'] == ['pods']


@pytest.mark.asyncio
async def test_missing_and_replaced_pod_are_not_fence_receipts(cluster):
    backend, api = cluster
    first = await create(backend)
    pod = api.objects.pop(('pods', first.pod_name))
    missing = await backend.stop_worker(first)
    assert missing.reason == 'PodAbsentIsNotFenceProof' and not missing.terminated
    pod['metadata']['uid'] = 'different'
    api.objects['pods', first.pod_name] = pod
    changed = await backend.stop_worker(first)
    assert changed.reason == 'PodUIDChanged' and not changed.terminated
    assert not any(c[0] == 'DELETE' for c in api.calls)


@pytest.mark.asyncio
async def test_credentials_are_exact_name_immutable_and_never_overwritten_or_listed(cluster):
    backend, api = cluster
    mapping = {'GATEWAY_RELAY_SECRET': 'test-only', 'OPENAI_API_KEY': 'test-provider'}
    name = await backend.create_credentials('worker-credential', mapping)
    before = copy.deepcopy(api.objects['secrets', name])
    assert await backend.create_credentials(name, mapping) == name
    with pytest.raises(OwnershipConflict):
        await backend.create_credentials(name, {'OPENAI_API_KEY': 'different'})
    assert api.objects['secrets', name] == before
    assert not any(kind == 'secrets' and name is None and method == 'GET'
                   for method, kind, name in api.calls)


@pytest.mark.asyncio
async def test_api_failure_does_not_expose_response_body():
    def fail(request):
        return httpx.Response(403, json={'message': 'PRIVATE-CREDENTIAL'})
    async with httpx.AsyncClient(transport=httpx.MockTransport(fail), base_url='https://kube.test') as client:
        backend = KubernetesBackend('pilot', 'image', client=client)
        with pytest.raises(KubernetesError) as caught:
            await create(backend)
        assert 'PRIVATE-CREDENTIAL' not in str(caught.value)


@pytest.mark.asyncio
async def test_recovery_cannot_create_missing_or_substituted_claim(cluster):
    backend, api = cluster
    first = await create(backend)
    del api.objects['pods', first.pod_name]
    claim = api.objects.pop(('persistentvolumeclaims', first.pvc_name))
    kwargs = dict(require_existing_claim=True, expected_claim_uid=first.pvc_uid)
    with pytest.raises(OwnershipConflict, match='missing'):
        await backend.create_worker(first.conversation_id, 2, {}, first.secret_name, **kwargs)
    claim['metadata']['uid'] = 'substitute'
    api.objects['persistentvolumeclaims', first.pvc_name] = claim
    with pytest.raises(OwnershipConflict, match='UID changed'):
        await backend.create_worker(first.conversation_id, 2, {}, first.secret_name, **kwargs)
    claim['metadata']['uid'] = first.pvc_uid
    claim['spec']['volumeName'] = 'other-volume'
    with pytest.raises(OwnershipConflict, match='volume changed'):
        await backend.create_worker(first.conversation_id, 2, {}, first.secret_name,
                                    expected_volume_name='original-volume', **kwargs)
