# Session cluster pilot deployment

This directory installs the companion and retained local storage. Keep the rendered
node name, controller configuration, credentials, and deployment receipts outside
this public repository. The manifests contain placeholders and are not ready to
apply unchanged.

## Build

The fork-only `session-cluster.yml` workflow uses a standard public Ubuntu runner
and builds `linux/amd64` from the checked-out commit. It appends the companion
Dockerfile stage to the root Dockerfile, leaving the normal upstream image intact.
All actions are pinned. The image contains public source only.

For a local builder with Docker Buildx 0.13 or newer:

```sh
cat Dockerfile contrib/session-cluster/Dockerfile > /tmp/hermes-session-cluster.Dockerfile
docker buildx build --platform linux/amd64 --target session_cluster \
  --file /tmp/hermes-session-cluster.Dockerfile \
  --tag hermes-session-cluster:test --load .
```

CI checks companion imports, the non-root UID, SQLite, and CLI startup. It exports
an OCI archive before attempting GHCR publication. The archive and checksum expire
after two days. If publication fails, download that exact run's artifact and
import it on each intended guest using `sudo k3s ctr images import <archive>`.
Verify the archive checksum and imported image name before rendering the manifests.
Use the published digest when GHCR is available. Check unauthenticated pull access;
a successful push does not establish public visibility.

Docker documents [multiple exporters](https://docs.docker.com/build/exporters/#multiple-exporters)
and [OCI archives](https://docs.docker.com/build/exporters/#export-image-layout-to-file).
The base Hermes runtime is large. Standard-runner disk use and build time need to
be established by the first build; this workflow does not provision larger runners.

## Install prerequisites

Use an administrator for namespace, StorageClass, RBAC, controller claim, and
Deployment installation. The controller service account cannot install these.
Apply `namespace.yaml`, `storage-class.yaml`, then `rbac.yaml`. The class uses the
existing K3s local-path provisioner, `WaitForFirstConsumer`, and `Retain`. Verify
that the provisioner creates node affinity on the resulting PV and that its helper
Pod can run under the cluster's admission configuration before starting a worker.
A retained PV is recovery material, not a backup.

Supply these namespace resources from private files, without putting credential
values on the command line:

- ConfigMap `hermes-controller-config`, containing `controller.json` and `hermes.yaml`.
- Secret `hermes-controller-credentials`, containing `DISCORD_BOT_TOKEN` and `OPENAI_API_KEY`.
- Secret `hermes-controller-operator`, containing `HERMES_CLUSTER_OPERATOR_TOKEN`.

Controller configuration uses `data_dir=/data/controller`,
`native_config_path=/config/hermes.yaml`, and
`relay_url=http://hermes-controller:8080`. Populate the allowlists and bot identity
privately. Start with one admitted worker. Increase the configured maximum to three
only after measuring representative turn peaks. Requests of 512Mi and limits of
1Gi per worker are provisional. The scheduler admits requests; the memory limit
can still kill a worker. Pending admission must leave messages queued.

Replace the controller node selector with the chosen dedicated guest's hostname
and replace the image placeholder with the verified digest. Review the full
rendered manifests, then apply `controller.yaml`. The companion and knowledge
service share one process and one claim. Recreate avoids overlapping rollout
Pods; the application must also hold an exclusive durable-state lock. Do not force
delete the controller or remap its local PV while its guest might still be alive.
The Service is cluster-internal. `/healthz` and `/readyz` report startup and
connection state. Operational `/status` requires the separate operator bearer.

## Worker ownership and recovery

The controller creates bare Pods with `restartPolicy: Never`, one immutable
ConfigMap per generation, and a stable conversation PVC. Workers have no Kubernetes
service-account token or Discord bot token. Each generated immutable Secret holds
only the provider, relay, and knowledge credentials. The controller can get/create
Secrets but cannot list, patch, or delete them. It can delete Pods by UID only.
The application API provides no arbitrary Kubernetes administration.

The native configuration is mounted at `/config/hermes.yaml`, the bootstrap JSON
at `/config/worker.json`, and state at `/data`. Workers connect outbound to the
companion. There is no per-worker Service or restart-triggering liveness probe.
Readiness reads the worker's current status and process ID; the connector checks
actual relay ownership before routing messages.

A Pending Pod is queued capacity, not a failed turn to replay. Failed, Unknown,
NodeLost, deletion acceptance, and an absent Pod are not proof of termination.
Ordinary recovery requires terminated container state on the recorded Pod UID.
A written receipt alone does not authorize ordinary recovery. The separate privileged operator path below requires a fresh physical-node inspection. Keep
the terminated Pod object until recovery has checked it; deleting it first makes
that check impossible. Guest loss must not start a replacement on another node.
Local PV node affinity keeps the retained home on its original guest. Every recovery call must
require the existing PVC, check its UID, and check its bound PV name when known.
A missing, Lost, or substituted claim blocks recovery rather than creating an
empty home. PVCs, ConfigMaps, and Secrets survive worker stop; cleanup is explicit.

Recovery keeps the conversation's original native config and personality. The
controller reads the recorded generation's immutable ConfigMap and verifies its
ownership, content digest and binding to the exact recorded Pod before creating
the replacement. New identities also record the ConfigMap UID; legacy identities
use the recorded Pod's content binding. Missing or changed evidence blocks
provisioning. A retry can use the recorded current generation's config as its
anchor. It never copies old credentials or replaces the authoritative source.
The new generation uses current transport addresses, credentials, image and
resource limits. Fresh conversations use the latest global config and personality.
Keep the prior Pod and ConfigMap until recovery has created and recorded the new
generation, since both are needed to verify these pinned settings.

New worker Pods explicitly tolerate the `node.kubernetes.io/not-ready` and
`node.kubernetes.io/unreachable` `NoExecute` taints indefinitely. Their tolerations
omit `tolerationSeconds`. Kubernetes otherwise adds the API server's configured
default limit, normally 300 seconds, and may
evict the recorded Pod before its node returns, losing the termination evidence
required for recovery. These two tolerations preserve the controller's recovery
decision through a node outage. They do not tolerate `NoSchedule` taints, prevent
other eviction or deletion, restart containers, or authorize a replacement.
See [Kubernetes taint-based eviction](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/#taint-based-evictions).

Adopting an existing worker preserves its Pod specification, including any older
300-second tolerations. A controller upgrade does not patch running Pods. After
deliberate recovery creates a new generation, inspect that generation before a
long node-outage drill:

```sh
kubectl -n "$namespace" get pod "$worker_pod" \
  -o jsonpath='{.spec.restartPolicy}{"\n"}{.spec.tolerations}{"\n"}'
```

Require `Never` and both exact `NoExecute` keys with no `tolerationSeconds`.
Node return still requires the recorded Pod UID and actual terminated container
states before resume. Keep the container ID, restart count and start/finish times
in the drill receipt; a matching Pod UID alone does not prove process continuity.

## Stop, park, and resume

Send these command texts in the owning Discord conversation, from its allowed
actor. The `/cluster` commands are handled as messages; they are not additional
registered Discord application commands:

- `/stop` interrupts the active Hermes turn and denies its pending tool approvals.
  The worker stays allocated and can accept another message. This does not undo
  a tool or external action that already committed.
- `/cluster park` requests worker shutdown. Its acknowledgment means the request
  was accepted. Wait for `/cluster status` to report `stopped` and verify the
  recorded container terminated with exit code zero. Native cleanup and final
  history publication must finish before the process exits. A failed publication
  or interrupted process needs reconciliation, even if shutdown was requested.
  Relay delivery tasks get the native adapter-cleanup timeout to finish their
  receipts before disconnect. If that wait expires, the worker exits nonzero and
  retains uncertain obligations for inspection; it does not resend them.
  The controller records park intent before sending the shutdown request. Only
  that conversation generation's durable intent plus verified successful
  termination can produce `stopped`. A worker's `going_idle` frame alone, including
  one sent during a signal-driven shutdown, requires recovery even after exit zero.
- `/cluster resume <reconciliation note>` authorizes another worker generation
  against the same PVC after termination is proved. The note must describe what
  was checked. It is retained in the controller audit ledger.

There is no automatic idle parking. A lost connection can reconnect the same
owner; it does not authorize another Pod. Workers run the Python module directly
with `HERMES_GATEWAY_NO_SUPERVISE=1`, so a process crash is visible to Kubernetes.

Resume is not a retry of the old turn. Dispatched but unacknowledged ingress is
marked `interrupted`; acknowledged ingress remains `admitted`, which proves only
worker admission. Neither status is replayed. Unsent `queued` messages remain
eligible and will run after recovery, so inspect those before authorizing resume.
Old approval prompts are invalidated and credentials rotate with the generation.
Do not click an old approval to authorize work in the replacement.

## Inspect retained state before recovery

Use a private operator terminal. These commands can print conversation content;
keep their output with the private deployment receipts. Do not select every column
from `conversations`, because that table also contains worker credentials.

Get the sanitized controller status without copying the operator token out of its
container. Set `conversation_id` and `worker_pod` from the returned identity, then
save the exact Pod state before any lifecycle operation:

```sh
cluster_namespace=hermes-session-cluster
kubectl -n "$cluster_namespace" exec -i deployment/hermes-controller -- python - <<'PYTHON'
import json, os, urllib.request
request = urllib.request.Request('http://127.0.0.1:8080/status', headers={
    'Authorization': 'Bearer ' + os.environ['HERMES_CLUSTER_OPERATOR_TOKEN']})
with urllib.request.urlopen(request) as response:
    print(json.dumps(json.load(response), indent=2))
PYTHON
conversation_id=replace-with-recorded-conversation-id
worker_pod=replace-with-recorded-pod-name
umask 077
kubectl -n "$cluster_namespace" get pod "$worker_pod" -o json > worker-pod-receipt.json
```

Compare `metadata.uid` with the controller's `identity.pod_uid`. Inspect every
`status.containerStatuses[].state.terminated` and any init-container termination,
not just the Pod phase. Record node, PVC UID, bound PV and exit reason. An absent
Pod or a dead guest cannot satisfy ordinary recovery.

Inspect ingress and outbound attempts through read-only SQLite. An outbound result
with `ambiguous: true`, or an attempt with no result, needs comparison against the
actual Discord message and any external tool target:

```sh
kubectl -n "$cluster_namespace" exec -i deployment/hermes-controller -- python - "$conversation_id" <<'PYTHON'
import json, sqlite3, sys
cid = sys.argv[1]
db = sqlite3.connect('file:/data/controller/router.sqlite?mode=ro', uri=True)
db.row_factory = sqlite3.Row
queries = {
    'owner': ('SELECT id,generation,status,identity,detail FROM conversations WHERE id=?', (cid,)),
    'ingress': ('SELECT id,status,generation,payload FROM ingress WHERE conversation_id=? ORDER BY created', (cid,)),
    'outbound': ('SELECT worker_id,request_id,status,action,result FROM outbound WHERE worker_id LIKE ? ORDER BY created', (cid + ':%',)),
    'prompts': ('SELECT id,generation,message_id,expires,response FROM prompts WHERE conversation_id=?', (cid,)),
}
for label, (query, parameters) in queries.items():
    print(label, json.dumps([dict(row) for row in db.execute(query, parameters)], indent=2))
db.close()
PYTHON
```

A running worker can be inspected directly by setting `inspection_pod` to its
recorded name. A terminated container cannot be entered with `kubectl exec`. For
that case, first verify termination and the claim identity, then use an
administrator to create this temporary read-only inspector. Fill the three values
from the recorded worker and PV. It runs no Hermes gateway and mounts no Secrets.
Stop it before resuming the conversation.

```sh
worker_image=replace-with-recorded-image-digest
worker_node=replace-with-recorded-node-name
retained_claim=replace-with-recorded-pvc-name
inspection_pod=hermes-retained-inspector
cat <<YAML | kubectl -n "$cluster_namespace" apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: $inspection_pod
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  nodeSelector:
    kubernetes.io/hostname: $worker_node
  securityContext:
    runAsNonRoot: true
    runAsUser: 10000
    runAsGroup: 10000
    seccompProfile: {type: RuntimeDefault}
  containers:
    - name: inspect
      image: $worker_image
      command: [python, -c, 'import time; time.sleep(600)']
      securityContext:
        allowPrivilegeEscalation: false
        capabilities: {drop: [ALL]}
      resources:
        requests: {cpu: 10m, memory: 64Mi}
        limits: {cpu: 250m, memory: 256Mi}
      volumeMounts:
        - {name: state, mountPath: /data, readOnly: true}
  volumes:
    - name: state
      persistentVolumeClaim: {claimName: $retained_claim, readOnly: true}
YAML
kubectl -n "$cluster_namespace" wait --for=condition=Ready "pod/$inspection_pod" --timeout=60s
```

Read bootstrap metadata, native output obligations and pending shared-state
publication. This script never imports Hermes or invokes a recovery sweep:

```sh
kubectl -n "$cluster_namespace" exec -i "$inspection_pod" -- python - <<'PYTHON'
import json, sqlite3
from pathlib import Path
home = Path('/data/hermes')
identity = json.loads((home / 'cluster-worker-identity.json').read_text())
print('identity', json.dumps({key: identity.get(key) for key in
    ('worker_id', 'generation', 'conversation_key', 'snapshot_id', 'config_revision', 'personality_digest')}))
print('worker status', (home / 'cluster-worker-status.json').read_text())
for name, table, query in (
    ('state.db', 'delivery_obligations',
     'SELECT obligation_id,session_key,state,attempts,last_error,content FROM delivery_obligations ORDER BY updated_at'),
    ('state.db', 'async_delegations',
     'SELECT delegation_id,state,delivery_state,parent_session_id FROM async_delegations ORDER BY updated_at'),
    ('knowledge-outbox.db', 'outbox',
     'SELECT id,route,result IS NOT NULL AS has_receipt FROM outbox ORDER BY created'),
):
    path = home / name
    if not path.exists():
        continue
    db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
        print(table, json.dumps([dict(row) for row in db.execute(query)], indent=2))
    db.close()
PYTHON
# Run this only for the temporary inspector, never for the retained worker Pod.
kubectl -n "$cluster_namespace" delete pod hermes-retained-inspector --wait=true
```

If SQLite cannot open a crashed WAL database from the read-only mount, copy the
complete stopped database and its WAL/SHM sidecars to a private writable inspection
directory. Inspect that copy. Do not discard sidecars or use `immutable=1` to hide
the WAL, and do not start Hermes against the copy.

The native delivery ledger retains final text in `pending`, `attempting` or
`failed` obligations. These workers suppress its boot, reconnect and flood-retry
resends. Do not run the ordinary gateway against this home to recover a reply;
that enables native recovery sends with new Relay request IDs. `delivered` and
`abandoned` rows have native retention limits, and the ledger has a row cap, so
capture evidence promptly rather than treating it as an unlimited archive.

For each uncertain attempt, check Discord by the recorded message IDs and inspect
the external system or workspace for the actual effect. Put the result and receipt
references in the resume note. If the reply already arrived, do not resend it. If
an action did not commit, authorize that specific action in a new message after
resume. If only the reply was lost, copy the retained text manually or explicitly
ask for that reply to be sent again without rerunning tools. There is no controller
API to replay an old outbound attempt or mark a native obligation reconciled;
the historical row remains evidence. Do not rewrite its state with SQL.

For example, after verifying the stated facts, send this in the owning conversation:

```text
/cluster resume Checked Discord message 123 and the target file receipt. The file write completed; only the final reply was missing. Do not repeat that write.
```

Then send a separate, specific follow-up. Recovery can also be requested with
`POST /conversations/{id}/recover` and JSON `{"reconciliation_note":"..."}` using
the operator bearer. `POST /conversations/{id}/park` has the same accepted-request
semantics as the Discord park command. Neither endpoint removes the need to
inspect queued messages, pending delegates, tool effects and termination.

## Explicit operator recovery after original-node reboot

If Kubernetes deleted a legacy Pod, ordinary Discord resume stays blocked. An
operator who has actually observed the original guest stop and restart, inspected
its returned runtime, and reconciled its effects can use the separate
`POST /conversations/{cid}/recover-attested` endpoint with the operator bearer.
This is an explicit trust in that operator's physical inspection. Receipt hashes
bind submitted evidence; they are not cryptographic proof from Kubernetes.
Do not manufacture historical Pod fields or infer a fence from absence alone.

The JSON contract is defined by `recovery_attestation.validate` and rejects unknown
fields. It requires schema_version 1, method `original-node-reboot`, exact
expected_generation and prior_identity, and a reconciliation_note. `binding`
contains node_name, node_uid, returned boot_id, pvc_uid, volume_name and config_uid.
`home` contains the actual retained identity file's worker_id, generation,
conversation_key, source, config_revision and personality_digest. Read that file
from the original stopped home without starting Hermes or editing its identity.
`inspection` contains a Unix observed_at within ten minutes, actual shutdown and
start receipt SHA256 references, and explicit true results for node_ready,
runtime_pod_uid_absent, sandbox_pod_uid_absent, pod_cgroup_absent and
no_other_claimant. Inspect the exact old Pod UID across the returned node's runtime,
sandboxes and cgroups, plus all current claim users. Keep raw evidence privately.

The controller independently requires the old Pod to be absent, the original bound
claim/PV and selected node, no active claim user, an immutable generation ConfigMap
matching the inspected saved home, unchanged audience and no live owner connection.
It atomically consumes the authorization with the generation change and keeps an
audit record. A repeated request cannot create another generation. Provisioning
rechecks namespace/configuration identities and can resume after controller restart
without making a second physical assertion. It preserves the original home and
configuration while rotating worker credentials. It never replays old tool calls.
Do not use this endpoint for another node, substituted storage, an uncertain guest,
or an unverified external effect.

## Snapshot and scheduler boundaries

A fresh conversation home pins the current authorized knowledge snapshot before
native gateway initialization. Same-PVC restarts and generation recovery retain
that snapshot. Native `/new`, `/reset`, compression and worker resume do not fetch
a new shared snapshot. Start a fresh Discord conversation with a new private home
to adopt current shared memory, skill prompt indexes or changed personality/native
configuration. Do not delete pin files to force a refresh. Existing-home bootstrap
rejects changes to the configured personality or native configuration digest.

Approved memory and skill mutations publish through the shared authority. The
worker's accepted local changes remain usable, while its prompt snapshot stays
fixed. History search uses the shared authorized index. Pending authority mutations
and history batches retry through their durable idempotent outbox; this does not
replay Discord sends or arbitrary tool effects.

Workers retain native background terminal and subagent completion delivery, so a
finished chat reply does not establish idleness. They do not start shared cron,
curator/background review, kanban dispatch, heartbeat schedules, hosted-room
workers or credential-refresh services. The worker blocks their lifecycle and
scheduler commands and disables cronjob/kanban toolsets. Process liveness,
session housekeeping, stall detection and session-owned completion watchers remain
active. An idle worker is therefore not a shared scheduler replica.

## Acceptance and rollback

Before admitting traffic, verify node reservations, allocatable memory, all node
health, controller readiness, worker no-token mounts, retained claim placement,
and no credentials in ConfigMaps or logs. Exercise repeated messages in one
conversation, a second conversation, concurrent turns, and the queued fourth
worker after the configured maximum is reached. Measure worker startup, turn
latency, RSS and memory peaks, PVC usage, and host pressure. Inspect a deliberate
worker exit and verify there is no unsolicited restart or turn replay.

For the guest-loss drill, use an approved idle guest with its conversation already
backed up and no in-flight external effect. Record owner, generation, Pod UID,
PVC UID, bound PV, and guest identity. Stop that guest deliberately, observe the
conversation staying unavailable, and verify another guest never claims it.
Restore the same guest and reconcile explicitly. A whole-guest shutdown is an
operator action, not an HTTP controller operation.

Quiesce admissions and drain active turns before backup. Stop the single controller
and workers through their normal shutdown paths, then copy the complete retained
controller and worker state to an owner-only destination on a different machine.
Include SQLite files, configuration and credential references, retained home,
workspace, and ownership receipts. Verify checksums and rehearse restoration to
an isolated copy. An etcd snapshot alone does not back up local-path volumes.

For rollback, stop admissions, drain, and stop the companion. Keep all PVCs and
fencing records. Reinstall the previously verified image/configuration only after
confirming the old processes are stopped. Never use `kubectl delete namespace` as
a rollback, and never force-delete a Pod to manufacture a fencing receipt. Keep
the original independent agent running until the pilot acceptance checks pass.
