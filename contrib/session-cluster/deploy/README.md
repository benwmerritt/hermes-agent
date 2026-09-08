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
Record a successful process exit or an operator fencing receipt before deliberate
recovery. Guest loss must not start a replacement on another node. Local PV node
affinity keeps the retained home on its original guest. The recovery call must
require the existing PVC, check its UID, and check its bound PV name when known.
A missing, Lost, or substituted claim blocks recovery rather than creating an
empty home. PVCs, ConfigMaps, and Secrets survive worker stop; cleanup is explicit.

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
