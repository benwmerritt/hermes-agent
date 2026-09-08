"""Namespace-scoped retained workers. The router owns admission and recovery decisions."""

from __future__ import annotations

import base64
import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
import re
import ssl
from typing import Any

import httpx
import yaml


LABEL = "hermes-cluster/conversation"
MANAGED = {"app.kubernetes.io/managed-by": "hermes-session-cluster"}
_DNS = re.compile(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?")


class KubernetesError(RuntimeError):
    """A sanitized API failure. Response bodies can contain private configuration."""


class OwnershipConflict(KubernetesError):
    """Existing resources do not belong to this exact worker generation."""


@dataclass(frozen=True)
class WorkerIdentity:
    conversation_id: str
    generation: int
    pod_name: str
    pod_uid: str
    pvc_name: str
    config_name: str
    secret_name: str
    pvc_uid: str
    volume_name: str | None = None


@dataclass(frozen=True)
class WorkerStatus:
    phase: str
    ready: bool = False
    node_name: str | None = None
    pod_ip: str | None = None
    terminated: bool = False
    reason: str | None = None


def _name(value: str) -> str:
    if not isinstance(value, str) or len(value) > 63 or not _DNS.fullmatch(value):
        raise ValueError("Resource names must be DNS labels, at most 63 characters")
    return value


def _conversation_key(conversation_id: str) -> str:
    if not conversation_id:
        raise ValueError("conversation_id is required")
    return hashlib.sha256(conversation_id.encode()).hexdigest()[:32]


def _terminated(pod: dict[str, Any]) -> bool:
    """Absent/evicted/Failed alone is not proof the old process stopped."""
    status = pod.get("status", {})
    states = status.get("containerStatuses", [])
    expected = {c["name"] for c in pod.get("spec", {}).get("containers", [])}
    return bool(expected) and expected == {c["name"] for c in states} and all(
        "terminated" in c.get("state", {}) for c in states
    ) and all(
        "terminated" in c.get("state", {})
        for c in status.get("initContainerStatuses", [])
    )


class KubernetesBackend:
    """Use the mounted service account; never export kubeconfig or log API bodies.

    Workers connect outbound to the companion, so no per-worker Service is needed.
    The caller persists identity before dispatch and never calls create_worker for
    a replacement until its durable ledger has recorded termination or fencing.
    """

    def __init__(
        self,
        namespace: str,
        image: str,
        *,
        hermes_config: dict[str, Any] | None = None,
        storage_class: str = "hermes-session-local",
        storage_size: str = "5Gi",
        worker_service_account: str = "hermes-worker",
        resources: dict[str, Any] | None = None,
        client: httpx.AsyncClient | None = None,
        token_path: str = "/var/run/secrets/kubernetes.io/serviceaccount/token",
        ca_path: str = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
        api_url: str | None = None,
    ):
        self.namespace = _name(namespace)
        self.image = image
        self.hermes_config = hermes_config or {}
        self.storage_class = _name(storage_class)
        self.storage_size = storage_size
        self.worker_service_account = _name(worker_service_account)
        self.resources = resources or {
            "requests": {"cpu": "250m", "memory": "512Mi", "ephemeral-storage": "256Mi"},
            "limits": {"cpu": "2", "memory": "1Gi", "ephemeral-storage": "2Gi"},
        }
        self._token_path = Path(token_path)
        self._owns_client = client is None
        if client is None:
            host = os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
            port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
            client = httpx.AsyncClient(
                base_url=api_url or f"https://{host}:{port}",
                verify=ssl.create_default_context(cafile=ca_path),
                timeout=httpx.Timeout(30, connect=10),
                trust_env=False,
            )
        self.client = client
        self._base = f"/api/v1/namespaces/{self.namespace}"

    async def aclose(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        headers = {}
        if self._owns_client:
            # Projected service-account tokens rotate during long controller lifetimes.
            headers["Authorization"] = "Bearer " + self._token_path.read_text().strip()
        try:
            response = await self.client.request(method, self._base + path, headers=headers, **kwargs)
        except httpx.HTTPError as exc:
            raise KubernetesError(f"Kubernetes {method} transport failed; reconcile before retry") from exc
        if response.status_code not in {200, 201, 202, 404, 409}:
            raise KubernetesError(f"Kubernetes {method} returned HTTP {response.status_code}")
        return response

    async def _get(self, kind: str, name: str) -> dict[str, Any] | None:
        response = await self._request("GET", f"/{kind}/{_name(name)}")
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise KubernetesError(f"Kubernetes GET {kind} did not return an object")
        return response.json()

    async def _create(self, kind: str, body: dict[str, Any]) -> dict[str, Any]:
        response = await self._request("POST", f"/{kind}", json=body)
        if response.status_code == 201:
            return response.json()
        if response.status_code == 409:
            existing = await self._get(kind, body["metadata"]["name"])
            if existing is not None:
                return existing
        raise KubernetesError(f"Kubernetes create {kind} failed; reconcile before retry")

    async def create_credentials(self, name: str, mapping: dict[str, str]) -> str:
        """Create/reuse an immutable exact-name Secret; never enumerate Secrets."""
        name = _name(name)
        if not mapping or any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", k) for k in mapping):
            raise ValueError("Credentials must use valid environment variable names")
        data = {k: base64.b64encode(v.encode()).decode() for k, v in mapping.items()}
        body = {
            "apiVersion": "v1", "kind": "Secret",
            "metadata": {"name": name, "labels": MANAGED},
            "type": "Opaque", "immutable": True, "data": data,
        }
        secret = await self._create("secrets", body)
        if secret.get("data") != data or not secret.get("immutable") or any(
            secret.get("metadata", {}).get("labels", {}).get(k) != v for k, v in MANAGED.items()
        ):
            raise OwnershipConflict("Existing credential Secret differs; it was not overwritten")
        return name

    async def create_worker(
        self,
        conversation_id: str,
        generation: int,
        worker_config: dict[str, Any],
        credential_ref: str,
        *,
        require_existing_claim: bool = False,
        expected_claim_uid: str | None = None,
        expected_volume_name: str | None = None,
    ) -> WorkerIdentity:
        if type(generation) is not int or not 1 <= generation <= 999999999:
            raise ValueError("generation must be a positive bounded integer")
        key = _conversation_key(conversation_id)
        name = f"hsc-{key}-g{generation}"
        pvc_name = f"hsc-{key}"
        credential_ref = _name(credential_ref)
        labels = {**MANAGED, LABEL: key, "hermes-cluster/role": "worker"}
        config_data = {
            "worker.json": json.dumps(worker_config, sort_keys=True, separators=(",", ":")),
            "hermes.yaml": yaml.safe_dump(self.hermes_config, sort_keys=True),
        }
        signature = hashlib.sha256(json.dumps({
            "config": config_data, "image": self.image, "secret": credential_ref,
            "resources": self.resources, "service_account": self.worker_service_account,
        }, sort_keys=True).encode()).hexdigest()
        response = await self._request("GET", "/pods", params={"labelSelector": f"{LABEL}={key}"})
        if response.status_code != 200:
            raise KubernetesError("Cannot verify existing conversation ownership")
        for pod in response.json().get("items", []):
            if pod["metadata"]["name"] != name and not _terminated(pod):
                raise OwnershipConflict("Prior worker has not been confirmed terminated")
        claim_body = {
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": pvc_name, "labels": labels},
            "spec": {"accessModes": ["ReadWriteOnce"], "storageClassName": self.storage_class,
                     "resources": {"requests": {"storage": self.storage_size}}},
        }
        if generation > 1 or require_existing_claim or expected_claim_uid or expected_volume_name:
            pvc = await self._get("persistentvolumeclaims", pvc_name)
            if pvc is None:
                raise OwnershipConflict("Retained claim is missing; refusing fresh storage")
        else:
            pvc = await self._create("persistentvolumeclaims", claim_body)
        if (pvc.get("metadata", {}).get("labels", {}).get(LABEL) != key
                or pvc.get("metadata", {}).get("ownerReferences")
                or pvc.get("spec", {}).get("storageClassName") != self.storage_class
                or pvc.get("status", {}).get("phase") == "Lost"):
            raise OwnershipConflict("Retained claim is incompatible or lost; refusing fresh storage")
        if expected_claim_uid and pvc["metadata"]["uid"] != expected_claim_uid:
            raise OwnershipConflict("Retained claim UID changed; refusing replacement storage")
        if expected_volume_name and pvc.get("spec", {}).get("volumeName") != expected_volume_name:
            raise OwnershipConflict("Retained claim volume changed; refusing replacement storage")
        config = await self._create("configmaps", {
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": name, "labels": labels},
            "immutable": True, "data": config_data,
        })
        if config.get("data") != config_data or not config.get("immutable"):
            raise OwnershipConflict("Generation configuration differs; it was not overwritten")
        pod = await self._create("pods", {
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "labels": labels,
                         "annotations": {"hermes-cluster/spec-sha256": signature}},
            "spec": {
                "restartPolicy": "Never", "serviceAccountName": self.worker_service_account,
                "automountServiceAccountToken": False, "enableServiceLinks": False,
                "terminationGracePeriodSeconds": 120,
                # Default 300s node-loss eviction would delete our termination evidence.
                # Only the controller may authorize recovery of this retained owner.
                "tolerations": [
                    {"key": f"node.kubernetes.io/{condition}", "operator": "Exists",
                     "effect": "NoExecute"}
                    for condition in ("not-ready", "unreachable")],
                "securityContext": {"runAsNonRoot": True, "runAsUser": 10000,
                    "runAsGroup": 10000, "fsGroup": 10000, "fsGroupChangePolicy": "OnRootMismatch",
                    "seccompProfile": {"type": "RuntimeDefault"}},
                "topologySpreadConstraints": [{"maxSkew": 1,
                    "topologyKey": "kubernetes.io/hostname", "whenUnsatisfiable": "ScheduleAnyway",
                    "labelSelector": {"matchLabels": {"hermes-cluster/role": "worker"}}}],
                "containers": [{
                    "name": "worker", "image": self.image, "imagePullPolicy": "IfNotPresent",
                    "command": ["python", "-m", "hermes_cluster.worker", "--config", "/config/worker.json"],
                    "env": [{"name": "HERMES_HOME", "value": "/data/hermes"},
                            {"name": "HOME", "value": "/data"}],
                    "envFrom": [{"secretRef": {"name": credential_ref}}],
                    "resources": self.resources,
                    "securityContext": {"allowPrivilegeEscalation": False,
                                        "capabilities": {"drop": ["ALL"]}},
                    "volumeMounts": [{"name": "state", "mountPath": "/data"},
                        {"name": "config", "mountPath": "/config", "readOnly": True},
                        {"name": "credentials", "mountPath": "/credentials", "readOnly": True}],
                    "readinessProbe": {"exec": {"command": ["python", "-c",
                        "import json,os; s=json.load(open('/data/hermes/cluster-worker-status.json')); "
                        "assert s['state']=='ready'; os.kill(s['pid'],0)"]},
                        "initialDelaySeconds": 2, "periodSeconds": 5, "timeoutSeconds": 2},
                }],
                "volumes": [{"name": "state", "persistentVolumeClaim": {"claimName": pvc_name}},
                    {"name": "config", "configMap": {"name": name}},
                    {"name": "credentials", "secret": {"secretName": credential_ref, "defaultMode": 0o440}}],
            },
        })
        if pod.get("metadata", {}).get("annotations", {}).get("hermes-cluster/spec-sha256") != signature:
            raise OwnershipConflict("Pod name exists with a different worker specification")
        return WorkerIdentity(conversation_id, generation, name, pod["metadata"]["uid"],
                              pvc_name, name, credential_ref, pvc["metadata"]["uid"],
                              pvc.get("spec", {}).get("volumeName"))

    async def status(self, identity: WorkerIdentity) -> WorkerStatus:
        pod = await self._get("pods", identity.pod_name)
        if pod is None:
            return WorkerStatus("Unknown", reason="PodAbsentIsNotFenceProof")
        if pod["metadata"]["uid"] != identity.pod_uid:
            return WorkerStatus("Unknown", reason="PodUIDChanged")
        status = pod.get("status", {})
        ready = not pod["metadata"].get("deletionTimestamp") and any(
            c.get("type") == "Ready" and c.get("status") == "True"
            for c in status.get("conditions", [])
        )
        return WorkerStatus(status.get("phase", "Pending"), ready,
                            pod.get("spec", {}).get("nodeName"), status.get("podIP"),
                            _terminated(pod), status.get("reason"))

    async def stop_worker(self, identity: WorkerIdentity) -> WorkerStatus:
        """Request graceful deletion; never equate acceptance/absence with fencing.

        Call only after application drain. Persist a termination or external fence
        receipt in the router ledger before authorizing a replacement generation.
        """
        state = await self.status(identity)
        if state.reason in {"PodUIDChanged", "PodAbsentIsNotFenceProof"}:
            return state
        response = await self._request("DELETE", f"/pods/{_name(identity.pod_name)}", json={
            "apiVersion": "v1", "kind": "DeleteOptions",
            "preconditions": {"uid": identity.pod_uid}, "gracePeriodSeconds": 120,
        })
        if response.status_code == 409:
            raise OwnershipConflict("Pod identity changed before stop; nothing else was deleted")
        return state
