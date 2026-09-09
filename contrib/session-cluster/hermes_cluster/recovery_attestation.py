"""Explicit operator assertions for a rebooted original node, never automatic fencing.

The operator must inspect the real node/runtime and retained home. Hashes bind
that submitted evidence; they do not authenticate Kubernetes or physical state.
"""
import hashlib
import math
import re
import time

from .ledger import OwnershipError, canonical


def _object(value, keys):
    if not isinstance(value, dict) or set(value) != set(keys):
        raise ValueError("operator evidence has missing or unexpected fields")
    return value


def validate(owner, body, *, now=None):
    _object(body, ("schema_version", "method", "expected_generation", "prior_identity",
                   "binding", "home", "inspection", "reconciliation_note"))
    if (type(body["schema_version"]) is not int or body["schema_version"] != 1
            or body["method"] != "original-node-reboot"
            or type(body["expected_generation"]) is not int
            or body["expected_generation"] != owner["generation"]
            or not 1 <= body["expected_generation"] < 999999999
            or body["prior_identity"] != owner["identity"]):
        raise OwnershipError("operator evidence does not match the retained owner")
    note = body["reconciliation_note"]
    if not isinstance(note, str) or not 10 <= len(note.strip()) <= 4000:
        raise ValueError("a bounded reconciliation note is required")
    binding = _object(body["binding"], ("node_name", "node_uid", "boot_id", "pvc_uid", "volume_name", "config_uid"))
    if any(not isinstance(v, str) or not 1 <= len(v) <= 200 for v in binding.values()):
        raise ValueError("node, volume and configuration identities are required")
    home = _object(body["home"], ("worker_id", "generation", "conversation_key", "source", "config_revision", "personality_digest"))
    if (home["worker_id"] != owner["id"] or type(home["generation"]) is not int
            or home["generation"] != owner["generation"]
            or home["conversation_key"] != owner["native_key"] or home["source"] != owner["source"]):
        raise OwnershipError("inspected home belongs to another retained conversation")
    inspection = _object(body["inspection"], ("observed_at", "shutdown_receipt_sha256", "start_receipt_sha256",
        "node_ready", "runtime_pod_uid_absent", "sandbox_pod_uid_absent", "pod_cgroup_absent", "no_other_claimant"))
    stamp = inspection["observed_at"]
    try:
        age = (time.time() if now is None else now) - stamp if type(stamp) in (int, float) else math.inf
    except OverflowError:
        age = math.inf
    if not math.isfinite(age) or not -30 <= age <= 600:
        raise ValueError("original node inspection must be current, within ten minutes")
    for field in ("node_ready", "runtime_pod_uid_absent", "sandbox_pod_uid_absent", "pod_cgroup_absent", "no_other_claimant"):
        if inspection[field] is not True:
            raise OwnershipError("operator has not established the original node fence")
    for value in (home["config_revision"], home["personality_digest"],
                  inspection["shutdown_receipt_sha256"], inspection["start_receipt_sha256"]):
        if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
            raise ValueError("evidence and saved configuration require SHA256 references")
    return {**body, "evidence_sha256": hashlib.sha256(canonical(body).encode()).hexdigest()}
