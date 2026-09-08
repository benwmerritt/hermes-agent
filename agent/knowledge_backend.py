"""Optional profile-scoped authority for native learning tools.

Configured by the embedding runtime before agent initialization. Exceptions propagate:
an unavailable authority must never turn a shared write into a local-only success.
"""
from contextvars import ContextVar
from hermes_constants import hermes_home_key

_backends = {}
staging = ContextVar("knowledge_staging", default=False)
operation_id = ContextVar("knowledge_operation_id", default=None)
operation_fingerprint = ContextVar("knowledge_operation_fingerprint", default=None)


def register_knowledge_backend(home, backend):
    key = hermes_home_key(home)
    if backend is None:
        _backends.pop(key, None)
    else:
        _backends[key] = backend


def get_knowledge_backend(home=None):
    return _backends.get(hermes_home_key(home))


def authoritative_skill_mutation(payload):
    backend = get_knowledge_backend()
    if backend is None or staging.get():
        return None
    return backend.mutate_skill(payload)


def approved_knowledge_replay(payload, callback):
    backend = get_knowledge_backend()
    identifier = payload.get("_authority_operation_id")
    if backend is None or not identifier or operation_id.get() is not None:
        return None
    return backend.replay_approved(identifier, payload, callback)
