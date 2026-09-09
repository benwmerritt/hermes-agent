"""Best-effort native audit/usage metadata for accepted authority transactions."""
from contextlib import suppress


def capture_before(names):
    from tools import skill_ledger, skill_manager_tool
    captured = {}
    for name in names:
        with suppress(Exception):
            found = skill_manager_tool._find_skill(name)
            captured[name] = skill_ledger.capture_before(
                found["path"] if found else None, complete_package=True, skill=name)
    return captured


def record_accepted(receipt, previous, captured, payload):
    from tools.skill_manager_tool import _record_success
    operations = payload.get("operations") or [payload]
    for resource, old in zip(receipt["resources"], previous):
        name = resource["key"]
        action = "delete" if resource["value"] is None else ("patch" if old and old.get("value") else "create")
        # One CAS bundle transaction is one native audit event, even when its staged
        # implementation used several file operations. Never audit rejected staging.
        with suppress(Exception):
            _record_success(action, name, {"success": True}, file_path=None,
                absorbed_into=payload.get("absorbed_into"), task_id=payload.get("task_id"),
                session_id=payload.get("session_id"), ledger_before=captured.get(name),
                refresh_prompt=False, push_sync=False,
                extra_evidence={"authority_receipt": receipt["mutation_id"], "atomic_bundle": True,
                    "operations": [op.get("action") for op in operations
                                   if (op.get("name") or payload.get("name")) == name]})
