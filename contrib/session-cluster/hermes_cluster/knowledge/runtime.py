"""Worker bootstrap, immutable prompt materialization, and native tool adapters."""
from contextlib import contextmanager
import json
from pathlib import Path
import shutil
import tempfile
import threading
import yaml

from .client import KnowledgeClient
from .store import KnowledgeError, digest, encode, validate_resource


def _materialize(home, resources):
    from tools.memory_tool_store import ENTRY_DELIMITER
    home = Path(home)
    (home / "memories").mkdir(parents=True, exist_ok=True)
    for target, filename in (("memory", "MEMORY.md"), ("user", "USER.md")):
        entries = [r["value"]["content"] for r in resources if r["kind"] == "memory"
                   and r.get("value") and r["value"]["target"] == target]
        (home / "memories" / filename).write_text(ENTRY_DELIMITER.join(entries), encoding="utf-8")
    (home / "skills").mkdir(exist_ok=True)
    names = set()
    for row in resources:
        if row["kind"] != "skill" or row.get("value") is None:
            continue
        name = row["key"]
        if name in names:
            raise KnowledgeError("ambiguous skill name across readable audiences: " + name)
        names.add(name)
        validate_resource("skill", name, row["value"])
        for relative, content in row["value"]["files"].items():
            path = home / "skills" / name / relative
            if path.is_symlink() or any(p.is_symlink() for p in path.parents if p != home.parent):
                raise KnowledgeError("refusing symlink in knowledge projection")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")


@contextmanager
def _home_context(home):
    from hermes_constants import set_hermes_home_override, reset_hermes_home_override
    token = set_hermes_home_override(home)
    try:
        yield
    finally:
        reset_hermes_home_override(token)


class KnowledgeRuntime:
    def __init__(self, client, home, snapshot):
        self.client, self.home, self.snapshot = client, Path(home), snapshot
        self.snapshot_id = snapshot["snapshot_id"]
        self.client.snapshot_id = self.snapshot_id
        self.snapshot_home = self.home / ".knowledge-snapshot"
        self.prompt_skills_dir = self.snapshot_home / "skills"
        self.resources = {(r["kind"], r["key"], r["audience"]): r for r in snapshot["resources"]}
        self.lock = threading.RLock()
        self.last_history_db = None

    def create_memory_store(self, *args, **kwargs):
        from .memory import SharedMemoryStore
        return SharedMemoryStore(self, *args, **kwargs)

    def replay_approved(self, identifier, payload, callback):
        from agent.knowledge_backend import operation_id, operation_fingerprint
        with self.lock:
            with self.client.connect() as db:
                old = db.execute("SELECT route,request FROM outbox WHERE id=?", (identifier,)).fetchone()
            if old:
                request = json.loads(old[1])
                if request.get("origin", {}).get("approval_payload_hash") != digest(payload):
                    raise KnowledgeError("approved operation id reused with different payload", 409)
                receipt = self.client.mutate(old[0], request)
                self.accept(receipt)
                return {"success": True, "receipt": identifier, "replayed": True}
            token = operation_id.set(identifier)
            fingerprint_token = operation_fingerprint.set(digest(payload))
            try:
                return callback()
            finally:
                operation_id.reset(token)
                operation_fingerprint.reset(fingerprint_token)

    def accept(self, receipt):
        for row in receipt.get("resources", []):
            self.resources[(row["kind"], row["key"], row["audience"])] = row
        # The prompt projection lives in a different, immutable directory.
        for row in receipt.get("resources", []):
            if row["kind"] == "skill":
                root = self.home / "skills" / row["key"]
                if root.exists():
                    if root.is_symlink():
                        raise KnowledgeError("refusing symlink in skill projection")
                    shutil.rmtree(root)
        _materialize(self.home, list(self.resources.values()))

    def mutate_skill(self, payload):
        from agent.knowledge_backend import staging
        from tools import skill_manager_tool as smt
        with self.lock, tempfile.TemporaryDirectory(prefix="knowledge-stage-", dir=self.home) as temp:
            stage = Path(temp)
            # Stage only the worker's already-authorized local bundles; no shared mount.
            shutil.copytree(self.home / "skills", stage / "skills", symlinks=True)
            if any(p.is_symlink() for p in (stage / "skills").rglob("*")):
                return json.dumps({"success": False, "error": "symlinks cannot be staged as shared skills"})
            config = self.home / "config.yaml"
            if config.exists():
                shutil.copyfile(config, stage / "config.yaml")
            operations = payload.get("operations") or [payload]
            names = list(dict.fromkeys(op.get("name") or payload.get("name") for op in operations))
            if any(not isinstance(n, str) or "/" in n or "\\" in n or n in {"", ".", ".."} for n in names):
                return json.dumps({"success": False, "error": "shared skills require bare skill names"})
            # Resolve replay before local validation: the previous attempt may have committed remotely.
            before = [self.resources.get(("skill", n, self.snapshot["audience"])) for n in names]
            if any(r["kind"] == "skill" and r["key"] in names and r["audience"] != self.snapshot["audience"]
                   for r in self.resources.values()):
                return json.dumps({"success": False, "error": "read-only audience skill cannot be modified"})
            token = staging.set(True)
            gate_token = smt._skill_gate_bypass.set(True)
            try:
                with _home_context(stage):
                    raw = smt._skill_manage_from(payload)
            finally:
                smt._skill_gate_bypass.reset(gate_token)
                staging.reset(token)
            result = json.loads(raw)
            if not result.get("success"):
                return raw
            mutations = []
            for name, old in zip(names, before):
                root = stage / "skills" / name
                # Native category is allowed, but the authority canonicalizes by bare name.
                if not root.exists():
                    candidates = list((stage / "skills").rglob(f"{name}/SKILL.md"))
                    root = candidates[0].parent if len(candidates) == 1 else root
                files = {}
                if root.exists():
                    for path in root.rglob("*"):
                        if path.is_symlink():
                            return json.dumps({"success": False, "error": "symlinks cannot be published in skills"})
                        if path.is_file():
                            files[path.relative_to(root).as_posix()] = path.read_text(encoding="utf-8")
                mutations.append({"key": name, "expected_revision": old["revision"] if old else 0,
                                  "value": {"files": files} if files else None})
            try:
                receipt = self.client.mutate("/v1/skills/mutations", {"operations": mutations,
                    "origin": {"task_id": payload.get("task_id"), "session_id": payload.get("session_id")}})
                self.accept(receipt)
            except Exception as exc:
                return json.dumps({"success": False, "error": f"Shared skill write failed: {exc}"})
            result["receipt"] = receipt["mutation_id"]
            if "path" in result:
                result["path"] = str(self.home / "skills" / names[0])
            return json.dumps(result)

    def search_history(self, request):
        try:
            return json.dumps(self.client.post("/v1/history/search", request))
        except Exception as exc:
            return json.dumps({"success": False, "error": f"Shared history unavailable: {exc}"})

    def flush_history(self, db_path):
        from .outbox import flush_history
        with self.lock:
            self.last_history_db = Path(db_path)
            return flush_history(self.client, self.last_history_db)

    def close(self):
        from agent.knowledge_backend import register_knowledge_backend
        try:
            if self.last_history_db is not None:
                self.flush_history(self.last_history_db)
        finally:
            register_knowledge_backend(self.home, None)
            self.client.close()


def bootstrap_knowledge(*, url, token, hermes_home, snapshot_id=None):
    """Run under the worker's exclusive home lock, before constructing native agents.

    Existing homes pin their first snapshot automatically. A new conversation requires
    a new home. Errors are fatal; this function never substitutes an empty/local store.
    """
    from agent.knowledge_backend import register_knowledge_backend
    home = Path(hermes_home)
    home.mkdir(parents=True, exist_ok=True)
    config = home / "config.yaml"
    if config.exists():
        settings = (yaml.safe_load(config.read_text()) or {}).get("skills", {}) or {}
        if any(settings.get(k) for k in ("create_dir", "external_dirs", "trusted_project_dirs")):
            raise KnowledgeError("cluster worker skills must stay inside its audience-scoped home")
    pin_path = home / "knowledge-snapshot.json"
    if pin_path.exists():
        pin = json.loads(pin_path.read_text())
        if snapshot_id and snapshot_id != pin["snapshot_id"]:
            raise KnowledgeError("cannot change snapshot inside an existing conversation")
        snapshot_id = pin["snapshot_id"]
    client = KnowledgeClient(url, token, home)
    try:
        snapshot = client.post("/v1/snapshots", {"snapshot_id": snapshot_id})
        runtime = KnowledgeRuntime(client, home, snapshot)
        client.recover()
        if runtime.snapshot_home.exists():
            shutil.rmtree(runtime.snapshot_home)
        _materialize(runtime.snapshot_home, snapshot["resources"])
        if config.exists():
            shutil.copyfile(config, runtime.snapshot_home / "config.yaml")
        # Bootstrap owns this private projection, including removal of stale/deleted files.
        skills = home / "skills"
        if skills.exists():
            if skills.is_symlink():
                raise KnowledgeError("skill projection cannot be a symlink")
            shutil.rmtree(skills)
        runtime.accept({"resources": client.accepted_resources()})
        from utils import atomic_write_text
        atomic_write_text(pin_path, encode({"snapshot_id": snapshot["snapshot_id"]}))
        register_knowledge_backend(home, runtime)
        return runtime
    except BaseException:
        client.close()
        raise
