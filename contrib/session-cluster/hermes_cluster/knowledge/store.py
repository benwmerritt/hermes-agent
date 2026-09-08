"""Local SQLite authority. Only the controller can grant worker capabilities."""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import sqlite3
import time
import uuid


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value):
    return hashlib.sha256(encode(value).encode()).hexdigest()


class KnowledgeError(Exception):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


from .history import HistoryStore


class KnowledgeStore(HistoryStore):
    def __init__(self, path, *, max_bytes=512 * 1024 * 1024):
        if str(path) == ":memory:":
            raise ValueError("knowledge authority requires a durable local database path")
        self.path = Path(path)
        if not isinstance(max_bytes, int) or max_bytes < 128 * 1024:
            raise ValueError("knowledge max_bytes must be at least 128 KiB")
        self.max_bytes = max_bytes
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.transaction() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS grants (
                    token TEXT PRIMARY KEY, agent TEXT NOT NULL, conversation TEXT NOT NULL,
                    generation INTEGER NOT NULL, audience TEXT NOT NULL, reads TEXT NOT NULL,
                    active INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS resources (
                    agent TEXT NOT NULL, audience TEXT NOT NULL, kind TEXT NOT NULL,
                    key TEXT NOT NULL, revision INTEGER NOT NULL, value TEXT,
                    provenance TEXT NOT NULL, PRIMARY KEY(agent,audience,kind,key));
                CREATE TABLE IF NOT EXISTS receipts (
                    agent TEXT NOT NULL, conversation TEXT NOT NULL, id TEXT NOT NULL,
                    request_hash TEXT NOT NULL, result TEXT NOT NULL,
                    PRIMARY KEY(agent,conversation,id));
                CREATE TABLE IF NOT EXISTS snapshots (
                    id TEXT PRIMARY KEY, agent TEXT NOT NULL, conversation TEXT NOT NULL,
                    access TEXT NOT NULL, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS history (
                    agent TEXT NOT NULL, audience TEXT NOT NULL, conversation TEXT NOT NULL,
                    session TEXT NOT NULL, message TEXT NOT NULL, revision INTEGER NOT NULL,
                    payload TEXT NOT NULL, provenance TEXT NOT NULL,
                    PRIMARY KEY(agent,conversation,session,message));
            """)

    @contextmanager
    def transaction(self):
        db = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA busy_timeout=15000")
            page_size = db.execute("PRAGMA page_size").fetchone()[0]
            db.execute(f"PRAGMA max_page_count={self.max_bytes // page_size}")
            db.execute("PRAGMA journal_size_limit=4194304")
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def register_worker(self, *, token, agent_id, conversation_key, generation,
                        audience, read_audiences=None):
        """Trusted Python API; never exposed by the HTTP router. Same generation may rotate keys."""
        if not all(isinstance(x, str) and x for x in (token, agent_id, conversation_key, audience)):
            raise ValueError("worker grant fields must be nonempty strings")
        if len(token) < 24 or not isinstance(generation, int) or generation < 1:
            raise ValueError("use an opaque token of at least 24 characters and positive generation")
        reads = sorted(set(read_audiences or [audience]) | {audience})
        with self.transaction() as db:
            previous = db.execute("SELECT max(generation) FROM grants WHERE agent=? AND conversation=?",
                                  (agent_id, conversation_key)).fetchone()[0]
            if previous is not None and generation < previous:
                raise KnowledgeError("stale registration", 409)
            db.execute("UPDATE grants SET active=0 WHERE agent=? AND conversation=?",
                       (agent_id, conversation_key))
            db.execute("INSERT INTO grants VALUES(?,?,?,?,?,?,1) ON CONFLICT(token) DO UPDATE SET "
                       "agent=excluded.agent,conversation=excluded.conversation,generation=excluded.generation,"
                       "audience=excluded.audience,reads=excluded.reads,active=1",
                       (digest(token), agent_id, conversation_key, generation, audience, encode(reads)))

    def revoke_worker(self, conversation_key, generation=None, *, agent_id=None):
        with self.transaction() as db:
            clauses, args = ["conversation=?"], [conversation_key]
            for col, value in (("generation", generation), ("agent", agent_id)):
                if value is not None:
                    clauses.append(f"{col}=?")
                    args.append(value)
            db.execute("UPDATE grants SET active=0 WHERE " + " AND ".join(clauses), args)

    def _authorize(self, db, token):
        row = db.execute("SELECT * FROM grants WHERE token=? AND active=1", (digest(token),)).fetchone()
        if row is None:
            raise KnowledgeError("worker credential revoked or invalid", 403)
        return dict(row)

    def _require_write_capacity(self, db):
        pages = db.execute("PRAGMA page_count").fetchone()[0] - db.execute("PRAGMA freelist_count").fetchone()[0]
        size = db.execute("PRAGMA page_size").fetchone()[0]
        if pages * size >= self.max_bytes * .90:
            raise KnowledgeError("knowledge storage budget exhausted; archive or expand before further writes", 507)

    @staticmethod
    def _visible(grant, row):
        return row["agent"] == grant["agent"] and row["audience"] in json.loads(grant["reads"])

    @staticmethod
    def _provenance(grant, origin):
        return {"agent_id": grant["agent"], "conversation_key": grant["conversation"],
                "generation": grant["generation"], "audience": grant["audience"],
                "origin": origin, "recorded_at": time.time()}

    def snapshot(self, token, snapshot_id=None):
        with self.transaction() as db:
            grant = self._authorize(db, token)
            access = digest([grant["audience"], grant["reads"]])
            if snapshot_id:
                row = db.execute("SELECT * FROM snapshots WHERE id=?", (snapshot_id,)).fetchone()
                if row is None or (row["agent"], row["conversation"], row["access"]) != (
                        grant["agent"], grant["conversation"], access):
                    raise KnowledgeError("snapshot not found", 404)
                return json.loads(row["payload"])
            self._require_write_capacity(db)
            rows = db.execute("SELECT * FROM resources WHERE agent=? ORDER BY kind,key,audience", (grant["agent"],))
            resources, size = [], 0
            from .limits import MAX_SNAPSHOT_BYTES
            for raw in rows:
                if not self._visible(grant, raw):
                    continue
                row = dict(raw)
                size += len(encode(row).encode())
                if size > MAX_SNAPSHOT_BYTES:
                    raise KnowledgeError("knowledge snapshot exceeds 8 MiB; operator pruning required", 507)
                row["value"] = json.loads(row["value"]) if row["value"] is not None else None
                row["provenance"] = json.loads(row["provenance"])
                resources.append(row)
            payload = {"snapshot_id": str(uuid.uuid4()), "resources": resources,
                       "audience": grant["audience"], "created_at": time.time()}
            db.execute("INSERT INTO snapshots VALUES(?,?,?,?,?)", (
                payload["snapshot_id"], grant["agent"], grant["conversation"], access, encode(payload)))
            return payload

    def _receipt(self, db, grant, request):
        origin = request.get("origin", {})
        if not isinstance(origin, dict) or len(encode(origin).encode()) > 4096:
            raise KnowledgeError("origin must be an object of at most 4 KiB")
        mutation_id = request.get("mutation_id")
        if not isinstance(mutation_id, str) or not 1 <= len(mutation_id) <= 200:
            raise KnowledgeError("mutation_id is required (at most 200 characters)")
        row = db.execute("SELECT * FROM receipts WHERE agent=? AND conversation=? AND id=?",
                         (grant["agent"], grant["conversation"], mutation_id)).fetchone()
        if row:
            if row["request_hash"] != digest(request):
                raise KnowledgeError("mutation_id reused with different content", 409)
            result = json.loads(row["result"])
            if any(r["audience"] != grant["audience"] for r in result.get("resources", [])):
                raise KnowledgeError("receipt not found", 404)
            return result

    def _save_receipt(self, db, grant, request, result):
        db.execute("INSERT INTO receipts VALUES(?,?,?,?,?)", (
            grant["agent"], grant["conversation"], request["mutation_id"], digest(request), encode(result)))
        return result

    def receipt(self, token, mutation_id):
        with self.transaction() as db:
            grant = self._authorize(db, token)
            row = db.execute("SELECT result FROM receipts WHERE agent=? AND conversation=? AND id=?",
                             (grant["agent"], grant["conversation"], mutation_id)).fetchone()
            if row is None:
                raise KnowledgeError("receipt not found", 404)
            result = json.loads(row[0])
            if any(r["audience"] != grant["audience"] for r in result.get("resources", [])):
                raise KnowledgeError("receipt not found", 404)
            return result

    def mutate(self, token, kind, request):
        if kind not in {"memory", "skill"}:
            raise KnowledgeError("unknown resource kind")
        operations = request.get("operations")
        if not isinstance(operations, list) or not 1 <= len(operations) <= 100:
            raise KnowledgeError("operations must contain 1 to 100 entries")
        with self.transaction() as db:
            grant = self._authorize(db, token)
            previous = self._receipt(db, grant, request)
            if previous is not None:
                return previous
            self._require_write_capacity(db)
            updated, touched = [], set()
            for op in operations:
                if not isinstance(op, dict):
                    raise KnowledgeError("operations must contain objects")
                key, expected = op.get("key"), op.get("expected_revision")
                if not isinstance(key, str) or not key or len(key) > 200 or key in touched:
                    raise KnowledgeError("keys must be nonempty, bounded and unique in a batch")
                if not isinstance(expected, int) or expected < 0:
                    raise KnowledgeError("expected_revision must be a nonnegative integer")
                touched.add(key)
                value = op.get("value")
                validate_resource(kind, key, value)
                row = db.execute("SELECT revision FROM resources WHERE agent=? AND audience=? AND kind=? AND key=?",
                    (grant["agent"], grant["audience"], kind, key)).fetchone()
                revision = row[0] if row else 0
                if revision != expected:
                    raise KnowledgeError(f"CAS conflict for {key}: expected {expected}, current {revision}", 409)
                provenance = self._provenance(grant, request.get("origin", {}))
                db.execute("INSERT INTO resources VALUES(?,?,?,?,?,?,?) ON CONFLICT(agent,audience,kind,key) "
                           "DO UPDATE SET revision=excluded.revision,value=excluded.value,provenance=excluded.provenance",
                    (grant["agent"], grant["audience"], kind, key, revision + 1,
                     encode(value) if value is not None else None, encode(provenance)))
                updated.append({"kind": kind, "key": key, "revision": revision + 1, "value": value,
                                "audience": grant["audience"], "provenance": provenance})
            return self._save_receipt(db, grant, request, {
                "success": True, "mutation_id": request["mutation_id"], "resources": updated})


def validate_resource(kind, key, value):
    if value is None:
        return
    if not isinstance(value, dict):
        raise KnowledgeError("resource value must be an object")
    if kind == "memory":
        if value.get("target") not in {"memory", "user"} or not isinstance(value.get("content"), str):
            raise KnowledgeError("memory requires target and content")
        if not value["content"].strip() or len(value["content"]) > 10000:
            raise KnowledgeError("memory entry must contain 1 to 10000 characters")
        return
    if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,63}", key) or key in {".", ".."}:
        raise KnowledgeError("invalid skill name")
    files = value.get("files")
    if not isinstance(files, dict) or "SKILL.md" not in files or len(files) > 100:
        raise KnowledgeError("skill bundle requires SKILL.md and at most 100 files")
    if len(encode(files).encode()) > 2_000_000:
        raise KnowledgeError("skill bundle exceeds 2 MB")
    for path, content in files.items():
        if not isinstance(path, str):
            raise KnowledgeError("skill file paths must be strings")
        p = PurePosixPath(path)
        if (not path or p.is_absolute() or any(part in {".", ".."} for part in path.split("/"))
                or "\\" in path or ":" in path or not isinstance(content, str)):
            raise KnowledgeError("skill files must be relative UTF-8 text files without traversal")
