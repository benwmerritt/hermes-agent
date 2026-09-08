"""Durable admission and delivery records owned by one controller process.

An acknowledged Relay delivery means admission, not completed execution. Once
we attempt a wire send we never replay it automatically after uncertainty.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from pathlib import Path
import secrets
import sqlite3
import threading
import time
from contextlib import contextmanager


class AdmissionError(ValueError):
    pass


class OwnershipError(PermissionError):
    pass


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class Ledger:
    def __init__(self, path: Path, *, max_workers: int = 3, max_queued: int = 100,
                 max_ledger_bytes: int = 128 * 1024 * 1024):
        if max_workers < 1 or max_queued < 1:
            raise ValueError("capacity limits must be positive")
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        path.touch(mode=0o600, exist_ok=True)
        os.chmod(path, 0o600)
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.execute("PRAGMA wal_autocheckpoint=128")
        self.db.execute("PRAGMA journal_size_limit=4194304")
        page_size = self.db.execute("PRAGMA page_size").fetchone()[0]
        if max_ledger_bytes < 1024 * 1024:
            raise ValueError("retained ledger budget must be at least 1 MiB")
        self.max_ledger_bytes = max_ledger_bytes
        self.db.execute(f"PRAGMA max_page_count={max_ledger_bytes // page_size}")
        self.lock = threading.RLock()
        self.max_workers, self.max_queued = max_workers, max_queued
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS conversations (
              id TEXT PRIMARY KEY, native_key TEXT NOT NULL, source TEXT NOT NULL,
              audience TEXT NOT NULL, actor TEXT NOT NULL, generation INTEGER NOT NULL,
              status TEXT NOT NULL, relay_secret TEXT NOT NULL, knowledge_token TEXT NOT NULL,
              identity TEXT, detail TEXT, created REAL NOT NULL, updated REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ingress (
              id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL REFERENCES conversations(id),
              payload TEXT NOT NULL, status TEXT NOT NULL, generation INTEGER,
              created REAL NOT NULL, updated REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS outbound (
              worker_id TEXT NOT NULL, request_id TEXT NOT NULL, action TEXT NOT NULL,
              status TEXT NOT NULL, result TEXT, created REAL NOT NULL,
              PRIMARY KEY(worker_id, request_id)
            );
            CREATE TABLE IF NOT EXISTS messages (
              message_id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL,
              generation INTEGER NOT NULL, kind TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS prompts (
              id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL, generation INTEGER NOT NULL,
              actor TEXT NOT NULL, channel_id TEXT NOT NULL, message_id TEXT,
              options TEXT NOT NULL, expires REAL NOT NULL, response TEXT
            );
            CREATE TABLE IF NOT EXISTS audit (
              id INTEGER PRIMARY KEY, at REAL NOT NULL, conversation_id TEXT,
              event TEXT NOT NULL, detail TEXT NOT NULL
            );
        """)

    @contextmanager
    def transaction(self):
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                yield self.db
                self.db.execute("COMMIT")
            except BaseException:
                self.db.execute("ROLLBACK")
                raise

    @staticmethod
    def _row(row):
        if row is None:
            return None
        result = dict(row)
        for key in ("source", "identity", "payload", "result", "options"):
            if key in result and result[key] is not None:
                result[key] = json.loads(result[key])
        return result

    def get(self, conversation_id: str):
        with self.lock:
            return self._row(self.db.execute("SELECT * FROM conversations WHERE id=?", (conversation_id,)).fetchone())

    def conversations(self):
        with self.lock:
            return [self._row(r) for r in self.db.execute("SELECT * FROM conversations ORDER BY created")]

    def record(self, conversation_id: str | None, event: str, detail: dict):
        with self.transaction() as db:
            db.execute("INSERT INTO audit(at,conversation_id,event,detail) VALUES(?,?,?,?)",
                       (time.time(), conversation_id, event, canonical(detail)))

    def admit(self, *, agent_id: str, native_key: str, source: dict, audience: str,
              event_id: str, payload: dict, actor: str) -> tuple[dict, bool]:
        encoded = canonical(payload)
        if len(encoded.encode()) > 262144:
            raise AdmissionError("message exceeds controller admission limit")
        cid = hashlib.sha256(canonical([agent_id, source.get("scope_id"), native_key]).encode()).hexdigest()[:24]
        now = time.time()
        with self.transaction() as db:
            previous = db.execute("SELECT * FROM ingress WHERE id=?", (event_id,)).fetchone()
            if previous:
                if previous["conversation_id"] != cid or previous["payload"] != encoded:
                    raise AdmissionError("duplicate event identifier changed its content or destination")
                return self._row(db.execute("SELECT * FROM conversations WHERE id=?", (cid,)).fetchone()), False
            used = db.execute("PRAGMA page_count").fetchone()[0] * db.execute("PRAGMA page_size").fetchone()[0]
            if used + len(encoded.encode()) > self.max_ledger_bytes * 0.75:
                raise AdmissionError("retained ledger budget reached; operator archival is required before new messages")
            if db.execute("SELECT count(*) FROM ingress WHERE status='queued'").fetchone()[0] >= self.max_queued:
                raise AdmissionError("queue is full; this message was not admitted")
            row = db.execute("SELECT * FROM conversations WHERE id=?", (cid,)).fetchone()
            if row is None:
                db.execute("INSERT INTO conversations VALUES(?,?,?,?,?,?,?, ?,?,?,?, ?,?)",
                           (cid, native_key, canonical(source), audience, actor, 1, "queued",
                            secrets.token_urlsafe(32), secrets.token_urlsafe(32), None, None, now, now))
            elif row["audience"] != audience or row["actor"] != actor:
                raise OwnershipError("conversation audience or owner changed; operator reconciliation required")
            db.execute("INSERT INTO ingress VALUES(?,?,?,'queued',NULL,?,?)", (event_id, cid, encoded, now, now))
            return self._row(db.execute("SELECT * FROM conversations WHERE id=?", (cid,)).fetchone()), True

    def reserve_next(self):
        with self.transaction() as db:
            used = db.execute("SELECT count(*) FROM conversations WHERE status NOT IN ('queued','stopped')").fetchone()[0]
            if used >= self.max_workers:
                return None
            row = db.execute("SELECT * FROM conversations WHERE status='queued' ORDER BY created LIMIT 1").fetchone()
            if row is None:
                return None
            db.execute("UPDATE conversations SET status='provisioning',updated=? WHERE id=?", (time.time(), row["id"]))
            return self._row(db.execute("SELECT * FROM conversations WHERE id=?", (row["id"],)).fetchone())

    def transition(self, cid: str, generation: int, status: str, *, detail: str = "", identity: dict | None = None):
        with self.transaction() as db:
            changed = db.execute("UPDATE conversations SET status=?, detail=?, identity=COALESCE(?,identity), updated=? WHERE id=? AND generation=?",
                                 (status, detail, canonical(identity) if identity else None, time.time(), cid, generation)).rowcount
            if changed != 1:
                raise OwnershipError("stale owner generation")

    def queued(self, cid: str):
        with self.lock:
            return [self._row(r) for r in self.db.execute("SELECT * FROM ingress WHERE conversation_id=? AND status='queued' ORDER BY created", (cid,))]

    def dispatch(self, event_id: str, cid: str, generation: int):
        with self.transaction() as db:
            owner = db.execute("SELECT generation,status FROM conversations WHERE id=?", (cid,)).fetchone()
            if not owner or owner["generation"] != generation or owner["status"] != "ready":
                raise OwnershipError("owner is not ready")
            if db.execute("UPDATE ingress SET status='dispatched',generation=?,updated=? WHERE id=? AND conversation_id=? AND status='queued'",
                          (generation, time.time(), event_id, cid)).rowcount != 1:
                raise AdmissionError("event has already been dispatched")

    def acknowledge(self, event_id: str, cid: str, generation: int):
        with self.transaction() as db:
            if not db.execute("SELECT 1 FROM ingress WHERE id=? AND conversation_id=? AND generation=?", (event_id, cid, generation)).fetchone():
                raise OwnershipError("acknowledgment belongs to another owner")
            db.execute("UPDATE ingress SET status='admitted',updated=? WHERE id=? AND status='dispatched'", (time.time(), event_id))

    def authenticate(self, authorization: str):
        if not authorization.startswith("Bearer "):
            raise OwnershipError("missing worker credential")
        try:
            token = authorization[7:]
            raw = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)).decode()
            worker_id, expiry, signature = raw.rsplit(":", 2)
            cid, gen = worker_id.rsplit(":", 1)
            expiry, gen = int(expiry), int(gen)
        except (ValueError, UnicodeError) as exc:
            raise OwnershipError("invalid worker credential") from exc
        row = self.get(cid)
        if (not row or row["generation"] != gen or row["status"] in ("queued", "stopped", "recovery_required")
                or expiry < time.time() or expiry > time.time() + 600):
            raise OwnershipError("expired or revoked worker credential")
        expected = hmac.new(row["relay_secret"].encode(), f"{worker_id}:{expiry}".encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, signature):
            raise OwnershipError("invalid worker credential")
        return row

    def begin_outbound(self, worker_id: str, request_id: str, action: dict):
        with self.transaction() as db:
            row = db.execute("SELECT * FROM outbound WHERE worker_id=? AND request_id=?", (worker_id, request_id)).fetchone()
            if row:
                if row["action"] != canonical(action):
                    raise OwnershipError("outbound request identifier changed payload")
                return json.loads(row["result"]) if row["result"] else {"success": False, "ambiguous": True, "error": "prior send requires reconciliation"}
            db.execute("INSERT INTO outbound VALUES(?,?,?,'started',NULL,?)", (worker_id, request_id, canonical(action), time.time()))
        return None

    def finish_outbound(self, worker_id: str, request_id: str, result: dict):
        with self.transaction() as db:
            db.execute("UPDATE outbound SET status='finished',result=? WHERE worker_id=? AND request_id=?", (canonical(result), worker_id, request_id))

    def remember_message(self, message_id: str, cid: str, generation: int, kind: str):
        with self.transaction() as db:
            db.execute("INSERT OR IGNORE INTO messages VALUES(?,?,?,?)", (message_id, cid, generation, kind))

    def owns_message(self, message_id: str, cid: str, *, bot_only: bool = False) -> bool:
        with self.lock:
            row = self.db.execute("SELECT kind FROM messages WHERE message_id=? AND conversation_id=?", (message_id, cid)).fetchone()
            return bool(row and (not bot_only or row["kind"] == "bot"))

    def create_prompt(self, prompt_id: str, row: dict, channel_id: str, options: list, timeout: int = 300):
        with self.transaction() as db:
            db.execute("INSERT INTO prompts VALUES(?,?,?,?,?,NULL,?,?,NULL)",
                       (prompt_id, row["id"], row["generation"], row["actor"], channel_id,
                        canonical(options), time.time() + min(max(timeout, 1), 3600)))

    def attach_prompt(self, prompt_id: str, message_id: str):
        with self.transaction() as db:
            db.execute("UPDATE prompts SET message_id=? WHERE id=?", (message_id, prompt_id))

    def pending_prompts(self):
        with self.lock:
            rows = self.db.execute("SELECT p.* FROM prompts p JOIN conversations c ON c.id=p.conversation_id WHERE p.response IS NULL AND p.expires>? AND p.generation=c.generation AND c.status NOT IN ('stopped','recovery_required')", (time.time(),))
            return [self._row(row) for row in rows]

    def prompt_owner(self, prompt_id: str):
        with self.lock:
            row = self.db.execute("SELECT c.* FROM conversations c JOIN prompts p ON p.conversation_id=c.id WHERE p.id=?", (prompt_id,)).fetchone()
            if not row:
                raise OwnershipError("unknown approval request")
            return self._row(row)

    def consume_prompt(self, prompt_id: str, *, actor: str, channel_id: str, message_id: str,
                       option: str, event_id: str | None = None):
        with self.transaction() as db:
            prompt = db.execute("SELECT * FROM prompts WHERE id=?", (prompt_id,)).fetchone()
            if not prompt or prompt["actor"] != actor or prompt["channel_id"] != channel_id or prompt["message_id"] != message_id:
                raise OwnershipError("this control belongs to another conversation or user")
            row = db.execute("SELECT * FROM conversations WHERE id=?", (prompt["conversation_id"],)).fetchone()
            if (row["generation"] != prompt["generation"] or row["status"] != "ready"
                    or prompt["expires"] < time.time() or prompt["response"] is not None):
                raise OwnershipError("this control is expired, answered or interrupted")
            if option not in {item["id"] for item in json.loads(prompt["options"])}:
                raise OwnershipError("control option was not offered")
            db.execute("UPDATE prompts SET response=? WHERE id=?", (option, prompt_id))
            if event_id:
                source = json.loads(row["source"])
                source["user_id"] = actor
                payload = {"text": f"/{option}", "message_type": "command", "source": source,
                           "message_id": event_id, "prompt_response": {"prompt_id": prompt_id,
                           "option_id": option, "prompt_message_id": message_id}}
                db.execute("INSERT INTO ingress VALUES(?,?,?,'queued',NULL,?,?)",
                           (event_id, row["id"], canonical(payload), time.time(), time.time()))
            return self._row(row)

    def prepare_recovery(self, cid: str, generation: int, note: str):
        """Caller must first verify the retained Pod's terminated container state."""
        if len(note.strip()) < 10:
            raise ValueError("record how uncertain actions were reconciled before recovery")
        with self.transaction() as db:
            row = db.execute("SELECT * FROM conversations WHERE id=? AND generation=?", (cid, generation)).fetchone()
            if not row or row["status"] not in ("stopped", "recovery_required", "unavailable"):
                raise OwnershipError("conversation is not eligible for deliberate recovery")
            db.execute("UPDATE ingress SET status='interrupted',updated=? WHERE conversation_id=? AND status='dispatched'", (time.time(), cid))
            db.execute("UPDATE prompts SET response='interrupted' WHERE conversation_id=? AND response IS NULL", (cid,))
            db.execute("UPDATE conversations SET generation=generation+1,status='queued',relay_secret=?,knowledge_token=?,detail=?,updated=? WHERE id=?",
                       (secrets.token_urlsafe(32), secrets.token_urlsafe(32), note, time.time(), cid))
            db.execute("INSERT INTO audit(at,conversation_id,event,detail) VALUES(?,?,?,?)",
                       (time.time(), cid, "recovery_authorized", canonical({"prior_generation": generation, "reconciliation": note})))
        return self.get(cid)

    def summary(self):
        with self.lock:
            queue = self.db.execute("SELECT count(*) FROM ingress WHERE status='queued'").fetchone()[0]
        return {"max_workers": self.max_workers, "max_queued": self.max_queued, "queued_messages": queue,
                "conversations": [{k: row[k] for k in ("id", "generation", "status", "identity", "detail")} for row in self.conversations()]}
