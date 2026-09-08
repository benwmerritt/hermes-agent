"""Synchronous authority client with a durable, ordered local request outbox."""
import json
from contextlib import contextmanager
from pathlib import Path
import sqlite3
import threading
import httpx
from .store import KnowledgeError, digest, encode


class KnowledgeClient:
    def __init__(self, url, token, home):
        self.http = httpx.Client(base_url=url.rstrip("/"),
                                 headers={"Authorization": f"Bearer {token}"}, timeout=20)
        self.path = Path(home) / "knowledge-outbox.db"
        self.lock = threading.RLock()
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS outbox (
                    id TEXT UNIQUE NOT NULL, route TEXT NOT NULL, request TEXT NOT NULL,
                    result TEXT, created INTEGER PRIMARY KEY AUTOINCREMENT);
            """)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=15)
        try:
            page_size = db.execute("PRAGMA page_size").fetchone()[0]
            db.execute(f"PRAGMA max_page_count={128 * 1024 * 1024 // page_size}")
            with db:
                yield db
        finally:
            db.close()

    def post(self, route, body):
        response = self.http.post(route, json=body)
        if response.is_error:
            try:
                detail = response.json().get("detail", "knowledge request failed")
            except ValueError:
                detail = "knowledge request failed"
            raise KnowledgeError(str(detail), response.status_code)
        return response.json()

    def mutate(self, route, body):
        from agent.knowledge_backend import operation_id, operation_fingerprint
        request = dict(body)
        request.setdefault("mutation_id", operation_id.get() or digest([route, body]))
        if operation_fingerprint.get() is not None:
            request["origin"] = {**request.get("origin", {}), "approval_payload_hash": operation_fingerprint.get()}
        with self.lock:
            with self.connect() as db:
                old = db.execute("SELECT request,result FROM outbox WHERE id=?", (request["mutation_id"],)).fetchone()
                if old and old[0] != encode(request):
                    raise KnowledgeError("local idempotency conflict", 409)
                if old and old[1]:
                    # Recheck authorization even for a local replay receipt.
                    self.post("/v1/snapshots", {"snapshot_id": self.snapshot_id})
                    result = json.loads(old[1])
                    if result.get("rejected"):
                        raise KnowledgeError(result["error"], result["status"])
                    return result
                db.execute("INSERT OR IGNORE INTO outbox(id,route,request) VALUES(?,?,?)",
                           (request["mutation_id"], route, encode(request)))
            try:
                result = self.post(route, request)
            except KnowledgeError as exc:
                if exc.status in {400, 404, 409, 422}:
                    with self.connect() as db:
                        db.execute("UPDATE outbox SET result=? WHERE id=?", (
                            encode({"rejected": True, "error": str(exc), "status": exc.status}), request["mutation_id"]))
                raise
            with self.connect() as db:
                db.execute("UPDATE outbox SET result=? WHERE id=?", (encode(result), request["mutation_id"]))
                if route == "/v1/history/batches":
                    db.execute("DELETE FROM outbox WHERE id=?", (request["mutation_id"],))
            return result

    def recover(self):
        """Replay unacknowledged requests in order, retaining failures for operator resolution."""
        results = []
        with self.lock:
            while True:
                with self.connect() as db:
                    row = db.execute("SELECT route,request FROM outbox WHERE result IS NULL ORDER BY created LIMIT 1").fetchone()
                if row is None:
                    return results
                results.append(self.mutate(row[0], json.loads(row[1])))

    def accepted_resources(self):
        with self.connect() as db:
            rows = db.execute("SELECT result FROM outbox WHERE result IS NOT NULL ORDER BY created").fetchall()
        return [resource for (result,) in rows for resource in json.loads(result).get("resources", [])]

    def close(self):
        self.http.close()
