"""Stream native rows into byte-bounded durable batches, including later revisions."""
import json
import sqlite3
from .limits import MAX_HISTORY_BATCH_BYTES
from .store import KnowledgeError, digest, encode


def _enqueue(local, records):
    if records:
        request = {"records": records}
        request["mutation_id"] = digest(["history", request])
        local.execute("INSERT OR IGNORE INTO outbox(id,route,request) VALUES(?,?,?)",
                      (request["mutation_id"], "/v1/history/batches", encode(request)))


def _export_rows(source_db, local):
    local.execute("CREATE TABLE IF NOT EXISTS exports (key TEXT PRIMARY KEY, hash TEXT NOT NULL, revision INTEGER NOT NULL)")
    local.execute("CREATE TEMP TABLE seen_exports (key TEXT PRIMARY KEY)")
    records, batch_bytes = [], 0

    def add(record):
        nonlocal records, batch_bytes
        size = len(encode(record).encode())
        if size > 1_000_000:
            raise KnowledgeError("native history record exceeds 1 MB; export stopped without advancing cursor", 413)
        if batch_bytes + size > MAX_HISTORY_BATCH_BYTES or len(records) >= 500:
            _enqueue(local, records)
            records, batch_bytes = [], 0
        records.append(record)
        batch_bytes += size

    rows = source_db.execute("SELECT m.id,m.session_id,m.role,m.content,m.tool_call_id,m.tool_calls,m.tool_name,"
        "m.timestamp,m.active,m.compacted,m._compressed_summary,s.title,s.source,s.model,s.started_at,"
        "s.ended_at,s.parent_session_id,s.end_reason FROM messages m JOIN sessions s ON s.id=m.session_id ORDER BY m.id")
    meta_keys = ("title", "source", "model", "started_at", "ended_at", "parent_session_id", "end_reason")
    for row in rows:
        payload = dict(row)
        session, message = str(payload.pop("session_id")), str(payload.pop("id"))
        payload["session_meta"] = {key: payload.pop(key) for key in meta_keys}
        key = encode([session, message])
        local.execute("INSERT INTO seen_exports VALUES(?)", (key,))
        fingerprint = digest(payload)
        old = local.execute("SELECT hash,revision FROM exports WHERE key=?", (key,)).fetchone()
        if old and old[0] == fingerprint:
            continue
        revision = old[1] + 1 if old else 1
        add({"session_id": session, "message_id": message, "revision": revision, "payload": payload})
        local.execute("INSERT OR REPLACE INTO exports VALUES(?,?,?)", (key, fingerprint, revision))
    removed = local.execute("SELECT key,revision FROM exports WHERE hash!='deleted' "
                            "AND key NOT IN (SELECT key FROM seen_exports)")
    for key, revision in removed:
        session, message = json.loads(key)
        add({"session_id": session, "message_id": message, "revision": revision + 1, "payload": {"deleted": True}})
        local.execute("UPDATE exports SET hash='deleted',revision=? WHERE key=?", (revision + 1, key))
    _enqueue(local, records)


def flush_history(client, source):
    if not source.exists():
        return {"acknowledged": 0}
    with client.lock:
        db = sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True)
        db.row_factory = sqlite3.Row
        try:
            db.execute("BEGIN")
            # Export cursor and pending requests commit together, before any network send.
            with client.connect() as local:
                _export_rows(db, local)
        finally:
            db.close()
        receipts = client.recover()
    return {"acknowledged": sum(r.get("acknowledged", 0) for r in receipts)}
