"""Audience-filtered durable recall; counts and snippets are computed after authorization."""
import json


class HistoryStore:
    def ingest_history(self, token, request):
        from .store import KnowledgeError, encode
        records = request.get("records")
        if not isinstance(records, list) or not 1 <= len(records) <= 500:
            raise KnowledgeError("history batch requires 1 to 500 records")
        with self.transaction() as db:
            grant = self._authorize(db, token)
            receipt = self._receipt(db, grant, request)
            if receipt is not None:
                return receipt
            self._require_write_capacity(db)
            for record in records:
                if not isinstance(record, dict):
                    raise KnowledgeError("history records must be objects")
                session, message = record.get("session_id"), record.get("message_id")
                revision = record.get("revision")
                if not all(isinstance(x, str) and 0 < len(x) <= 200 for x in (session, message)):
                    raise KnowledgeError("history requires bounded session_id and message_id")
                if not isinstance(revision, int) or revision < 1:
                    raise KnowledgeError("history revision must be positive")
                payload = record.get("payload")
                if not isinstance(payload, dict) or len(encode(payload)) > 1_000_000:
                    raise KnowledgeError("invalid history payload")
                old = db.execute("SELECT revision,payload FROM history WHERE agent=? AND conversation=? AND session=? AND message=?",
                    (grant["agent"], grant["conversation"], session, message)).fetchone()
                if old and (revision < old[0] or (revision == old[0] and encode(payload) != old[1])):
                    raise KnowledgeError("history revision conflict", 409)
                db.execute("INSERT INTO history VALUES(?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(agent,conversation,session,message) DO UPDATE SET "
                    "revision=excluded.revision,payload=excluded.payload,provenance=excluded.provenance",
                    (grant["agent"], grant["audience"], grant["conversation"], session, message, revision,
                     encode(payload), encode(self._provenance(grant, request.get("origin", {})))))
            return self._save_receipt(db, grant, request, {
                "success": True, "mutation_id": request["mutation_id"], "acknowledged": len(records)})

    def search_history(self, token, request):
        from .recall import search_history
        return search_history(self, token, request)
