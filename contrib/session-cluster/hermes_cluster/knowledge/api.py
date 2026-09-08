"""Worker-only HTTP surface. Controller grant/revoke APIs are deliberately absent."""
from fastapi import APIRouter, Header, HTTPException
import sqlite3
from .store import KnowledgeError
from .limits import BoundedKnowledgeRoute


def create_knowledge_router(store):
    router = APIRouter(prefix="/v1", route_class=BoundedKnowledgeRoute)

    def call(authorization, method, *args):
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(401, "worker credential required")
        try:
            return method(authorization[7:], *args)
        except KnowledgeError as exc:
            raise HTTPException(exc.status, str(exc)) from exc
        except sqlite3.DatabaseError as exc:
            raise HTTPException(503, "knowledge storage unavailable or full; operator action required") from exc

    @router.post("/snapshots")
    def snapshot(body: dict, authorization: str | None = Header(default=None)):
        return call(authorization, store.snapshot, body.get("snapshot_id"))

    @router.post("/memory/mutations")
    def memory(body: dict, authorization: str | None = Header(default=None)):
        return call(authorization, store.mutate, "memory", body)

    @router.post("/skills/mutations")
    def skills(body: dict, authorization: str | None = Header(default=None)):
        return call(authorization, store.mutate, "skill", body)

    @router.get("/receipts/{mutation_id}")
    def receipt(mutation_id: str, authorization: str | None = Header(default=None)):
        return call(authorization, store.receipt, mutation_id)

    @router.post("/history/batches")
    def ingest(body: dict, authorization: str | None = Header(default=None)):
        return call(authorization, store.ingest_history, body)

    @router.post("/history/search")
    def search(body: dict, authorization: str | None = Header(default=None)):
        return call(authorization, store.search_history, body)

    return router
