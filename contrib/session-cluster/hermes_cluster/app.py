"""Private companion API; only worker-authenticated and operator routes mutate state."""

from contextlib import asynccontextmanager
import hmac
import os

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from .knowledge import create_knowledge_router
from .ledger import OwnershipError
from .relay_server import create_relay_router


def create_app(controller):
    @asynccontextmanager
    async def lifespan(app):
        await controller.start()
        try:
            yield
        finally:
            await controller.close()

    app = FastAPI(title="Hermes session controller", docs_url=None, redoc_url=None,
                  openapi_url=None, lifespan=lifespan)
    app.include_router(create_relay_router(controller))
    app.include_router(controller.media.router())
    app.include_router(create_knowledge_router(controller.knowledge))

    async def operator(request: Request):
        expected = os.environ.get("HERMES_CLUSTER_OPERATOR_TOKEN", "")
        provided = request.headers.get("authorization", "")
        if len(expected) < 24 or not hmac.compare_digest(provided.encode(), f"Bearer {expected}".encode()):
            raise HTTPException(403, "operator authorization required")

    @app.get("/healthz")
    @app.get("/readyz")
    async def health():
        healthy = controller.healthy()
        return JSONResponse({"ready": healthy}, status_code=200 if healthy else 503)

    @app.get("/status", dependencies=[Depends(operator)])
    async def status():
        return controller.ledger.summary()

    @app.post("/conversations/{cid}/park", dependencies=[Depends(operator)])
    async def park(cid: str):
        try:
            await controller.park(cid)
        except (OwnershipError, ValueError) as exc:
            raise HTTPException(409, str(exc)) from exc
        return {"status": "draining"}

    @app.post("/conversations/{cid}/recover", dependencies=[Depends(operator)])
    async def recover(cid: str, request: Request):
        data = bytearray()
        async for chunk in request.stream():
            if len(data) + len(chunk) > 8192:
                raise HTTPException(413, "reconciliation note too large")
            data.extend(chunk)
        import json
        try:
            body = json.loads(data)
            await controller.recover(cid, body.get("reconciliation_note", ""))
        except (OwnershipError, ValueError, TypeError) as exc:
            raise HTTPException(409, str(exc)) from exc
        return {"status": "queued"}

    @app.post("/conversations/{cid}/recover-attested", dependencies=[Depends(operator)])
    async def recover_attested(cid: str, request: Request):
        from .kubernetes import KubernetesError
        import json
        data = bytearray()
        async for chunk in request.stream():
            if len(data) + len(chunk) > 32768:
                raise HTTPException(413, "operator evidence too large")
            data.extend(chunk)
        try:
            await controller.recover_attested(cid, json.loads(data))
        except (OwnershipError, ValueError, TypeError, KubernetesError) as exc:
            raise HTTPException(409, str(exc)) from exc
        return {"status": "queued"}

    return app
