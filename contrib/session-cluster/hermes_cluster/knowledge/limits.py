"""Finite storage and HTTP budgets for the small companion pilot."""
from fastapi import HTTPException, Request
from fastapi.routing import APIRoute

MAX_REQUEST_BYTES = 4 * 1024 * 1024
MAX_SNAPSHOT_BYTES = 8 * 1024 * 1024
MAX_HISTORY_BATCH_BYTES = 2 * 1024 * 1024


class BoundedKnowledgeRoute(APIRoute):
    def get_route_handler(self):
        downstream = super().get_route_handler()

        async def bounded(request: Request):
            length = request.headers.get("content-length")
            if length is not None:
                try:
                    size = int(length)
                except ValueError as exc:
                    raise HTTPException(400, "invalid Content-Length") from exc
                if size < 0 or size > MAX_REQUEST_BYTES:
                    raise HTTPException(413, "knowledge request exceeds 4 MiB")
            # Check actual streamed bytes too: chunked requests need no Content-Length.
            body = bytearray()
            async for chunk in request.stream():
                if len(body) + len(chunk) > MAX_REQUEST_BYTES:
                    raise HTTPException(413, "knowledge request exceeds 4 MiB")
                body.extend(chunk)
            request._body = bytes(body)
            return await downstream(request)

        return bounded
