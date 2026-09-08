"""Bounded private media, authorized by conversation rather than URL secrecy."""

from __future__ import annotations

import asyncio
import hashlib
import os
from pathlib import Path
import tempfile

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse

from .ledger import Ledger, OwnershipError

MAX_MEDIA = 25 * 1024 * 1024


class MediaStore:
    def __init__(self, root: Path, ledger: Ledger, *, per_conversation_bytes=128 * 1024 * 1024,
                 total_bytes=1024 * 1024 * 1024):
        self.root, self.ledger = Path(root), ledger
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.per_conversation_bytes, self.total_bytes = per_conversation_bytes, total_bytes
        self.upload_slots = asyncio.Semaphore(2)
        with ledger.transaction() as db:
            db.execute("CREATE TABLE IF NOT EXISTS media(id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL, filename TEXT NOT NULL, mime TEXT NOT NULL, size INTEGER NOT NULL)")
            retained = {row[0] for row in db.execute("SELECT id FROM media")}
        # Called while the controller holds its exclusive process lock. Recover
        # files left between rename and metadata commit without erasing receipts.
        for path in self.root.iterdir():
            if path.is_file() and path.name not in retained:
                path.unlink()

    def put(self, cid: str, data: bytes, filename: str = "attachment", mime: str = "application/octet-stream", *, authorization: str | None = None):
        if not data or len(data) > MAX_MEDIA:
            raise ValueError("media must contain 1 to 25 MiB of data")
        filename = Path(filename).name[:150]
        filename = "".join(c for c in filename if c.isprintable() and c not in '/\\') or "attachment"
        mime = mime.split(";", 1)[0].strip()
        if any(c in mime for c in "\r\n") or len(mime) > 100:
            mime = "application/octet-stream"
        ident = hashlib.sha256(cid.encode() + b"\x00" + data).hexdigest()
        fd, temporary = tempfile.mkstemp(dir=self.root, prefix=".upload-")
        try:
            # Slow file writes must not hold the controller's SQLite lock.
            with os.fdopen(fd, "wb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            with self.ledger.lock:
                installed = False
                try:
                    if authorization is not None and self.ledger.authenticate(authorization)["id"] != cid:
                        raise OwnershipError("media destination differs from worker authority")
                    with self.ledger.transaction() as db:
                        if db.execute("SELECT 1 FROM media WHERE id=?", (ident,)).fetchone():
                            return ident
                        own = db.execute("SELECT COALESCE(sum(size),0) FROM media WHERE conversation_id=?", (cid,)).fetchone()[0]
                        total = db.execute("SELECT COALESCE(sum(size),0) FROM media").fetchone()[0]
                        if own + len(data) > self.per_conversation_bytes or total + len(data) > self.total_bytes:
                            raise ValueError("retained media quota reached; operator cleanup is required")
                        os.replace(temporary, self.root / ident)
                        installed = True
                        db.execute("INSERT INTO media VALUES(?,?,?,?,?)", (ident, cid, filename, mime, len(data)))
                except BaseException:
                    # Retain the lock through rollback cleanup so a concurrent
                    # identical upload cannot commit the file before removal.
                    if installed:
                        (self.root / ident).unlink(missing_ok=True)
                    raise
        finally:
            Path(temporary).unlink(missing_ok=True)
        return ident

    def get(self, ident: str, cid: str):
        with self.ledger.lock:
            row = self.ledger.db.execute("SELECT * FROM media WHERE id=? AND conversation_id=?", (ident, cid)).fetchone()
        if row is None:
            raise OwnershipError("media is not available to this conversation")
        path = self.root / row["id"]
        if not path.is_file() or path.is_symlink():
            raise FileNotFoundError("retained media is missing")
        return path, dict(row)

    def router(self):
        router = APIRouter()

        def authorize(request):
            try:
                return self.ledger.authenticate(request.headers.get("authorization", ""))
            except OwnershipError as exc:
                raise HTTPException(403, "worker authorization failed") from exc

        @router.post("/relay/media")
        async def upload(request: Request):
            owner = authorize(request)
            async with self.upload_slots:
                data = bytearray()
                async with asyncio.timeout(60):
                    async for chunk in request.stream():
                        if len(data) + len(chunk) > MAX_MEDIA:
                            raise HTTPException(413, "media exceeds 25 MiB")
                        data.extend(chunk)
                current = authorize(request)
                if current["generation"] != owner["generation"]:
                    raise HTTPException(403, "worker generation changed during upload")
                try:
                    ident = await asyncio.to_thread(self.put, owner["id"], bytes(data), request.headers.get("x-media-filename", "attachment"),
                                                    request.headers.get("content-type", "application/octet-stream"),
                                                    authorization=request.headers.get("authorization", ""))
                except OwnershipError as exc:
                    raise HTTPException(403, "worker authorization changed during media storage") from exc
                except ValueError as exc:
                    raise HTTPException(413, str(exc)) from exc
                return {"id": ident, "size": len(data)}

        @router.get("/relay/media/{ident}")
        async def download(ident: str, request: Request):
            owner = authorize(request)
            try:
                path, metadata = self.get(ident, owner["id"])
            except (OwnershipError, FileNotFoundError) as exc:
                raise HTTPException(404, "media unavailable") from exc
            return FileResponse(path, media_type=metadata["mime"], filename=metadata["filename"],
                                headers={"Cache-Control": "no-store"})

        return router
