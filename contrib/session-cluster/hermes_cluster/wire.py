"""Native Discord events projected onto the existing Relay wire contract."""

from __future__ import annotations

import hashlib
from pathlib import Path

from gateway.session import build_session_key

from .ledger import canonical


def routing_key(agent_id: str, event):
    source = event.source.to_dict()
    native_key = build_session_key(event.source)
    cid = hashlib.sha256(canonical([agent_id, source.get("scope_id"), native_key]).encode()).hexdigest()[:24]
    return cid, native_key, source


def event_identifier(event) -> str:
    # Native slash normalization omits message_id; the authentic interaction ID
    # is still stable across a redelivery. Never invent a random dedupe key.
    ident = event.message_id or getattr(event.raw_message, "id", None)
    if not ident:
        raise ValueError("Discord event has no durable event identity")
    return f"discord:{event.source.scope_id}:{event.source.chat_id}:{ident}"


def to_wire(event, cid: str, media, base_url: str):
    urls, attachments = [], []
    for i, raw_path in enumerate(event.media_urls):
        path = Path(raw_path)
        # Native adapter supplies downloaded files. Do not forward controller
        # paths or accept arbitrary URLs as an authenticated media reference.
        if not path.is_file() or path.is_symlink():
            raise ValueError("native attachment is not a regular local file")
        if path.stat().st_size > 25 * 1024 * 1024:
            raise ValueError("attachment exceeds Relay media limit")
        mime = event.media_types[i] if i < len(event.media_types) else "application/octet-stream"
        ident = media.put(cid, path.read_bytes(), path.name, mime)
        url = f"{base_url.rstrip('/')}/relay/media/{ident}"
        urls.append(url)
        attachments.append({"url": url, "mime": mime, "filename": path.name})
    return {"text": event.text, "message_type": event.message_type.value,
            "source": event.source.to_dict(), "message_id": str(event.message_id or getattr(event.raw_message, "id", "")),
            "reply_to_message_id": event.reply_to_message_id,
            "reply_to": {"text": event.reply_to_text, "author": event.reply_to_author_name,
                         "is_own": event.reply_to_is_own_message},
            "media_urls": urls, "media": attachments}
