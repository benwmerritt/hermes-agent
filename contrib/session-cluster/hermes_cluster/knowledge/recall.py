"""Bounded native-shaped recall with audience predicates applied inside every query."""
import json
from .store import KnowledgeError

_ORDER = "CAST(COALESCE(json_extract(payload,'$.timestamp'),0) AS REAL),CAST(message AS INTEGER),message"


def _shape(row):
    payload = json.loads(row["payload"])
    keys = ("role", "timestamp", "tool_name", "active", "compacted", "_compressed_summary", "session_meta")
    return {**{k: payload[k] for k in keys if k in payload},
            "content": str(payload.get("content") or "")[:4000],
            "id": row["message"], "message_id": row["message"], "revision": row["revision"],
            "provenance": json.loads(row["provenance"])}


def _read(db, where, args, session_id, around, window):
    where += " AND session=?"
    args = [*args, session_id]
    conversations = db.execute("SELECT DISTINCT conversation FROM history WHERE " + where, args).fetchmany(2)
    if not conversations:
        raise KnowledgeError("session not found", 404)
    if len(conversations) > 1:
        raise KnowledgeError("session id is ambiguous across conversations", 409)
    total = db.execute("SELECT count(*) FROM history WHERE " + where, args).fetchone()[0]
    base = "SELECT * FROM history WHERE " + where + " ORDER BY " + _ORDER
    extra = {}
    if around is not None:
        position = db.execute("WITH ordered AS (SELECT message,row_number() OVER (ORDER BY " + _ORDER +
            ") AS position FROM history WHERE " + where + ") SELECT position FROM ordered WHERE message=?",
            [*args, str(around)]).fetchone()
        if position is None:
            raise KnowledgeError("message not found", 404)
        offset = max(0, position[0] - 1 - window)
        count = min(total-offset, position[0] + window-offset)
        rows = db.execute(base + " LIMIT ? OFFSET ?", [*args, count, offset]).fetchall()
        extra = {"messages_before": offset, "messages_after": total-offset-count}
    else:
        rows = db.execute(base + " LIMIT 20", args).fetchall()
        if total > 20:
            offset = max(20, total-10)
            rows += db.execute(base + " LIMIT 10 OFFSET ?", [*args, offset]).fetchall()
    messages = [_shape(row) for row in rows]
    return {"success": True, "mode": "scroll" if around is not None else "read", "session_id": session_id,
            "session_meta": messages[0].get("session_meta", {}) if messages else {},
            "messages": messages, "message_count": total, "total_messages": total,
            "truncated": len(messages) < total,
            "hint": "Use a message id as around_message_id to read adjacent history.", **extra}


def _discover(db, where, args, request, limit):
    query = str(request.get("query") or "").lower()
    filtered, params = where, list(args)
    current = request.get("current_session_id")
    if current:
        filtered += " AND session!=?"
        params.append(current)
    roles = request.get("role_filter")
    if roles:
        roles = roles if isinstance(roles, list) else [roles]
        if len(roles) > 10 or any(not isinstance(r, str) for r in roles):
            raise KnowledgeError("invalid role_filter")
        filtered += " AND json_extract(payload,'$.role') IN (" + ",".join("?" for _ in roles) + ")"
        params.extend(roles)
    if query:
        filtered += " AND (instr(lower(COALESCE(json_extract(payload,'$.content'),'')),?)>0 OR " \
                    "instr(lower(COALESCE(json_extract(payload,'$.session_meta.title'),'')),?)>0)"
        params += [query, query]
    grouped = "SELECT session,conversation,max(COALESCE(json_extract(payload,'$.timestamp'),0)) AS last_activity " \
              "FROM history WHERE " + filtered + " GROUP BY session,conversation"
    total = db.execute("SELECT count(*) FROM (" + grouped + ")", params).fetchone()[0]
    selected = db.execute(grouped + " ORDER BY last_activity DESC LIMIT ?", [*params, limit]).fetchall()
    sessions, results = [], []
    for item in selected:
        scoped = " AND session=? AND conversation=?"
        identity = [item["session"], item["conversation"]]
        rows = db.execute("SELECT * FROM history WHERE " + filtered + scoped + " ORDER BY " + _ORDER + " LIMIT 5",
                          [*params, *identity]).fetchall()
        matches = [_shape(row) for row in rows]
        count = db.execute("SELECT count(*) FROM history WHERE " + where + scoped, [*args, *identity]).fetchone()[0]
        first = matches[0]
        meta = first.get("session_meta", {})
        sessions.append({"session_id": item["session"], "message_count": count, "matches": matches,
                         "last_activity": item["last_activity"]})
        results.append({"session_id": item["session"], "message_count": count, "title": meta.get("title"),
                        "source": meta.get("source"), "when": meta.get("started_at"),
                        "match_message_id": first["id"], "matched_role": first.get("role"),
                        "snippet": first["content"][:2000]})
    response = {"success": True, "mode": "discover" if query else "browse", "results": results,
                "sessions": sessions, "count": len(results), "total_sessions": total}
    if query and not results:
        response["hint"] = ("No matching authorized sessions. Search uses a literal substring; "
            "quotes and Boolean operators are literal characters, not search syntax. "
            "Try one distinctive word or an unquoted contiguous phrase, or omit query to browse.")
    return response


def search_history(store, token, request):
    if request.get("profile"):
        raise KnowledgeError("profile switching is unavailable in audience-scoped recall", 403)
    limit, window = request.get("limit", 3), request.get("window", 5)
    if not isinstance(limit, int) or not 1 <= limit <= 100:
        raise KnowledgeError("limit must be between 1 and 100")
    if not isinstance(window, int) or not 0 <= window <= 20:
        raise KnowledgeError("window must be between 0 and 20")
    with store.transaction() as db:
        grant = store._authorize(db, token)
        audiences = json.loads(grant["reads"])
        where = "agent=? AND audience IN (" + ",".join("?" for _ in audiences) + ") " \
                "AND COALESCE(json_extract(payload,'$.deleted'),0)=0"
        args = [grant["agent"], *audiences]
        if request.get("session_id"):
            return _read(db, where, args, request["session_id"], request.get("around_message_id"), window)
        return _discover(db, where, args, request, limit)
