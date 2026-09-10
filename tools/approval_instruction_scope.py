"""In-memory consent for exact instruction paths in a linked review worktree.

This is not a sandbox against arbitrary same-user code. The file-tool guard
alone consumes these grants; normal approval patterns and child runs do not.
"""
import os
from pathlib import Path
import threading
import time

from tools import approval_context
from tools.file_tools_paths import (
    _authoritative_workspace_root, _expand_tilde, _terminal_env_type_for_task,
)

CHOICE = "instruction_15m"
SECONDS = 15 * 60
_lock = threading.Lock()
_sessions: dict[str, dict] = {}


def clear_session(session_key: str) -> None:
    with _lock:
        _sessions.pop(session_key, None)


def candidate(paths: list[str], task_id: str):
    """Fail closed on missing identity, remote paths, symlinks or live state.

    Validate Git's reciprocal linked-worktree records, not just a `.git`
    filename. Directory identities invalidate grants if a worktree is replaced.
    """
    session_key = approval_context.get_current_session_key(default="")
    session_id = approval_context._approval_session_id.get()
    if not session_key or not session_id or not task_id or task_id == "default":
        return None
    if _terminal_env_type_for_task(task_id) != "local":
        return None
    try:
        from hermes_constants import get_hermes_home
        anchor = _authoritative_workspace_root(task_id)
        if not anchor:
            return None
        root = Path(anchor).resolve(strict=True)
        while not (root / ".git").exists():
            if root.parent == root:
                return None
            root = root.parent
        marker = root / ".git"
        if marker.is_symlink() or not marker.is_file():
            return None
        pointer = marker.read_text(encoding="utf-8").strip()
        if not pointer.startswith("gitdir: "):
            return None
        gitdir = (root / pointer[8:]).resolve(strict=True)
        common = (gitdir / (gitdir / "commondir").read_text().strip()).resolve(strict=True)
        if gitdir == common or not (common / "objects").is_dir():
            return None
        if Path((gitdir / "gitdir").read_text().strip()).resolve(strict=True) != marker:
            return None
        home = get_hermes_home().resolve()
        if root.is_relative_to(home):
            return None
        resolved = []
        for raw in paths:
            path = Path(_expand_tilde(raw))
            if not path.is_absolute():
                path = Path(anchor) / path
            # Do not normalize away a symlink followed by '..'.
            if any(p.is_symlink() for p in (path, *path.parents)):
                return None
            path = path.resolve()
            if path == root or not path.is_relative_to(root) or path.is_relative_to(home):
                return None
            parent = path.parent
            while parent != root:
                if (parent / ".git").exists():
                    return None
                parent = parent.parent
            resolved.append(str(path))
        identity = tuple((str(p), p.stat().st_dev, p.stat().st_ino) for p in (root, gitdir, common))
    except (OSError, ValueError, RuntimeError):
        return None
    with _lock:
        bucket = _sessions.setdefault(session_key, {})
    return (session_key, session_id, task_id, os.getpid(), identity, tuple(sorted(set(resolved))), bucket)


def is_approved(scope) -> bool:
    if scope is None:
        return False
    key, session, task, pid, tree, paths, bucket = scope
    now = time.monotonic()
    with _lock:
        if _sessions.get(key) is not bucket:
            return False
        for target, expiry in list(bucket.items()):
            if expiry <= now:
                del bucket[target]
        return all(bucket.get((session, task, pid, tree, path), 0) > now for path in paths)


def grant(scope) -> bool:
    key, session, task, pid, tree, paths, bucket = scope
    with _lock:
        # A /new or cleanup while the user was answering must not resurrect consent.
        if _sessions.get(key) is not bucket:
            return False
        expiry = time.monotonic() + SECONDS
        for path in paths:
            bucket[(session, task, pid, tree, path)] = expiry
    return True


def request_data(scope) -> dict:
    return {"paths": list(scope[5]), "seconds": SECONDS, "worktree": scope[4][0][0]}


def format_scope_option(data: dict, command_prefix: str) -> str:
    """A request-specific explicit command; never reinterpret generic Session."""
    import json
    scope = data["instruction_scope"]
    paths = "\n".join(json.dumps(p, ensure_ascii=True) for p in scope["paths"])
    return ("Allow repeated edits to exactly these files for 15 minutes in this session only:\n"
            f"{paths}\nWorktree: {json.dumps(scope['worktree'], ensure_ascii=True)}\n"
            f"Reply `{command_prefix}approve instruction-15m {data['request_id']}`. "
            "No other files or child sessions are included.")
