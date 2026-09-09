import concurrent.futures
import json
from pathlib import Path
import socket
import threading
import time

from fastapi import FastAPI
import httpx
import pytest
import uvicorn

from hermes_cluster.knowledge import KnowledgeStore, KnowledgeError, create_knowledge_router, bootstrap_knowledge


@pytest.fixture
def authority(tmp_path):
    store = KnowledgeStore(tmp_path / "authority.db")
    app = FastAPI()
    app.include_router(create_knowledge_router(store))
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    server = uvicorn.Server(uvicorn.Config(app, log_level="error", lifespan="off"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started and time.monotonic() < deadline:
        time.sleep(.01)
    assert server.started
    yield store, f"http://127.0.0.1:{sock.getsockname()[1]}"
    server.should_exit = True
    thread.join(timeout=10)
    sock.close()
    assert not thread.is_alive()


def grant(store, conversation, audience="public", generation=1):
    token = f"opaque-worker-{conversation}-{generation}-credential"
    store.register_worker(token=token, agent_id="wallace", conversation_key=conversation,
                          generation=generation, audience=audience)
    return token


def mutation(key, content, revision=0, identifier=None):
    return {"mutation_id": identifier or key, "operations": [{"key": key,
            "expected_revision": revision, "value": {"target": "memory", "content": content}}]}


def test_concurrent_adds_cas_atomicity_receipts_and_restart(tmp_path):
    store = KnowledgeStore(tmp_path / "authority.db")
    a, b = grant(store, "a"), grant(store, "b")
    with concurrent.futures.ThreadPoolExecutor() as pool:
        results = list(pool.map(lambda pair: store.mutate(pair[0], "memory", mutation(pair[1], pair[1])),
                                [(a, "alpha"), (b, "beta")]))
    assert all(r["success"] for r in results)
    store = KnowledgeStore(tmp_path / "authority.db")
    assert {r["key"] for r in store.snapshot(b)["resources"]} == {"alpha", "beta"}
    first = store.mutate(a, "memory", mutation("alpha", "updated", 1, "edit"))
    assert store.mutate(a, "memory", mutation("alpha", "updated", 1, "edit")) == first
    with pytest.raises(KnowledgeError, match="reused"):
        store.mutate(a, "memory", mutation("alpha", "different", 1, "edit"))
    request = mutation("gamma", "new")
    request["operations"].append(mutation("alpha", "stale", 1)["operations"][0])
    with pytest.raises(KnowledgeError, match="CAS conflict"):
        store.mutate(b, "memory", request)
    assert "gamma" not in {r["key"] for r in store.snapshot(b)["resources"]}


def test_acl_snapshots_receipts_history_counts_and_generation(authority):
    store, url = authority
    a, b, private = grant(store, "a"), grant(store, "b"), grant(store, "private", "dm:ben")
    store.mutate(private, "memory", mutation("secret", "private marker"))
    store.mutate(private, "skill", {"mutation_id": "private-skill", "operations": [{"key": "private-check",
        "expected_revision": 0, "value": {"files": {"SKILL.md": "Private procedure"}}}]})
    snap = store.snapshot(private)
    history = {"mutation_id": "history", "records": [{"session_id": "private-session", "message_id": "1",
                "revision": 1, "payload": {"role": "user", "content": "private marker", "timestamp": 1}}]}
    store.ingest_history(private, history)
    headers = {"Authorization": f"Bearer {b}"}
    with httpx.Client(base_url=url, headers=headers) as client:
        assert client.post("/v1/snapshots", json={"audience": "dm:ben"}).json()["resources"] == []
        assert client.post("/v1/snapshots", json={"snapshot_id": snap["snapshot_id"]}).status_code == 404
        assert client.get("/v1/receipts/secret").status_code == 404
        assert client.post("/v1/history/search", json={"query": "private"}).json()["total_sessions"] == 0
        assert client.post("/v1/history/search", json={"session_id": "private-session"}).status_code == 404
        assert client.post("/v1/history/search", json={"profile": "private"}).status_code == 403
        assert client.post("/v1/memory/mutations", json={**mutation("public", "visible"), "audience": "dm:ben"}).status_code == 200
    pinned = store.snapshot(a)
    store.mutate(b, "memory", mutation("later", "later update"))
    assert store.snapshot(a, pinned["snapshot_id"]) == pinned
    grant(store, "a", generation=2)
    for operation in (lambda: store.snapshot(a), lambda: store.mutate(a, "memory", mutation("stale", "stale")),
                      lambda: store.search_history(a, {}), lambda: store.receipt(a, "public")):
        with pytest.raises(KnowledgeError) as error:
            operation()
        assert error.value.status == 403


def test_skill_bundle_validation_and_two_writer_conflict(tmp_path):
    store = KnowledgeStore(tmp_path / "authority.db")
    a, b = grant(store, "a"), grant(store, "b")
    skill = {"mutation_id": "skill", "operations": [{"key": "launch", "expected_revision": 0,
             "value": {"files": {"SKILL.md": "---\nname: launch\n---\nTest", "scripts/check.py": "print('ok')"}}}]}
    store.mutate(a, "skill", skill)
    with pytest.raises(KnowledgeError, match="CAS conflict"):
        store.mutate(b, "skill", skill)
    skill["mutation_id"] = "bad"
    skill["operations"][0]["value"]["files"]["../escape"] = "bad"
    with pytest.raises(KnowledgeError, match="relative"):
        store.mutate(a, "skill", skill)
    assert len(store.snapshot(b)["resources"]) == 1


def make_runtime(authority, tmp_path, monkeypatch, name, approval=False, memory_enabled=True):
    store, url = authority
    home = tmp_path / name
    home.mkdir()
    home.joinpath("config.yaml").write_text(f"memory:\n  write_approval: {str(approval).lower()}\n  memory_enabled: {str(memory_enabled).lower()}\nskills:\n  write_approval: {str(approval).lower()}\n")
    monkeypatch.setenv("HERMES_HOME", str(home))
    return bootstrap_knowledge(url=url, token=grant(store, name), hermes_home=home)


def test_native_memory_and_skills_fresh_worker_prompt_pin(authority, tmp_path, monkeypatch):
    from tools.memory_tool import create_memory_store, memory_tool, load_on_disk_store
    from tools.skill_manager_tool import skill_manage
    from tools.skills_tool import skill_view
    from agent.prompt_builder import build_skills_system_prompt
    from model_tools import handle_function_call
    a = make_runtime(authority, tmp_path, monkeypatch, "a")
    memory = create_memory_store()
    memory.load_from_disk()
    original_prompt = memory.format_for_system_prompt("memory")
    original_skills = build_skills_system_prompt()
    assert json.loads(memory_tool("add", content="The launch checklist requires the blue flag.", store=memory))["success"]
    skill = "---\nname: launch-check\ndescription: Use when checking a launch.\n---\n# Launch check\nRead references/flag.md and require the blue flag.\n"
    response = json.loads(handle_function_call("skill_manage", {"operations": [
        {"action": "create", "name": "launch-check", "content": skill},
        {"action": "write_file", "name": "launch-check", "file_path": "references/flag.md", "file_content": "The blue flag must be present."}]},
        task_id="knowledge-test", enabled_toolsets=["skills"]))
    assert response["success"], response
    assert memory.format_for_system_prompt("memory") == original_prompt
    assert build_skills_system_prompt() == original_skills
    assert load_on_disk_store().format_for_system_prompt("memory") == original_prompt
    a.close()
    b = make_runtime(authority, tmp_path, monkeypatch, "b")
    try:
        fresh = load_on_disk_store()
        assert "blue flag" in fresh.format_for_system_prompt("memory")
        assert "launch-check" in build_skills_system_prompt()
        viewed = skill_view("launch-check")
        assert "Read references/flag.md" in viewed
        assert "blue flag" in skill_view("launch-check", file_path="references/flag.md")
    finally:
        b.close()


def test_native_approval_denial_and_idempotent_replay(authority, tmp_path, monkeypatch):
    from tools.memory_tool import create_memory_store, memory_tool, apply_memory_pending
    from tools.skill_manager_tool import skill_manage, apply_skill_pending
    from tools import write_approval as wa
    from tools.skill_ledger import list_entries
    runtime = make_runtime(authority, tmp_path, monkeypatch, "approved", approval=True)
    try:
        memory = create_memory_store()
        memory.load_from_disk()
        staged = json.loads(memory_tool("add", content="Remember the approved flag.", store=memory))
        assert staged.get("staged"), staged
        assert authority[0].snapshot(grant(authority[0], "observer"))["resources"] == []
        payload = wa.get_pending("memory", staged["pending_id"])["payload"]
        assert apply_memory_pending(payload, memory)["success"]
        assert apply_memory_pending(payload, memory)["replayed"]
        skill = "---\nname: approved-check\ndescription: Use when checking approval.\n---\n# Check\nRequire approval.\n"
        staged_skill = json.loads(skill_manage("create", "approved-check", content=skill))
        assert staged_skill.get("staged"), staged_skill
        assert not (runtime.home / "skills" / "approved-check").exists()
        assert list_entries("approved-check") == []
        payload = wa.get_pending("skills", staged_skill["pending_id"])["payload"]
        assert json.loads(apply_skill_pending(payload))["success"]
        entries = list_entries("approved-check")
        assert len(entries) == 1
        assert entries[0]["evidence"]["authority_receipt"]
        assert json.loads(apply_skill_pending(payload))["replayed"]
        assert len(list_entries("approved-check")) == 1
        changed = {**payload, "content": skill + "Altered after approval"}
        with pytest.raises(KnowledgeError, match="different payload"):
            apply_skill_pending(changed)
        resources = authority[0].snapshot(grant(authority[0], "observer2"))["resources"]
        assert len(resources) == 2
        assert all(r["revision"] == 1 for r in resources)
        home = runtime.home
        runtime.close()
        runtime = bootstrap_knowledge(url=authority[1], token=grant(authority[0], "approved", generation=2), hermes_home=home)
        assert len(list_entries("approved-check")) == 1
        assert json.loads(apply_skill_pending(payload))["replayed"]
        assert len(list_entries("approved-check")) == 1
    finally:
        runtime.close()


def test_lost_response_recovery_and_retained_snapshot(authority, tmp_path, monkeypatch):
    from tools.memory_tool import create_memory_store, memory_tool
    runtime = make_runtime(authority, tmp_path, monkeypatch, "recover")
    old_snapshot = runtime.snapshot_id
    memory = create_memory_store()
    memory.load_from_disk()
    post = runtime.client.post
    def drop_response(route, body):
        result = post(route, body)
        if route == "/v1/memory/mutations":
            raise httpx.ReadError("simulated lost response after durable commit")
        return result
    runtime.client.post = drop_response
    result = json.loads(memory_tool("add", content="Persist through a lost response.", store=memory))
    assert not result["success"]
    runtime.client.post = post
    runtime.close()
    runtime = bootstrap_knowledge(url=authority[1], token=grant(authority[0], "recover", generation=2),
                                  hermes_home=runtime.home)
    try:
        assert runtime.snapshot_id == old_snapshot
        memory = create_memory_store()
        memory.load_from_disk()
        assert memory.format_for_system_prompt("memory") is None
        assert memory.memory_entries == ["Persist through a lost response."]
        assert len(authority[0].snapshot(grant(authority[0], "observer"))["resources"]) == 1
    finally:
        runtime.close()


def test_native_history_export_revisions_and_inline_search_after_stop(authority, tmp_path, monkeypatch):
    from hermes_state import SessionDB
    from agent.inline_tool_executors import _session_search, InlineToolContext
    from types import SimpleNamespace
    a = make_runtime(authority, tmp_path, monkeypatch, "history-a")
    db = SessionDB(db_path=a.home / "state.db")
    db.create_session("session-a", source="discord")
    db.append_message("session-a", "user", "The exported blue flag history.")
    assert a.flush_history(a.home / "state.db")["acknowledged"] == 1
    assert a.flush_history(a.home / "state.db")["acknowledged"] == 0
    a.close()
    db.close()
    b = make_runtime(authority, tmp_path, monkeypatch, "history-b")
    try:
        agent = SimpleNamespace(session_id="session-b")
        result = json.loads(_session_search(agent, {"query": "blue flag"}, InlineToolContext("test")))
        assert result["total_sessions"] == 1
        assert result["sessions"][0]["session_id"] == "session-a"
        direct = json.loads(_session_search(agent, {"session_id": "session-a"}, InlineToolContext("test")))
        assert direct["messages"][0]["content"] == "The exported blue flag history."
        from tools.session_search_tool import session_search
        assert json.loads(session_search(query="blue flag"))["total_sessions"] == 1
        assert not json.loads(session_search(profile="other-private-profile"))["success"]
    finally:
        b.close()


def test_shared_history_schema_is_scoped_stable_and_preserves_local_fts(authority, tmp_path, monkeypatch):
    from copy import deepcopy
    from tools.session_search_tool import SESSION_SEARCH_SCHEMA
    from tools.registry import registry

    native = deepcopy(SESSION_SEARCH_SCHEMA)
    runtime = make_runtime(authority, tmp_path, monkeypatch, "schema-worker")
    try:
        shared = registry.get_definitions({"session_search"})[0]["function"]
        assert "literal substring" in shared["description"]
        assert "FTS5" not in json.dumps(shared)
        properties = shared["parameters"]["properties"]
        assert "not search syntax" in properties["query"]["description"]
        assert "Ignored" in properties["sort"]["description"]
        assert "Ignored" in properties["detail"]["description"]
        assert "rejected" in properties["profile"]["description"]
        assert "one exact role" in properties["role_filter"]["description"]
        assert registry.get_definitions({"session_search"})[0]["function"] == shared
        assert SESSION_SEARCH_SCHEMA == native
        with monkeypatch.context() as other_profile:
            other = tmp_path / "local-profile"
            other.mkdir()
            other_profile.setenv("HERMES_HOME", str(other))
            assert registry.get_definitions({"session_search"})[0]["function"] == native
        assert registry.get_definitions({"session_search"})[0]["function"] == shared
    finally:
        runtime.close()
    assert registry.get_definitions({"session_search"})[0]["function"] == native


def test_native_literal_search_after_stop_with_quotes_and_audience_filter(authority, tmp_path, monkeypatch):
    from hermes_state import SessionDB
    from tools import session_search_tool  # noqa: F401 (native registry registration)
    from tools.registry import registry

    a = make_runtime(authority, tmp_path, monkeypatch, "literal-a")
    db = SessionDB(db_path=a.home / "state.db")
    db.create_session("session-a", source="discord")
    db.append_message("session-a", "user", "Cluster canary 742 uses blue lantern 742")
    assert a.flush_history(a.home / "state.db")["acknowledged"] == 1
    a.close()
    db.close()
    authority[0].revoke_worker("literal-a")
    private = grant(authority[0], "private-canary", audience="dm:private")
    authority[0].ingest_history(private, {"mutation_id": "private-history", "records": [
        {"session_id": "private-session", "message_id": "1", "revision": 1,
         "payload": {"role": "user", "content": 'Private "canary" "742" and canary 742', "timestamp": 2}}
    ]})
    b = make_runtime(authority, tmp_path, monkeypatch, "literal-b")
    try:
        search = lambda args: json.loads(registry.dispatch("session_search", args))
        quoted = search({"query": '"canary" "742"', "limit": 5, "detail": "full"})
        assert quoted["success"] and quoted["total_sessions"] == 0
        assert quoted["results"] == [] and quoted["sessions"] == []
        assert "literal substring" in quoted["hint"]
        assert "unquoted contiguous phrase" in quoted["hint"]
        found = search({"query": "canary 742"})
        assert found["total_sessions"] == found["count"] == 1
        assert found["results"][0]["session_id"] == "session-a"
        assert found["sessions"][0]["matches"][0]["provenance"]["conversation_key"] == "literal-a"
        assert search({"session_id": "private-session"})["success"] is False
        assert search({"query": "Private"})["total_sessions"] == 0
        assert search({"query": "canary 742", "role_filter": "user"})["total_sessions"] == 1
        assert search({"query": "canary 742", "role_filter": "user,assistant"})["total_sessions"] == 0
        assert search({"query": "canary 742", "sort": "oldest", "detail": "full"}) == found
        assert search({"profile": "other"})["success"] is False
    finally:
        b.close()


def test_revoked_backend_never_falls_back_to_local(authority, tmp_path, monkeypatch):
    from tools.memory_tool import create_memory_store, memory_tool
    runtime = make_runtime(authority, tmp_path, monkeypatch, "revoked")
    memory = create_memory_store()
    memory.load_from_disk()
    authority[0].revoke_worker("revoked")
    try:
        result = json.loads(memory_tool("add", content="Must not be written locally.", store=memory))
        assert not result["success"]
        assert (runtime.home / "memories" / "MEMORY.md").read_text() == ""
        assert json.loads(runtime.search_history({}))["success"] is False
    finally:
        runtime.close()


def test_denied_write_and_disabled_approval_replay(authority, tmp_path, monkeypatch):
    from tools.memory_tool import load_on_disk_store, memory_tool, apply_memory_pending
    from tools import terminal_tool
    runtime = make_runtime(authority, tmp_path, monkeypatch, "denied", approval=True)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(terminal_tool, "_get_approval_callback", lambda: lambda *a, **kw: "deny")
            result = json.loads(memory_tool("add", content="This write was denied.", store=load_on_disk_store()))
            assert result["success"] is False
        assert runtime.resources == {}
    finally:
        runtime.close()
    runtime = make_runtime(authority, tmp_path, monkeypatch, "disabled", memory_enabled=False)
    try:
        result = apply_memory_pending({"action": "add", "target": "memory", "content": "disabled"}, load_on_disk_store())
        assert result["success"] is False
        assert "disabled" in result["error"]
    finally:
        runtime.close()


def test_skill_failed_batch_never_publishes_or_changes_live_files(authority, tmp_path, monkeypatch):
    from tools.skill_manager_tool import skill_manage
    runtime = make_runtime(authority, tmp_path, monkeypatch, "failed-batch")
    try:
        skill = "---\nname: atomic-check\ndescription: Use when checking atomic writes.\n---\n# Check\nKeep the original text.\n"
        result = json.loads(skill_manage("batch", "", operations=[
            {"action": "create", "name": "atomic-check", "content": skill},
            {"action": "patch", "name": "atomic-check", "old_string": "nonexistent needle", "new_string": "changed"}]))
        assert result["success"] is False
        assert not (runtime.home / "skills" / "atomic-check").exists()
        assert not runtime.resources
    finally:
        runtime.close()


def test_history_outage_revision_and_deletion_recover_without_duplicates(authority, tmp_path, monkeypatch):
    import sqlite3
    from hermes_state import SessionDB
    runtime = make_runtime(authority, tmp_path, monkeypatch, "history-revisions")
    db_path = runtime.home / "state.db"
    db = SessionDB(db_path=db_path)
    db.create_session("history", source="discord")
    db.append_message("history", "assistant", "Original durable marker")
    post = runtime.client.post
    def unavailable(*args, **kwargs):
        raise httpx.ConnectError("test authority unavailable")
    runtime.client.post = unavailable
    with pytest.raises(httpx.ConnectError):
        runtime.flush_history(db_path)
    runtime.client.post = post
    try:
        assert runtime.flush_history(db_path)["acknowledged"] == 1
        with sqlite3.connect(db_path) as local:
            local.execute("UPDATE messages SET content='Compressed summary marker',compacted=1,_compressed_summary=1")
        assert runtime.flush_history(db_path)["acknowledged"] == 1
        result = json.loads(runtime.search_history({"session_id": "history"}))
        assert result["message_count"] == 1
        assert result["messages"][0]["revision"] == 2
        assert result["messages"][0]["_compressed_summary"] == 1
        with sqlite3.connect(db_path) as local:
            local.execute("DELETE FROM messages")
        assert runtime.flush_history(db_path)["acknowledged"] == 1
        assert json.loads(runtime.search_history({"query": "marker"}))["total_sessions"] == 0
        with runtime.client.connect() as local:
            assert local.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0
    finally:
        db.close()
        runtime.close()


def test_request_and_storage_budgets_fail_closed(authority, tmp_path):
    import sqlite3
    from hermes_cluster.knowledge.limits import MAX_REQUEST_BYTES
    token = grant(authority[0], "body-limit")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    with httpx.Client(base_url=authority[1], headers=headers) as client:
        response = client.post("/v1/memory/mutations", content=b"x" * (MAX_REQUEST_BYTES + 1))
        assert response.status_code == 413
        response = client.post("/v1/history/batches", content=(b"x" * 16384 for _ in range(257)))
        assert response.status_code == 413
    store = KnowledgeStore(tmp_path / "bounded.db", max_bytes=128 * 1024)
    token = grant(store, "budget")
    failure = None
    for i in range(100):
        try:
            store.mutate(token, "memory", mutation(str(i), "x" * 9000))
        except (KnowledgeError, sqlite3.DatabaseError) as exc:
            failure = exc
            break
    assert failure is not None
    assert store.path.stat().st_size <= 128 * 1024
    store.revoke_worker("budget")
    with pytest.raises(KnowledgeError) as error:
        store.snapshot(token)
    assert error.value.status == 403


def test_deleted_skill_can_be_recreated_from_fresh_snapshot(authority, tmp_path, monkeypatch):
    from tools.skill_manager_tool import skill_manage
    first = make_runtime(authority, tmp_path, monkeypatch, "delete-first")
    skill = "---\nname: recreate-check\ndescription: Use when checking recreation.\n---\n# Check\nVerify recreation.\n"
    assert json.loads(skill_manage("create", "recreate-check", content=skill))["success"]
    assert json.loads(skill_manage("delete", "recreate-check"))["success"]
    first.close()
    second = make_runtime(authority, tmp_path, monkeypatch, "delete-second")
    try:
        result = json.loads(skill_manage("create", "recreate-check", content=skill))
        assert result["success"], result
        resource = next(r for r in second.resources.values() if r["key"] == "recreate-check")
        assert resource["revision"] == 3
    finally:
        second.close()


def test_history_revision_cannot_relabel_or_cross_an_existing_audience(tmp_path):
    store = KnowledgeStore(tmp_path / "history-audience.db")
    private = grant(store, "moving", audience="private")
    request = {"mutation_id": "private-original", "records": [{"session_id": "session", "message_id": "1",
        "revision": 1, "payload": {"content": "original private history", "role": "user"}}]}
    store.ingest_history(private, request)
    public = grant(store, "moving", audience="public", generation=2)
    for operation in (lambda: store.receipt(public, request["mutation_id"]),
                      lambda: store.ingest_history(public, request),
                      lambda: store.ingest_history(public, {**request, "origin": {"changed": True}})):
        with pytest.raises(KnowledgeError, match="receipt not found") as denied:
            operation()
        assert denied.value.status == 404
    update = {"mutation_id": "public-revision", "records": [{"session_id": "session", "message_id": "1",
        "revision": 2, "payload": {"content": "replacement under new audience", "role": "user"}}]}
    with pytest.raises(KnowledgeError, match="audience is immutable"):
        store.ingest_history(public, update)
    assert store.search_history(public, {})["total_sessions"] == 0
    reader = grant(store, "private-reader", audience="private")
    result = store.search_history(reader, {"session_id": "session"})
    assert result["messages"][0]["content"] == "original private history"
    assert result["messages"][0]["revision"] == 1


def test_legacy_receipts_without_audience_fail_closed(tmp_path):
    import sqlite3
    path = tmp_path / "legacy.db"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE receipts(agent TEXT,conversation TEXT,id TEXT,request_hash TEXT,result TEXT,"
                   "PRIMARY KEY(agent,conversation,id))")
        db.execute("INSERT INTO receipts VALUES('wallace','legacy','old','hash',?)",
                   (json.dumps({"success": True, "acknowledged": 42}),))
    store = KnowledgeStore(path)
    token = grant(store, "legacy")
    with pytest.raises(KnowledgeError, match="receipt not found"):
        store.receipt(token, "old")
    accepted = store.mutate(token, "memory", mutation("new", "new scoped receipt"))
    assert store.receipt(token, "new") == accepted
    assert KnowledgeStore(path).receipt(token, "new") == accepted


def test_many_history_removals_and_compression_commit_every_tombstone(authority, tmp_path, monkeypatch):
    import sqlite3
    from hermes_state import SessionDB
    runtime = make_runtime(authority, tmp_path, monkeypatch, "history-many")
    db_path = runtime.home / "state.db"
    db = SessionDB(db_path=db_path)
    db.create_session("many", source="discord")
    with sqlite3.connect(db_path) as local:
        local.executemany("INSERT INTO messages(session_id,role,content,timestamp) VALUES('many','user',?,?)",
                          [(f"message {i}", i) for i in range(503)])
    try:
        assert runtime.flush_history(db_path)["acknowledged"] == 503
        with sqlite3.connect(db_path) as local:
            local.execute("DELETE FROM messages WHERE id < (SELECT max(id) FROM messages)")
            local.execute("UPDATE messages SET content='remaining summary',compacted=1,_compressed_summary=1")
        assert runtime.flush_history(db_path)["acknowledged"] == 503
        result = json.loads(runtime.search_history({"session_id": "many"}))
        assert result["message_count"] == 1
        assert result["messages"][0]["content"] == "remaining summary"
        with runtime.client.connect() as local:
            assert local.execute("SELECT count(*) FROM exports WHERE hash='deleted'").fetchone()[0] == 502
        assert runtime.flush_history(db_path)["acknowledged"] == 0
    finally:
        db.close()
        runtime.close()
