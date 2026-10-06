"""Authenticated task routes share peer and creation-policy ownership."""

import json
import threading
import urllib.request
from http.server import ThreadingHTTPServer
from types import SimpleNamespace

import pytest

from gateway.config import GatewayConfig, PlatformConfig
from plugins.platforms.a2a import protocol
from plugins.platforms.a2a.adapter import A2AAdapter, A2ARequestHandler


@pytest.fixture
def task_http(monkeypatch):
    monkeypatch.setenv("A2A_PEER_TOKENS", "alfred:test-a,gromit:test-g")
    monkeypatch.delenv("A2A_BEARER_TOKEN", raising=False)
    monkeypatch.delenv("A2A_TRUSTED_PEERS", raising=False)
    monkeypatch.setenv("A2A_RATE_LIMIT", "10000")
    config = GatewayConfig.from_dict({"gateway": {"a2a_conversation_only_peers": ["alfred"]}})
    adapter = A2AAdapter(PlatformConfig(enabled=True))
    adapter.set_session_store(SimpleNamespace(config=config))
    server = ThreadingHTTPServer(("127.0.0.1", 0), A2ARequestHandler)
    server.adapter = adapter
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    def request(method, params, peer="alfred"):
        token = {"alfred": "test-a", "gromit": "test-g"}[peer]
        req = urllib.request.Request(
            f"http://127.0.0.1:{server.server_port}/",
            data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(req, timeout=5) as response:
            body = response.read().decode()
            if response.headers.get_content_type() == "text/event-stream":
                return [json.loads(line[6:]) for line in body.splitlines() if line.startswith("data: ")]
            return json.loads(body)

    try:
        yield adapter, config, request
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def seed(adapter, task_id, peer, mode, *, terminal=True):
    adapter.tasks.create(task_id, "shared", peer, policy_mode=mode)
    if terminal:
        adapter.tasks.complete(task_id, protocol.STATE_COMPLETED, f"PRIVATE:{task_id}")
    adapter.tasks.set_push_config(task_id, "https://example.invalid/original")


def assert_hidden(request, task_id, peer):
    params = {"taskId": task_id, "metadata": {"peer": "gromit", "policy_mode": "unrestricted"},
              "pushNotificationConfig": {"url": "https://example.invalid/attacker"}}
    for method in ("GetTask", "SubscribeToTask", "CancelTask", "CreateTaskPushNotificationConfig",
                   "GetTaskPushNotificationConfig", "ListTaskPushNotificationConfigs", "DeleteTaskPushNotificationConfig"):
        response = request(method, params, peer)
        assert response["error"]["code"] == protocol.ERR_TASK_NOT_FOUND, (method, response)
        assert "PRIVATE:" not in json.dumps(response)


def test_all_http_task_routes_enforce_peer_and_policy(task_http, monkeypatch):
    adapter, config, request = task_http
    for peer, mode in (("alfred", "restricted"), ("gromit", "unrestricted")):
        seed(adapter, peer, peer, mode)
        seed(adapter, peer + "-working", peer, mode, terminal=False)
        other = "gromit" if peer == "alfred" else "alfred"
        assert_hidden(request, peer, other)
        assert_hidden(request, peer + "-working", other)
        assert adapter.tasks.get(peer + "-working")["state"] == protocol.STATE_SUBMITTED
        assert adapter.tasks.get_push_config(peer)["pushNotificationConfig"]["url"].endswith("/original")
        assert request("GetTask", {"taskId": peer}, peer)["result"]["id"] == peer
        assert "PRIVATE:" + peer in json.dumps(request("SubscribeToTask", {"taskId": peer}, peer))
        assert request("CancelTask", {"taskId": peer + "-working"}, peer)["result"]["status"]["state"] == protocol.STATE_CANCELED
        params = {"taskId": peer, "pushNotificationConfig": {"url": "https://example.invalid/owner"}}
        created = request("CreateTaskPushNotificationConfig", params, peer)["result"]
        assert request("GetTaskPushNotificationConfig", params, peer)["result"] == created
        assert request("ListTaskPushNotificationConfigs", params, peer)["result"]["configs"] == [created]
        assert request("DeleteTaskPushNotificationConfig", params, peer)["result"]["deleted"]
        assert request("ListTaskPushNotificationConfigs", params, peer)["result"]["configs"] == []

    # A live subscription must recheck the mode after waiting for its reply.
    seed(adapter, "waiting", "gromit", "unrestricted", terminal=False)
    watching = threading.Event()
    watch = adapter.tasks.watch
    def watch_and_signal(*args):
        future = watch(*args)
        watching.set()
        return future
    monkeypatch.setattr(adapter.tasks, "watch", watch_and_signal)
    streamed, failures = [], []
    def subscribe():
        try:
            streamed.append(request("SubscribeToTask", {"taskId": "waiting"}, "gromit"))
        except BaseException as exc:
            failures.append(exc)
    subscriber = threading.Thread(target=subscribe, daemon=True)
    subscriber.start()
    try:
        assert watching.wait(timeout=5)
        # Both directions hide existing tasks, even from their owner.
        config.a2a_conversation_only_peers[:] = ["gromit"]
    finally:
        adapter.tasks.complete("waiting", protocol.STATE_COMPLETED, "PRIVATE:waiting")
        subscriber.join(timeout=5)
    assert not subscriber.is_alive() and not failures
    assert streamed == [[]]
    for peer in ("alfred", "gromit"):
        assert_hidden(request, peer, peer)
        assert request("ListTasks", {"includeArtifacts": True}, peer)["result"]["totalSize"] == 0
    for peer, mode in (("alfred", "unrestricted"), ("gromit", "restricted")):
        seed(adapter, "new-" + peer, peer, mode)
        assert request("GetTask", {"taskId": "new-" + peer}, peer)["result"]["id"] == "new-" + peer
    seed(adapter, "legacy", "gromit", "unrestricted")
    del adapter.tasks._tasks["legacy"]["policy_mode"]
    assert_hidden(request, "legacy", "gromit")
    config.a2a_conversation_only_peers.clear()
    assert_hidden(request, "legacy", "gromit")


def test_http_pagination_filters_before_count_and_metadata_cannot_choose_identity(task_http):
    adapter, config, request = task_http
    for index in range(5):
        seed(adapter, f"a{index}", "alfred", "restricted")
        seed(adapter, f"g{index}", "gromit", "unrestricted")
        seed(adapter, f"old{index}", "alfred", "unrestricted")
    seen, token = [], ""
    for expected_size in (2, 2, 1):
        result = request("ListTasks", {"pageSize": 2, "pageToken": token, "includeArtifacts": True,
                                      "contextId": "shared", "status": protocol.STATE_COMPLETED,
                                      "metadata": {"peer": "gromit"}})["result"]
        assert result["totalSize"] == 5 and result["pageSize"] == 2
        assert len(result["tasks"]) == expected_size
        seen.extend(task["id"] for task in result["tasks"])
        token = result["nextPageToken"]
    assert seen == [f"a{i}" for i in reversed(range(5))] and token == ""
    assert request("ListTasks", {"pageToken": "100"})["result"]["totalSize"] == 5
    # The real authenticated send route stamps ownership even when dispatch ends early.
    result = request("SendMessage", {"message": {"role": "user", "parts": [{"text": "hello"}]},
                                     "metadata": {"peer": "gromit", "conversation_only": False}})["result"]
    task_id = result["task"]["id"]
    rec = adapter.tasks.get(task_id)
    assert (rec["peer"], rec["policy_mode"]) == ("alfred", "restricted")
    assert request("GetTask", {"taskId": task_id})["result"]["id"] == task_id
    assert_hidden(request, task_id, "gromit")


@pytest.mark.parametrize("method", ["SendMessage", "SendStreamingMessage"])
@pytest.mark.parametrize("change_mode", [True, False])
def test_inflight_completion_and_push_recheck_policy(task_http, monkeypatch, method, change_mode):
    from concurrent.futures import Future
    import time

    adapter, config, request = task_http
    ready = threading.Event()
    pushed, replies, failures = [], [], []
    pending = {}

    def prepare(params, peer, agent=None):
        task_id = "inflight-completion"
        rec = adapter.tasks.create(task_id, "completion-context", peer, policy_mode=adapter._policy_mode(peer))
        adapter.tasks.set_push_config(task_id, "https://example.invalid/callback")
        future = Future()
        pending.update(task_id=task_id, context_id="completion-context", peer=peer, future=future,
                       started=time.time(), created_iso=rec["created_iso"],
                       history_context_id=adapter._history_context_id(peer, "completion-context"))
        ready.set()
        return None, pending

    class PushResponse:
        status = 200
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return False

    real_open = urllib.request.urlopen
    def open_with_push_capture(req, *args, **kwargs):
        if req.full_url == "https://example.invalid/callback":
            pushed.append(json.loads(req.data))
            return PushResponse()
        return real_open(req, *args, **kwargs)

    monkeypatch.setattr(adapter, "_prepare_task", prepare)
    monkeypatch.setattr(urllib.request, "urlopen", open_with_push_capture)
    from plugins.platforms.a2a import security
    monkeypatch.setattr(security, "is_safe_callback_url", lambda *args, **kwargs: True)

    def send():
        try:
            replies.append(request(method, {"message": {"parts": [{"text": "hello"}]}}, "gromit"))
        except BaseException as exc:
            failures.append(exc)

    sender = threading.Thread(target=send, daemon=True)
    sender.start()
    try:
        assert ready.wait(timeout=5)
        if change_mode:
            config.a2a_conversation_only_peers[:] = ["alfred", "gromit"]
        pending["future"].set_result((protocol.STATE_COMPLETED, "PRIVATE:completion"))
    finally:
        sender.join(timeout=5)
    assert not sender.is_alive() and not failures
    if change_mode:
        assert "PRIVATE:completion" not in json.dumps(replies)
        assert not pushed
        assert "PRIVATE:completion" not in adapter.tasks.get(pending["task_id"])["reply"]
        # Also gate direct push delivery of an already-completed old-mode record.
        adapter.tasks.set_push_config(pending["task_id"], "https://example.invalid/callback")
        adapter._send_push_notification(pending["task_id"], "completion-context", "PRIVATE:late-push", protocol.STATE_COMPLETED)
        assert not pushed
    else:
        assert "PRIVATE:completion" in json.dumps(replies)
        assert "PRIVATE:completion" in json.dumps(pushed)
