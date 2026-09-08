import concurrent.futures

import pytest

from gateway.relay.auth import make_upgrade_token
from hermes_cluster.ledger import AdmissionError, Ledger, OwnershipError


def admit(store, event="message-1", thread="thread-a", audience="public"):
    source = {"scope_id": "guild", "chat_id": thread, "thread_id": thread, "user_id": "owner"}
    return store.admit(agent_id="agent", native_key=f"thread:{thread}", source=source,
                       audience=audience, event_id=event, payload={"text": event, "source": source}, actor="owner")


def test_concurrent_duplicate_ingress_reserves_only_one_owner_and_survives_reopen(tmp_path):
    path = tmp_path / "router.sqlite"
    store = Ledger(path, max_workers=1)
    with concurrent.futures.ThreadPoolExecutor(8) as pool:
        results = list(pool.map(lambda _: admit(store), range(20)))
    assert sum(is_new for _, is_new in results) == 1
    owner = store.reserve_next()
    assert owner["id"] == results[0][0]["id"]
    assert store.reserve_next() is None
    store.transition(owner["id"], 1, "ready")
    store.dispatch("message-1", owner["id"], 1)
    recovered = Ledger(path)
    assert recovered.queued(owner["id"]) == []  # Uncertain delivery never replays.
    recovered.acknowledge("message-1", owner["id"], 1)
    assert recovered.queued(owner["id"]) == []


def test_capacity_and_queue_bounds_are_independent_and_persistent(tmp_path):
    store = Ledger(tmp_path / "state", max_workers=1, max_queued=2)
    a, _ = admit(store)
    b, _ = admit(store, "message-2", "thread-b")
    assert store.reserve_next()["id"] == a["id"]
    assert store.reserve_next() is None
    assert store.get(b["id"])["status"] == "queued"
    with pytest.raises(AdmissionError, match="queue is full"):
        admit(store, "message-3", "thread-c")
    assert len(store.conversations()) == 2
    store.transition(a["id"], 1, "unavailable")
    assert store.reserve_next() is None  # A lost node is not a free slot or fence.


def test_changed_audience_and_duplicate_payload_fail_closed(tmp_path):
    store = Ledger(tmp_path / "state")
    admit(store)
    with pytest.raises(OwnershipError):
        admit(store, "message-2", audience="private")
    with pytest.raises(AdmissionError):
        admit(store, thread="thread-b")


def test_worker_credentials_expiry_revocation_and_message_scope(tmp_path):
    store = Ledger(tmp_path / "state")
    row, _ = admit(store)
    store.reserve_next()
    wid = f'{row["id"]}:1'
    token = make_upgrade_token(wid, row["relay_secret"])
    assert store.authenticate(f"Bearer {token}")["id"] == row["id"]
    with pytest.raises(OwnershipError):
        store.authenticate("Bearer " + make_upgrade_token(wid, "different-secret"))
    with pytest.raises(OwnershipError):
        store.authenticate("Bearer " + make_upgrade_token(wid, row["relay_secret"], 0))
    store.remember_message("bot-message", row["id"], 1, "bot")
    assert store.owns_message("bot-message", row["id"], bot_only=True)
    assert not store.owns_message("bot-message", "another-conversation", bot_only=True)
    store.transition(row["id"], 1, "stopped")
    with pytest.raises(OwnershipError):
        store.authenticate(f"Bearer {token}")


def test_outbound_lost_result_cannot_repeat_effect(tmp_path):
    path = tmp_path / "state"
    store = Ledger(path)
    action = {"op": "send", "content": "one reply"}
    assert store.begin_outbound("worker", "request", action) is None
    store = Ledger(path)
    assert store.begin_outbound("worker", "request", action)["ambiguous"]
    store.finish_outbound("worker", "request", {"success": True, "message_id": "sent"})
    assert store.begin_outbound("worker", "request", action)["message_id"] == "sent"
    with pytest.raises(OwnershipError):
        store.begin_outbound("worker", "request", {**action, "content": "changed"})


def test_approval_binds_actor_thread_message_generation_and_offered_option(tmp_path):
    store = Ledger(tmp_path / "state")
    row, _ = admit(store)
    store.reserve_next()
    store.transition(row["id"], 1, "ready")
    store.create_prompt("prompt", row, "thread-a", [{"id": "deny"}, {"id": "once"}])
    store.attach_prompt("prompt", "approval-message")
    args = dict(actor="owner", channel_id="thread-a", message_id="approval-message", option="deny")
    for changed in ({"actor": "stranger"}, {"channel_id": "thread-b"},
                    {"message_id": "other-message"}, {"option": "always"}):
        with pytest.raises(OwnershipError):
            store.consume_prompt("prompt", **{**args, **changed})
    assert store.consume_prompt("prompt", **args)["id"] == row["id"]
    with pytest.raises(OwnershipError):
        store.consume_prompt("prompt", **args)
