"""Run one native Hermes conversation: python -m hermes_cluster.worker --config FILE."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from pathlib import Path
import signal
import time

from hermes_cluster.worker_config import (
    IDENTITY_FILE, STATUS_FILE, claim_home, install_runtime_config, prepare_environment,
    read_config, write_json,
)

logger = logging.getLogger(__name__)


def status(config: dict, state: str, **details) -> None:
    write_json(Path(config["hermes_home"]) / STATUS_FILE, {
        "state": state, "pid": os.getpid(), "worker_id": config["worker_id"],
        "generation": config["generation"], "conversation_key": config["conversation_key"],
        "updated_at": time.time(), **details,
    })


def healthy(path: str) -> bool:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
        if value["state"] != "ready":
            return False
        os.kill(int(value["pid"]), 0)
        return True
    except (OSError, ValueError, KeyError):
        return False


async def _run(config: dict, knowledge) -> int:
    # All Hermes imports are delayed until home, credentials and knowledge are fixed.
    from hermes_cluster.worker_runtime import ConversationGateway, ConversationPolicy
    from gateway.config import Platform

    policy = ConversationPolicy(config)
    runner = ConversationGateway(policy)
    loop = asyncio.get_running_loop()
    stopping = asyncio.Event()
    tasks = set()

    async def shutdown():
        if stopping.is_set():
            return
        stopping.set()
        status(config, "stopping", snapshot_id=knowledge.snapshot_id)
        await runner.stop()

    def request_stop():
        task = asyncio.create_task(shutdown())
        tasks.add(task)
        task.add_done_callback(tasks.discard)

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, request_stop)

    async def publish_history():
        while not stopping.is_set():
            try:
                await asyncio.to_thread(knowledge.flush_history, Path(config["hermes_home"]) / "state.db")
            except Exception:
                logger.exception("history publication failed; retrying from retained session database")
            try:
                await asyncio.wait_for(stopping.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass

    history_task = None
    try:
        if not await runner.start() or not runner._running:
            return 1
        adapter = runner.adapters.get(Platform.RELAY)
        if adapter is None or not adapter.is_connected:
            return 1
        history_task = asyncio.create_task(publish_history(), name="cluster-history-publication")
        status(config, "ready", snapshot_id=knowledge.snapshot_id)
        await runner.wait_for_shutdown()
        status(config, "stopping", snapshot_id=knowledge.snapshot_id)
    finally:
        stopping.set()
        await runner.stop()
        if history_task is not None:
            await history_task
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(sig)
        await asyncio.to_thread(knowledge.flush_history, Path(config["hermes_home"]) / "state.db")
    # Teardown can discover an unacknowledged delivery after shutdown was requested.
    return int(runner.exit_code or 0)


def run(config: dict) -> int:
    with claim_home(config) as identity:
        status(config, "booting")
        knowledge = None
        try:
            workspace = prepare_environment(config)
            install_runtime_config(config)
            os.chdir(workspace)
            # Import after HERMES_HOME; this installs hooks before native discovery.
            from hermes_cluster.knowledge import bootstrap_knowledge

            requested_snapshot = config["knowledge"].get("snapshot_id")
            retained_snapshot = identity.get("snapshot_id")
            if retained_snapshot and requested_snapshot and requested_snapshot != retained_snapshot:
                raise ValueError("retained conversation cannot silently switch its knowledge snapshot")
            knowledge = bootstrap_knowledge(
                url=config["knowledge"]["url"], token=os.environ["HERMES_CLUSTER_KNOWLEDGE_TOKEN"],
                hermes_home=config["hermes_home"], snapshot_id=retained_snapshot or requested_snapshot,
            )
            identity["snapshot_id"] = knowledge.snapshot_id
            write_json(Path(config["hermes_home"]) / IDENTITY_FILE, identity)
            result = asyncio.run(_run(config, knowledge))
            status(config, "stopped" if result == 0 else "failed", snapshot_id=knowledge.snapshot_id)
            return result
        except Exception as exc:
            # Exception text can contain provider URLs/credentials. Keep durable
            # status metadata limited to the exception type.
            status(config, "failed", error_type=type(exc).__name__)
            raise
        finally:
            if knowledge is not None:
                knowledge.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--config")
    selection.add_argument("--health", metavar="STATUS_FILE")
    args = parser.parse_args()
    if args.health:
        return 0 if healthy(args.health) else 1
    logging.basicConfig(level=logging.INFO)
    return run(read_config(args.config))


if __name__ == "__main__":
    raise SystemExit(main())
