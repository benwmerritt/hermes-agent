"""Start the optional companion without importing an installed Hermes profile."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Retained Hermes conversation controller")
    parser.add_argument("--config", required=True, help="controller JSON configuration path")
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    required = {"data_dir", "guild_id", "bot_id", "allowed_user_ids", "allowed_channel_ids",
                "relay_url", "namespace", "image", "native_config_path"}
    if required - config.keys():
        parser.error("controller configuration is incomplete")
    if not config["allowed_user_ids"] or not config["allowed_channel_ids"]:
        parser.error("explicit user and channel scopes are required")
    root = Path(config["data_dir"])
    if not root.is_absolute():
        parser.error("data_dir must be absolute")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, 0o700)
    # Deployment Recreate is not a distributed lock. This additional retained
    # filesystem lock prevents two controllers from opening the same authority.
    with (root / "controller.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        connector_home = root / "connector-home"
        connector_home.mkdir(exist_ok=True, mode=0o700)
        if (connector_home / ".env").exists():
            raise ValueError("connector credentials must be injected, not loaded from a retained .env")
        os.environ["HERMES_HOME"] = str(connector_home)
        for key in ("DISCORD_BOT_TOKEN", "OPENAI_API_KEY", "HERMES_CLUSTER_OPERATOR_TOKEN"):
            if not os.environ.get(key):
                raise ValueError(f"missing injected {key}")
        import yaml
        import uvicorn
        from .app import create_app
        from .controller import Controller
        from .knowledge import KnowledgeStore
        from .kubernetes import KubernetesBackend
        config["native_config"] = yaml.safe_load(Path(config["native_config_path"]).read_text(encoding="utf-8"))
        backend = KubernetesBackend(config["namespace"], config["image"], hermes_config=config["native_config"],
                                     resources=config.get("worker_resources"))
        knowledge = KnowledgeStore(root / "knowledge.sqlite")
        controller = Controller(config, backend, knowledge)
        uvicorn.run(create_app(controller), host="0.0.0.0", port=8080, access_log=False,
                    limit_concurrency=64, ws_max_size=1024 * 1024, timeout_graceful_shutdown=40)


if __name__ == "__main__":
    main()
