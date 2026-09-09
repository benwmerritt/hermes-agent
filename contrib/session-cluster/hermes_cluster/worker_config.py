"""Worker bootstrap data, retained ownership, and private configuration."""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
from contextlib import contextmanager
from pathlib import Path

import yaml


STATUS_FILE = "cluster-worker-status.json"
IDENTITY_FILE = "cluster-worker-identity.json"


def write_json(path: Path, value: dict) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        os.chmod(temporary, 0o600)
        json.dump(value, stream, sort_keys=True)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def read_config(path: str | Path) -> dict:
    config = json.loads(Path(path).read_text(encoding="utf-8"))
    required = {"worker_id", "generation", "conversation_key", "source", "allowed_user_ids",
                "hermes_home", "workspace", "config_source", "relay", "knowledge"}
    if config.get("schema_version") != 1 or required - config.keys():
        raise ValueError("worker configuration requires schema_version 1 and all ownership fields")
    if not isinstance(config["generation"], int) or isinstance(config["generation"], bool) or config["generation"] < 1:
        raise ValueError("generation must be a positive integer")
    if not config["worker_id"] or not config["conversation_key"] or not config["allowed_user_ids"]:
        raise ValueError("worker identity, conversation and allowed users must be nonempty")
    for key in ("hermes_home", "workspace", "config_source"):
        if not Path(config[key]).is_absolute():
            raise ValueError(f"{key} must be an absolute path")
    if config["source"].get("platform") != "discord":
        raise ValueError("only Discord conversation workers are supported")
    if not config["source"].get("chat_id"):
        raise ValueError("source.chat_id is required")
    if config["source"].get("chat_type") != "dm" and not config["source"].get("scope_id"):
        raise ValueError("server conversations require source.scope_id")
    if config["source"].get("user_id") not in config["allowed_user_ids"]:
        raise ValueError("source owner must be an allowed user")
    if not config["relay"].get("url") or not config["relay"].get("bot_id") or not config["knowledge"].get("url"):
        raise ValueError("relay URL, bot identity, and knowledge URL are required")
    native_config = yaml.safe_load(Path(config["config_source"]).read_text(encoding="utf-8")) or {}
    revision = hashlib.sha256(json.dumps(native_config, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    if config.get("config_revision", revision) != revision:
        raise ValueError("immutable Hermes configuration digest does not match")
    config["config_revision"] = revision
    personality = config.get("personality", "")
    if not isinstance(personality, str):
        raise ValueError("personality must be text")
    config["personality"] = personality
    config["personality_digest"] = hashlib.sha256(personality.encode()).hexdigest()
    return config


@contextmanager
def claim_home(config: dict):
    home = Path(config["hermes_home"])
    home.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (home / "cluster-worker.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("another worker owns this retained home") from exc
        identity_path = home / IDENTITY_FILE
        identity = json.loads(identity_path.read_text(encoding="utf-8")) if identity_path.exists() else {}
        retained_keys = ("worker_id", "conversation_key", "source", "config_revision", "personality", "personality_digest")
        for key in retained_keys:
            if key in identity and identity[key] != config[key]:
                raise ValueError("retained home belongs to a different conversation")
        if identity.get("generation", 0) > config["generation"]:
            raise ValueError("stale worker generation cannot reuse retained home")
        identity.update({key: config[key] for key in (*retained_keys, "generation") if key in config})
        write_json(identity_path, identity)
        yield identity


def prepare_environment(config: dict) -> Path:
    home = Path(config["hermes_home"])
    if (home / ".env").exists():
        raise ValueError("workers do not load a retained .env; inject individual credentials")
    if any((home / name).exists() for name in ("auth.json", "nous_auth.json")):
        raise ValueError("OAuth state is not supported in conversation workers; use the approved proxy")
    if os.environ.get("DISCORD_BOT_TOKEN"):
        raise ValueError("Discord bot credentials belong only to the connector")
    for name, filename in config.get("secret_files", {}).items():
        if not name.isidentifier() or not name.isupper() or name in {"HOME", "PATH", "PYTHONPATH", "HERMES_HOME", "DISCORD_BOT_TOKEN"}:
            raise ValueError("invalid worker secret variable")
        value = Path(filename).read_text(encoding="utf-8").strip()
        if not value:
            raise ValueError(f"empty credential file for {name}")
        os.environ[name] = value
    for required in ("GATEWAY_RELAY_SECRET", "HERMES_CLUSTER_KNOWLEDGE_TOKEN"):
        if not os.environ.get(required):
            raise ValueError(f"missing injected credential: {required}")
    os.environ["HERMES_HOME"] = str(home)
    # Existing upstream environment bridges; behavior is authored only in worker.json.
    os.environ["GATEWAY_RELAY_ID"] = f"{config['worker_id']}:{config['generation']}"
    os.environ["GATEWAY_RELAY_URL"] = config["relay"]["url"]
    os.environ["GATEWAY_RELAY_PLATFORMS"] = "discord"
    os.environ["GATEWAY_RELAY_BOT_IDS"] = json.dumps({"discord": {"botId": config["relay"]["bot_id"]}})
    os.environ["HERMES_EXEC_ASK"] = "1"
    os.environ["HERMES_GATEWAY_NO_SUPERVISE"] = "1"
    workspace = Path(config["workspace"])
    workspace.mkdir(parents=True, exist_ok=True, mode=0o700)
    return workspace


def install_runtime_config(config: dict) -> None:
    raw = yaml.safe_load(Path(config["config_source"]).read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError("Hermes config must be a YAML mapping")
    gateway = raw.setdefault("gateway", {})
    gateway.update(relay_url=config["relay"]["url"], multiplex_profiles=False,
                   group_sessions_per_user=True, thread_sessions_per_user=False)
    raw["platforms"] = {"relay": {"enabled": True, "extra": {"relay_url": config["relay"]["url"]}}}
    raw.setdefault("terminal", {}).update(backend="local", cwd=config["workspace"])
    raw.setdefault("curator", {})["enabled"] = False
    raw.setdefault("auxiliary", {}).setdefault("background_review", {})["enabled"] = False
    raw.setdefault("kanban", {})["dispatch_in_gateway"] = False
    raw.setdefault("agent", {}).setdefault("disabled_toolsets", [])
    raw["agent"]["disabled_toolsets"] = sorted(set(raw["agent"]["disabled_toolsets"]) | {"cronjob", "kanban"})
    home = Path(config["hermes_home"])
    target = home / "config.yaml"
    temporary = target.with_suffix(".yaml.tmp")
    temporary.write_text(yaml.safe_dump(raw, sort_keys=False))
    temporary.chmod(0o600)
    temporary.replace(target)
    if "personality" in config:
        (home / "SOUL.md").write_text(config["personality"], encoding="utf-8")
