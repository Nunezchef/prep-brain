from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = BASE_DIR / "config.yaml"


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _env_overrides() -> Dict[str, Any]:
    overrides: Dict[str, Any] = {}
    db_path = os.getenv("PREP_BRAIN_DB_PATH")
    if db_path:
        overrides.setdefault("memory", {})["db_path"] = db_path

    ollama_url = os.getenv("OLLAMA_URL")
    if ollama_url:
        overrides.setdefault("ollama", {})["base_url"] = ollama_url

    allowed = os.getenv("TELEGRAM_ALLOWED_USER_IDS", "").strip()
    if allowed:
        ids = []
        for token in allowed.split(","):
            token = token.strip()
            if not token:
                continue
            try:
                ids.append(int(token))
            except ValueError:
                continue
        if ids:
            overrides.setdefault("telegram", {})["allowed_user_ids"] = ids

    return overrides


def resolve_path(path_value: str, *, base_dir: Optional[Path] = None) -> Path:
    candidate = Path(path_value)
    if not candidate.is_absolute():
        candidate = (base_dir or BASE_DIR) / candidate
    return candidate.resolve()


@lru_cache(maxsize=1)
def load_config() -> Dict[str, Any]:
    # Load .env once through a single interface.
    load_dotenv(dotenv_path=BASE_DIR / ".env")

    config_path = os.getenv("PREP_BRAIN_CONFIG")
    path = resolve_path(config_path, base_dir=Path.cwd()) if config_path else DEFAULT_CONFIG_PATH

    data: Dict[str, Any] = {}
    if path.exists():
        loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            data = loaded

    return _deep_merge(data, _env_overrides())


def reload_config() -> Dict[str, Any]:
    load_config.cache_clear()
    return load_config()


def get_db_path(config: Optional[Dict[str, Any]] = None) -> Path:
    cfg = config or load_config()
    db_path = str(cfg.get("memory", {}).get("db_path", "data/memory.db"))
    return resolve_path(db_path)


def get_log_path(config: Optional[Dict[str, Any]] = None) -> Path:
    cfg = config or load_config()
    log_path = str(cfg.get("paths", {}).get("log_file", "logs/prep-brain.log"))
    return resolve_path(log_path)


def get_pid_path(config: Optional[Dict[str, Any]] = None) -> Path:
    cfg = config or load_config()
    pid_path = str(cfg.get("paths", {}).get("pid_file", "run/prep-brain.pid"))
    return resolve_path(pid_path)
