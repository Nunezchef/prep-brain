import re
from pathlib import Path
from typing import Dict, Optional

import yaml

DEFAULT_GLOBAL_ALIASES: Dict[str, str] = {
    "#": "lb",
    "lbs": "lb",
    "cs": "case",
    "ea": "each",
    "pcs": "each",
    "qt": "quart",
    "pt": "pint",
    "gal": "gallon",
    "lex": "lexan",
    "hp": "high priority",
    "86": "out of stock",
    "par": "par level",
    "on the fly": "rush",
}


CONFIG_PATH = Path("config.yaml")


def _load_config() -> Dict[str, object]:
    if not CONFIG_PATH.exists():
        return {}
    try:
        return yaml.safe_load(CONFIG_PATH.read_text()) or {}
    except Exception:
        return {}


def _save_config(config: Dict[str, object]) -> None:
    CONFIG_PATH.write_text(yaml.dump(config, sort_keys=False, allow_unicode=True))


def get_alias_map(restaurant_tag: Optional[str] = None) -> Dict[str, str]:
    cfg = _load_config()
    lexicon_cfg = cfg.get("lexicon", {}) if isinstance(cfg.get("lexicon"), dict) else {}

    aliases: Dict[str, str] = {k.lower(): v for k, v in DEFAULT_GLOBAL_ALIASES.items()}

    configured_defaults = lexicon_cfg.get("default_aliases", {})
    if isinstance(configured_defaults, dict):
        for key, value in configured_defaults.items():
            aliases[str(key).strip().lower()] = str(value).strip()

    if restaurant_tag:
        restaurants = lexicon_cfg.get("restaurants", {})
        if isinstance(restaurants, dict):
            overrides = restaurants.get(restaurant_tag, {})
            if isinstance(overrides, dict):
                for key, value in overrides.items():
                    aliases[str(key).strip().lower()] = str(value).strip()

    return aliases


def resolve_alias(term: str, restaurant_tag: Optional[str] = None) -> str:
    value = str(term or "").strip()
    if not value:
        return ""
    aliases = get_alias_map(restaurant_tag=restaurant_tag)
    return aliases.get(value.lower(), value)


def replace_aliases_in_text(text: str, restaurant_tag: Optional[str] = None) -> str:
    value = str(text or "")
    if not value:
        return ""

    aliases = get_alias_map(restaurant_tag=restaurant_tag)
    out = value

    # Apply phrase aliases first (longest first) to preserve kitchen shorthand semantics.
    phrase_aliases = sorted(
        ((k, v) for k, v in aliases.items() if " " in k),
        key=lambda item: len(item[0]),
        reverse=True,
    )
    for alias, normalized in phrase_aliases:
        out = re.sub(rf"\b{re.escape(alias)}\b", normalized, out, flags=re.IGNORECASE)

    token_pattern = re.compile(r"\b[\w#]+\b")

    def _token_sub(match: re.Match[str]) -> str:
        token = match.group(0)
        return aliases.get(token.lower(), token)

    return token_pattern.sub(_token_sub, out)


def get_lexicon_config() -> Dict[str, object]:
    cfg = _load_config()
    lexicon_cfg = cfg.get("lexicon", {}) if isinstance(cfg.get("lexicon"), dict) else {}
    return {
        "default_aliases": lexicon_cfg.get("default_aliases", {}),
        "restaurants": lexicon_cfg.get("restaurants", {}),
        "builtin_defaults": DEFAULT_GLOBAL_ALIASES,
    }


def update_lexicon_config(payload: Dict[str, object]) -> Dict[str, object]:
    cfg = _load_config()
    lexicon_cfg = cfg.get("lexicon", {}) if isinstance(cfg.get("lexicon"), dict) else {}

    if "default_aliases" in payload and isinstance(payload["default_aliases"], dict):
        lexicon_cfg["default_aliases"] = payload["default_aliases"]

    if "restaurants" in payload and isinstance(payload["restaurants"], dict):
        lexicon_cfg["restaurants"] = payload["restaurants"]

    cfg["lexicon"] = lexicon_cfg
    _save_config(cfg)
    return get_lexicon_config()
