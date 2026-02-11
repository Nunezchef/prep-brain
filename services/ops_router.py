import re
from typing import Any, Dict, List, Optional

from services import memory
from services.audit_log import record_event
from services.entity_resolver import get_recipe_by_id, resolve_recipe_by_name


_PRICE_PATTERNS = [
    re.compile(
        r"(?i)\b(?:update|set|change)\b\s+(?:the\s+)?price\s+(?:of|for)\s+(?P<item>.+?)\s+(?:to|=)\s*(?P<value>\$?\s*[0-9][0-9.,]*)\s*(?P<tail>.*)$"
    ),
    re.compile(
        r"(?i)\b(?:update|set|change)\b\s+(?P<item>.+?)\s+price\s+(?:to|=)\s*(?P<value>\$?\s*[0-9][0-9.,]*)\s*(?P<tail>.*)$"
    ),
]

_COST_PATTERNS = [
    re.compile(
        r"(?i)\b(?:update|set|change)\b\s+(?:the\s+)?cost\s+(?:of|for)\s+(?P<item>.+?)\s+(?:to|=)\s*(?P<value>\$?\s*[0-9][0-9.,]*)\s*(?P<tail>.*)$"
    ),
    re.compile(
        r"(?i)\b(?:update|set|change)\b\s+(?P<item>.+?)\s+cost\s+(?:to|=)\s*(?P<value>\$?\s*[0-9][0-9.,]*)\s*(?P<tail>.*)$"
    ),
]


def _normalize_unit(tail: str) -> str:
    lowered = str(tail or "").lower()
    if re.search(r"\b(per|a|an)\s+portion\b", lowered):
        return "portion"
    if "portion" in lowered:
        return "portion"
    return "portion"


def parse_currency(value_text: str) -> Optional[float]:
    raw = str(value_text or "").strip().replace("$", "").replace(" ", "")
    if not raw:
        return None
    if raw.count(",") == 1 and raw.count(".") == 0:
        raw = raw.replace(",", ".")
    else:
        raw = raw.replace(",", "")
    try:
        return float(raw)
    except ValueError:
        return None


def detect_ops_intent(text: str) -> Optional[Dict[str, Any]]:
    value = str(text or "").strip()
    if not value:
        return None

    for pattern in _PRICE_PATTERNS:
        match = pattern.search(value)
        if not match:
            continue
        amount = parse_currency(match.group("value"))
        if amount is None:
            return None
        item = str(match.group("item") or "").strip(" .")
        item = re.sub(r"(?i)^the\s+", "", item).strip()
        unit = _normalize_unit(match.group("tail") or "")
        return {
            "intent": "update_recipe_sales_price",
            "target_name": item,
            "price": float(amount),
            "unit": unit,
            "raw_text": value,
            "confidence": 0.95,
        }

    for pattern in _COST_PATTERNS:
        match = pattern.search(value)
        if not match:
            continue
        amount = parse_currency(match.group("value"))
        if amount is None:
            return None
        item = str(match.group("item") or "").strip(" .")
        item = re.sub(r"(?i)^the\s+", "", item).strip()
        return {
            "intent": "cost_clarification",
            "target_name": item,
            "price": float(amount),
            "unit": "portion",
            "raw_text": value,
            "confidence": 0.95,
        }

    return None


def _table_columns(con, table_name: str) -> List[str]:
    return [str(row[1]) for row in con.execute(f"PRAGMA table_info({table_name})").fetchall()]


def _ensure_recipe_price_columns(con) -> Dict[str, bool]:
    cols = set(_table_columns(con, "recipes"))
    if "sales_price" not in cols:
        con.execute("ALTER TABLE recipes ADD COLUMN sales_price REAL DEFAULT 0")
        cols.add("sales_price")
    if "unit" not in cols:
        con.execute("ALTER TABLE recipes ADD COLUMN unit TEXT")
        cols.add("unit")
    if "updated_at" not in cols:
        con.execute("ALTER TABLE recipes ADD COLUMN updated_at TEXT")
        cols.add("updated_at")
    con.commit()
    return {"has_sales_price": "sales_price" in cols, "has_unit": "unit" in cols, "has_updated_at": "updated_at" in cols}


def _load_sync_linked_menu_price() -> bool:
    try:
        cfg = memory.load_config() or {}
    except Exception:
        return True
    ops_cfg = cfg.get("ops") if isinstance(cfg, dict) else {}
    if isinstance(ops_cfg, dict):
        return bool(ops_cfg.get("sync_linked_menu_price", True))
    return True


def _update_recipe_sales_price(
    *,
    recipe_id: int,
    price: float,
    unit: str,
    actor_telegram_user_id: int,
    actor_display_name: str,
    raw_note: str,
) -> Dict[str, Any]:
    con = memory.get_conn()
    try:
        cols_state = _ensure_recipe_price_columns(con)
        row = con.execute(
            "SELECT id, name, sales_price, unit FROM recipes WHERE id = ? LIMIT 1",
            (int(recipe_id),),
        ).fetchone()
        if not row:
            return {"status": "not_found", "message": f"Recipe #{recipe_id} not found."}

        old_price = float(row["sales_price"] or 0.0)
        current_unit = str(row["unit"] or "").strip()
        next_unit = current_unit or str(unit or "portion")
        updates = ["sales_price = ?"]
        params: List[Any] = [round(float(price), 2)]
        if cols_state["has_unit"] and not current_unit and next_unit:
            updates.append("unit = ?")
            params.append(next_unit)
        if cols_state["has_updated_at"]:
            updates.append("updated_at = CURRENT_TIMESTAMP")
        params.append(int(recipe_id))
        con.execute(
            f"UPDATE recipes SET {', '.join(updates)} WHERE id = ?",
            tuple(params),
        )

        menu_updates = 0
        if _load_sync_linked_menu_price():
            menu_cols = set(_table_columns(con, "menu_items"))
            if {"recipe_id", "selling_price"} <= menu_cols:
                menu_rows = con.execute(
                    "SELECT id, name, selling_price FROM menu_items WHERE recipe_id = ?",
                    (int(recipe_id),),
                ).fetchall()
                for menu_row in menu_rows:
                    menu_old = float(menu_row["selling_price"] or 0.0)
                    con.execute(
                        "UPDATE menu_items SET selling_price = ? WHERE id = ?",
                        (round(float(price), 2), int(menu_row["id"])),
                    )
                    record_event(
                        actor_telegram_user_id=actor_telegram_user_id,
                        actor_display_name=actor_display_name,
                        action_type="update_menu_item_selling_price",
                        entity_type="menu_item",
                        entity_id=int(menu_row["id"]),
                        old_value={"selling_price": menu_old},
                        new_value={"selling_price": round(float(price), 2)},
                        note=raw_note,
                        con=con,
                    )
                    menu_updates += 1

        con.commit()

        record_event(
            actor_telegram_user_id=actor_telegram_user_id,
            actor_display_name=actor_display_name,
            action_type="update_sales_price",
            entity_type="recipe",
            entity_id=int(recipe_id),
            old_value={"sales_price": round(float(old_price), 2), "unit": current_unit},
            new_value={"sales_price": round(float(price), 2), "unit": next_unit},
            note=raw_note,
            con=con,
        )
        con.commit()

        return {
            "status": "updated",
            "recipe_id": int(recipe_id),
            "recipe_name": str(row["name"] or ""),
            "price": round(float(price), 2),
            "unit": next_unit or "portion",
            "display_unit": str(unit or next_unit or "portion"),
            "menu_items_updated": int(menu_updates),
        }
    finally:
        con.close()


def execute_ops_intent(
    intent: Dict[str, Any],
    *,
    actor_telegram_user_id: int,
    actor_display_name: str,
) -> Dict[str, Any]:
    intent_name = str(intent.get("intent") or "")
    if intent_name == "cost_clarification":
        return {
            "status": "needs_clarification",
            "reason": "cost_is_calculated",
            "target_name": str(intent.get("target_name") or ""),
            "price": float(intent.get("price") or 0.0),
            "unit": str(intent.get("unit") or "portion"),
            "raw_text": str(intent.get("raw_text") or ""),
        }

    if intent_name != "update_recipe_sales_price":
        return {"status": "unsupported_intent"}

    price = float(intent.get("price") or 0.0)
    if price < 0:
        return {"status": "validation_error", "message": "Price must be non-negative."}
    if price > 500 and not bool(intent.get("confirmed")):
        return {
            "status": "needs_confirmation",
            "reason": "high_price",
            "target_name": str(intent.get("target_name") or ""),
            "price": float(price),
            "unit": str(intent.get("unit") or "portion"),
            "raw_text": str(intent.get("raw_text") or ""),
        }

    target_name = str(intent.get("target_name") or "").strip()
    if not target_name:
        return {"status": "validation_error", "message": "Missing recipe name."}

    resolved = resolve_recipe_by_name(target_name)
    if resolved["status"] == "no_match":
        return {"status": "not_found", "target_name": target_name}
    if resolved["status"] == "ambiguous":
        return {
            "status": "needs_choice",
            "target_name": target_name,
            "choices": resolved["matches"][:5],
            "intent": intent,
        }

    best = resolved.get("best") or {}
    return _update_recipe_sales_price(
        recipe_id=int(best["id"]),
        price=price,
        unit=str(intent.get("unit") or "portion"),
        actor_telegram_user_id=actor_telegram_user_id,
        actor_display_name=actor_display_name,
        raw_note=str(intent.get("raw_text") or ""),
    )


def apply_sales_price_to_recipe_id(
    *,
    recipe_id: int,
    price: float,
    unit: str,
    actor_telegram_user_id: int,
    actor_display_name: str,
    raw_note: str,
) -> Dict[str, Any]:
    recipe = get_recipe_by_id(int(recipe_id))
    if not recipe:
        return {"status": "not_found", "message": "Recipe not found."}
    return _update_recipe_sales_price(
        recipe_id=int(recipe_id),
        price=float(price),
        unit=str(unit or "portion"),
        actor_telegram_user_id=actor_telegram_user_id,
        actor_display_name=actor_display_name,
        raw_note=raw_note,
    )
