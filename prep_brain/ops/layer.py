from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

from prep_brain.config import load_config, resolve_path

CONFIG = load_config()

# ---- public API ----


@dataclass
class OpsResult:
    handled: bool
    reply: Optional[str] = None


def try_handle_text(update, context) -> OpsResult:
    """
    Entry point called by Telegram text handler BEFORE LLM.
    Uses context.user_data["pending_ops"] for multi-step clarification.
    """
    text = (getattr(update, "message", None) and update.message.text) or ""
    text = (text or "").strip()
    if not text:
        return OpsResult(False)

    pending = context.user_data.get("pending_ops")
    if pending:
        return _handle_pending(text, pending, context)

    # intent detection (deterministic)
    if _looks_like_add_recipe(text):
        return _start_add_recipe(text, context)

    m = _match_update_price_or_cost(text)
    if m:
        kind, name, value = m
        return _update_recipe_money(kind=kind, recipe_name=name, value=value)

    return OpsResult(False)


# ---- intent detection ----


def _looks_like_add_recipe(text: str) -> bool:
    t = text.lower()
    return any(
        k in t
        for k in (
            "add the following recipe",
            "add recipe",
            "create recipe",
            "new recipe",
            "save recipe",
        )
    )


def _match_update_price_or_cost(text: str) -> Optional[Tuple[str, str, float]]:
    """
    returns ("price"|"cost", recipe_name, value)
    """
    t = text.strip()
    # common patterns: "update price of X to 4.32", "set cost for X to $1.20"
    rx = re.compile(
        r"(?i)\b(update|set)\s+(price|cost)\s+(of|for)\s+(?P<name>.+?)\s+(to|=)\s*\$?(?P<val>\d+(\.\d+)?)"
    )
    m = rx.search(t)
    if not m:
        return None
    kind = m.group(2).lower()
    name = m.group("name").strip().strip('"').strip("'")
    val = float(m.group("val"))
    return kind, name, val


# ---- DB helpers ----


def _db_path() -> str:
    # single source of truth
    path = CONFIG.get("memory", {}).get("db_path", "data/memory.db")
    return str(resolve_path(path))


def _conn() -> sqlite3.Connection:
    con = sqlite3.connect(_db_path())
    con.row_factory = sqlite3.Row
    return con


def _table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    cur = con.execute(f"PRAGMA table_info({table})")
    return {row["name"] for row in cur.fetchall()}


def _best_recipe_match(con: sqlite3.Connection, name: str) -> Optional[sqlite3.Row]:
    # 1) exact
    cur = con.execute("SELECT * FROM recipes WHERE LOWER(name)=LOWER(?) LIMIT 1", (name,))
    row = cur.fetchone()
    if row:
        return row

    # 2) like
    cur = con.execute("SELECT * FROM recipes WHERE name LIKE ? LIMIT 10", (f"%{name}%",))
    rows = cur.fetchall()
    if len(rows) == 1:
        return rows[0]
    if rows:
        # 3) fuzzy best from candidates
        best = None
        best_s = 0.0
        for r in rows:
            s = SequenceMatcher(None, name.lower(), (r["name"] or "").lower()).ratio()
            if s > best_s:
                best_s = s
                best = r
        return best

    # 4) global fuzzy (small sample)
    cur = con.execute("SELECT * FROM recipes ORDER BY recent_sales_count DESC, id DESC LIMIT 50")
    rows = cur.fetchall()
    best = None
    best_s = 0.0
    for r in rows:
        s = SequenceMatcher(None, name.lower(), (r["name"] or "").lower()).ratio()
        if s > best_s:
            best_s = s
            best = r
    return best if best_s >= 0.55 else None


# ---- add recipe flow (draft + clarify) ----


def _start_add_recipe(text: str, context) -> OpsResult:
    name, ingredients = _parse_recipe_block(text)

    if not name:
        context.user_data["pending_ops"] = {"type": "add_recipe", "step": "need_name"}
        return OpsResult(True, "Recipe name? (Send just the name.)")

    draft_id = _create_recipe_draft(name=name, ingredients=ingredients)

    # decide missing fields (ask one at a time)
    pending = {"type": "add_recipe", "draft_id": draft_id, "name": name}
    missing = []

    # if yield unclear, ask
    if not _has_obvious_yield(ingredients):
        missing.append("yield")

    missing.append("station")  # default ask unless you later infer

    pending["missing"] = missing
    context.user_data["pending_ops"] = pending

    return _ask_next_missing(context)


def _handle_pending(text: str, pending: Dict[str, Any], context) -> OpsResult:
    ptype = pending.get("type")

    if ptype == "add_recipe" and pending.get("step") == "need_name":
        name = text.strip()
        context.user_data.pop("pending_ops", None)
        # restart with name
        return _start_add_recipe(f"add recipe\n{name}", context)

    if ptype == "add_recipe":
        draft_id = int(pending["draft_id"])
        missing: List[str] = list(pending.get("missing") or [])
        if not missing:
            context.user_data.pop("pending_ops", None)
            return OpsResult(True, "✅ Saved.")

        field = missing[0]

        if field == "yield":
            amt, unit = _parse_yield_answer(text)
            if amt is None or not unit:
                return OpsResult(True, "Yield? Example: `3 L` or `2 qt`")
            _update_recipe_fields(draft_id, {"yield_amount": amt, "yield_unit": unit})
            missing.pop(0)
            pending["missing"] = missing
            context.user_data["pending_ops"] = pending
            return _ask_next_missing(context)

        if field == "station":
            station = text.strip()
            # store best-effort
            _update_recipe_fields(draft_id, {"station": station})
            missing.pop(0)
            pending["missing"] = missing
            context.user_data["pending_ops"] = pending
            if missing:
                return _ask_next_missing(context)
            context.user_data.pop("pending_ops", None)
            return OpsResult(True, f"✅ Saved: {pending.get('name','recipe')}")

    # unknown pending
    context.user_data.pop("pending_ops", None)
    return OpsResult(False)


def _ask_next_missing(context) -> OpsResult:
    pending = context.user_data.get("pending_ops") or {}
    missing: List[str] = list(pending.get("missing") or [])
    if not missing:
        context.user_data.pop("pending_ops", None)
        return OpsResult(True, "✅ Saved.")

    nxt = missing[0]
    if nxt == "yield":
        return OpsResult(True, "Yield for this recipe? (Example: `3 L`, `2 qt`, `10 portions`)")
    if nxt == "station":
        return OpsResult(True, "Which station? (Prep / Hot / Cold / Pastry / Bar)")
    return OpsResult(True, "One more detail?")


def _parse_recipe_block(text: str) -> Tuple[Optional[str], List[Dict[str, Any]]]:
    lines = [ln.strip() for ln in text.splitlines()]
    lines = [ln for ln in lines if ln and not ln.lower().startswith("add the following recipe")]
    lines = [ln for ln in lines if not re.match(r"(?i)^add\s+recipe", ln)]

    if not lines:
        return None, []

    name = lines[0].strip(" :")
    ing_lines = lines[1:]

    ingredients = []
    for ln in ing_lines:
        parsed = _parse_ingredient_line(ln)
        if parsed:
            ingredients.append(parsed)
    return name, ingredients


def _parse_ingredient_line(line: str) -> Optional[Dict[str, Any]]:
    # supports: "60g Cumin seed", "3 L Grapeseed oil", "25g msg (only first batch)"
    rx = re.compile(r"^\s*(?P<num>\d+(\.\d+)?)\s*(?P<unit>[a-zA-Z#]+)\s+(?P<item>.+?)\s*$")
    m = rx.match(line)
    if not m:
        return None
    qty = float(m.group("num"))
    unit = m.group("unit").lower()
    item = m.group("item").strip()
    return {
        "inventory_item_id": None,
        "item_name_text": item,
        "quantity": qty,
        "unit": unit,
        "notes": None,
        "display_original": line.strip(),
    }


def _has_obvious_yield(ingredients: List[Dict[str, Any]]) -> bool:
    # if there is a large liquid amount like 3L, assume yield exists (still may be ambiguous)
    for ing in ingredients:
        u = (ing.get("unit") or "").lower()
        if u in {"l", "liter", "liters", "ml", "gal", "gallon", "qt", "quart"}:
            return True
    return False


def _parse_yield_answer(text: str) -> Tuple[Optional[float], Optional[str]]:
    rx = re.compile(r"^\s*(\d+(\.\d+)?)\s*([a-zA-Z#]+)\s*$")
    m = rx.match(text.strip())
    if not m:
        return None, None
    return float(m.group(1)), m.group(3).lower()


def _create_recipe_draft(name: str, ingredients: List[Dict[str, Any]]) -> int:
    con = _conn()
    cols = _table_columns(con, "recipes")
    recipe_fields: Dict[str, Any] = {"name": name}

    # prefer safe defaults
    if "is_active" in cols:
        recipe_fields["is_active"] = 1

    # choose instruction/method field
    method_col = (
        "method" if "method" in cols else ("instructions" if "instructions" in cols else None)
    )
    if method_col:
        recipe_fields[method_col] = ""

    # build insert dynamically
    keys = list(recipe_fields.keys())
    qmarks = ", ".join(["?"] * len(keys))
    sql = f"INSERT INTO recipes ({', '.join(keys)}) VALUES ({qmarks})"
    cur = con.execute(sql, tuple(recipe_fields[k] for k in keys))
    recipe_id = int(cur.lastrowid)

    # ingredients
    if "recipe_ingredients" in _existing_tables(con):
        ri_cols = _table_columns(con, "recipe_ingredients")
        for ing in ingredients:
            payload = {
                "recipe_id": recipe_id,
                "inventory_item_id": ing.get("inventory_item_id"),
                "item_name_text": ing.get("item_name_text"),
                "quantity": ing.get("quantity"),
                "unit": ing.get("unit"),
                "notes": ing.get("notes"),
            }
            # keep only supported columns
            payload = {k: v for k, v in payload.items() if k in ri_cols}
            keys2 = list(payload.keys())
            sql2 = f"INSERT INTO recipe_ingredients ({', '.join(keys2)}) VALUES ({', '.join(['?']*len(keys2))})"
            con.execute(sql2, tuple(payload[k] for k in keys2))
    else:
        # fallback: recipes.ingredients text/json if exists
        if "ingredients" in cols:
            blob = "\n".join([ing.get("display_original", "") for ing in ingredients]).strip()
            con.execute("UPDATE recipes SET ingredients=? WHERE id=?", (blob, recipe_id))

    con.commit()
    con.close()
    return recipe_id


def _update_recipe_fields(recipe_id: int, fields: Dict[str, Any]) -> None:
    con = _conn()
    cols = _table_columns(con, "recipes")

    # map generic keys to actual columns
    mapped: Dict[str, Any] = {}

    if "yield_amount" in cols and "yield_amount" in fields:
        mapped["yield_amount"] = fields["yield_amount"]
    if "yield_unit" in cols and "yield_unit" in fields:
        mapped["yield_unit"] = fields["yield_unit"]

    # station may be station_id or station text
    if "station" in cols and "station" in fields:
        mapped["station"] = fields["station"]
    elif "station_id" in cols and "station" in fields:
        # v1: store unresolved as NULL; later map station names -> ids
        mapped["station_id"] = None

    if not mapped:
        con.close()
        return

    set_sql = ", ".join([f"{k}=?" for k in mapped.keys()])
    con.execute(f"UPDATE recipes SET {set_sql} WHERE id=?", (*mapped.values(), recipe_id))
    con.commit()
    con.close()


def _existing_tables(con: sqlite3.Connection) -> set[str]:
    cur = con.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return {r[0] for r in cur.fetchall()}


# ---- update price/cost ----


def _update_recipe_money(kind: str, recipe_name: str, value: float) -> OpsResult:
    con = _conn()
    row = _best_recipe_match(con, recipe_name)
    if not row:
        con.close()
        return OpsResult(True, f"Can’t find recipe: {recipe_name}. Exact name?")

    cols = _table_columns(con, "recipes")
    rid = int(row["id"])
    rname = row["name"]

    if kind == "price":
        col = "sales_price" if "sales_price" in cols else ("selling_price" if "selling_price" in cols else None)
        if not col:
            con.close()
            return OpsResult(True, "No price column found in DB schema.")
        con.execute(f"UPDATE recipes SET {col}=? WHERE id=?", (value, rid))
        con.commit()
        con.close()
        return OpsResult(True, f"✅ {rname}: price set to ${value:.2f}")

    if kind == "cost":
        col = "cost" if "cost" in cols else None
        if not col:
            con.close()
            return OpsResult(True, "No cost column found in recipes table.")
        con.execute(f"UPDATE recipes SET {col}=? WHERE id=?", (value, rid))
        con.commit()
        con.close()
        return OpsResult(True, f"✅ {rname}: cost set to ${value:.2f}")

    con.close()
    return OpsResult(False)
