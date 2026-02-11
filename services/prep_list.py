import json
import re
import sqlite3
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from services import memory
from services.units import UnitNormalizationError, normalize_quantity

STATUS_TODO = "todo"
STATUS_IN_PROGRESS = "in_progress"
STATUS_DONE = "done"

DONE_WORD_RE = re.compile(r"\b(done|finished|ready|complete|completed)\b", re.IGNORECASE)
HALF_WORD_RE = re.compile(r"\bhalf\b", re.IGNORECASE)
REST_WORD_RE = re.compile(r"\b(rest|all)\b", re.IGNORECASE)
STATION_RE = re.compile(r"\b([a-z][a-z0-9\s]{1,30})\s+station\b", re.IGNORECASE)
QTY_UNIT_RE = re.compile(
    r"\b(?P<qty>\d+(?:\.\d+)?)\s*(?P<unit>quarts?|qt|liters?|l|ml|gallons?|gal|pints?|pt|cups?|fl\s*oz|oz\s*fl|oz|kg|g|lb|lbs|#|ea|each|pan|hotel|half\s*pan|lexan|batch|batches)\b",
    re.IGNORECASE,
)

PAN_LIKE_UNITS = {"pan", "hotel", "half pan", "lexan", "batch", "batches"}
COUNT_LIKE_UNITS = {"ea", "each"}
MASS_VOLUME_UNITS = {
    "quart": "qt",
    "quarts": "qt",
    "qt": "qt",
    "liter": "l",
    "liters": "l",
    "l": "l",
    "ml": "ml",
    "gallon": "gal",
    "gallons": "gal",
    "gal": "gal",
    "pint": "pt",
    "pints": "pt",
    "pt": "pt",
    "cup": "cup",
    "cups": "cup",
    "fl oz": "fl oz",
    "oz fl": "fl oz",
    "oz": "oz",
    "kg": "kg",
    "g": "g",
    "lb": "lb",
    "lbs": "lb",
    "#": "lb",
}


def _load_config() -> Dict[str, Any]:
    try:
        with open("config.yaml", "r") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def _get_conn() -> sqlite3.Connection:
    return memory.get_conn()


def _normalize_text(value: str) -> str:
    lowered = (value or "").lower()
    cleaned = re.sub(r"[^a-z0-9\s]+", " ", lowered)
    return " ".join(cleaned.split()).strip()


def _is_privileged_role(role: str) -> bool:
    text = _normalize_text(role)
    return any(token in text for token in ("chef", "sous", "cdc", "head"))


def resolve_staff_context(*, telegram_chat_id: int, display_name: str) -> Dict[str, Any]:
    con = _get_conn()
    try:
        row = con.execute(
            "SELECT * FROM staff WHERE is_active = 1 AND telegram_chat_id = ? LIMIT 1",
            (int(telegram_chat_id),),
        ).fetchone()
        if row:
            role = str(row["role"] or "")
            return {
                "ok": True,
                "staff_id": int(row["id"]),
                "staff_name": str(row["name"]),
                "role": role,
                "is_privileged": _is_privileged_role(role),
            }

        normalized_actor = _normalize_text(display_name)
        if normalized_actor:
            rows = con.execute("SELECT * FROM staff WHERE is_active = 1").fetchall()
            scored: List[Tuple[float, sqlite3.Row]] = []
            for candidate in rows:
                candidate_name = _normalize_text(str(candidate["name"] or ""))
                if not candidate_name:
                    continue
                if candidate_name == normalized_actor:
                    score = 1.0
                elif candidate_name in normalized_actor or normalized_actor in candidate_name:
                    score = 0.92
                else:
                    score = SequenceMatcher(None, normalized_actor, candidate_name).ratio()
                if score >= 0.7:
                    scored.append((score, candidate))
            if scored:
                scored.sort(key=lambda item: item[0], reverse=True)
                winner = scored[0][1]
                role = str(winner["role"] or "")
                return {
                    "ok": True,
                    "staff_id": int(winner["id"]),
                    "staff_name": str(winner["name"]),
                    "role": role,
                    "is_privileged": _is_privileged_role(role),
                }

        # Fallback: user is not explicitly mapped in staff table.
        return {
            "ok": True,
            "staff_id": None,
            "staff_name": display_name,
            "role": "",
            "is_privileged": False,
        }
    finally:
        con.close()


def _station_unit_preferences() -> Dict[str, str]:
    cfg = _load_config()
    prep_cfg = cfg.get("prep_list", {}) if isinstance(cfg.get("prep_list"), dict) else {}
    raw = prep_cfg.get("station_display_units", {})
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, str] = {}
    for key, value in raw.items():
        station_name = _normalize_text(str(key))
        unit = str(value or "").strip().lower()
        if station_name and unit:
            out[station_name] = unit
    return out


def _ensure_station_id(con: sqlite3.Connection, station_name: str) -> Optional[int]:
    name = " ".join(str(station_name or "").split()).strip()
    if not name:
        return None
    row = con.execute("SELECT id FROM stations WHERE LOWER(name) = LOWER(?) LIMIT 1", (name,)).fetchone()
    if row:
        return int(row["id"])
    cur = con.execute("INSERT INTO stations (name, is_active) VALUES (?, 1)", (name,))
    return int(cur.lastrowid)


def _format_qty(value: float) -> str:
    numeric = float(value)
    if abs(numeric - round(numeric)) < 1e-9:
        return str(int(round(numeric)))
    return f"{numeric:.2f}".rstrip("0").rstrip(".")


def _convert_canonical_to_display(
    *,
    canonical_value: float,
    canonical_unit: str,
    display_unit: str,
) -> str:
    value = float(canonical_value or 0.0)
    target = str(display_unit or canonical_unit or "each").strip().lower()
    src = str(canonical_unit or "each").strip().lower()

    if target in {"qt", "quart", "quarts"} and src == "ml":
        return f"{_format_qty(value / 946.352946)} qt"
    if target in {"pt", "pint", "pints"} and src == "ml":
        return f"{_format_qty(value / 473.176473)} pt"
    if target in {"gal", "gallon", "gallons"} and src == "ml":
        return f"{_format_qty(value / 3785.411784)} gal"
    if target in {"l", "liter", "liters"} and src == "ml":
        return f"{_format_qty(value / 1000.0)} l"
    if target in {"kg"} and src == "g":
        return f"{_format_qty(value / 1000.0)} kg"
    if target in {"lb", "#", "lbs"} and src == "g":
        return f"{_format_qty(value / 453.59237)} lb"

    if src in {"g", "ml", "each"}:
        return f"{_format_qty(value)} {src}"
    return f"{_format_qty(value)} {src or 'each'}"


def _effective_status(
    *,
    status: str,
    completed_quantity: float,
    target_quantity: float,
) -> str:
    if str(status or "").lower() == STATUS_DONE:
        return STATUS_DONE
    if float(target_quantity or 0) > 0 and float(completed_quantity or 0) >= float(target_quantity or 0):
        return STATUS_DONE
    if float(completed_quantity or 0) > 0:
        return STATUS_IN_PROGRESS
    return STATUS_TODO


def _parse_quantity_phrase(text: str) -> Optional[Dict[str, Any]]:
    raw = str(text or "")
    match = QTY_UNIT_RE.search(raw)
    if match:
        qty_value = float(match.group("qty"))
        unit_raw = " ".join((match.group("unit") or "").split()).lower()
        if unit_raw in PAN_LIKE_UNITS:
            return {
                "canonical_value": qty_value,
                "canonical_unit": "each",
                "display_original": f"{_format_qty(qty_value)} {unit_raw}",
                "display_unit": unit_raw,
                "kind": "explicit",
            }
        normalized_unit = MASS_VOLUME_UNITS.get(unit_raw, unit_raw)
        if normalized_unit in COUNT_LIKE_UNITS:
            return {
                "canonical_value": qty_value,
                "canonical_unit": "each",
                "display_original": f"{_format_qty(qty_value)} {normalized_unit}",
                "display_unit": normalized_unit,
                "kind": "explicit",
            }
        try:
            normalized = normalize_quantity(
                qty_value,
                normalized_unit,
                display_original=f"{_format_qty(qty_value)} {unit_raw}",
            )
            return {
                "canonical_value": float(normalized["canonical_value"]),
                "canonical_unit": str(normalized["canonical_unit"]),
                "display_original": str(normalized["display_original"]),
                "display_unit": normalized_unit,
                "kind": "explicit",
            }
        except UnitNormalizationError:
            return None

    if HALF_WORD_RE.search(raw):
        return {"fraction": 0.5, "kind": "fraction_half"}
    if REST_WORD_RE.search(raw):
        return {"fraction": 1.0, "kind": "fraction_rest"}
    if DONE_WORD_RE.search(raw):
        return {"fraction": 1.0, "kind": "done_word"}
    return None


def _extract_station_hint(text: str) -> Optional[str]:
    match = STATION_RE.search(str(text or ""))
    if not match:
        return None
    return " ".join(match.group(1).split()).strip().title()


def _resolve_recipe_for_update(
    con: sqlite3.Connection,
    *,
    text: str,
    station_hint: Optional[str],
) -> Dict[str, Any]:
    text_norm = _normalize_text(text)
    rows = con.execute(
        """
        SELECT
            pli.id,
            pli.recipe_id,
            COALESCE(pli.target_quantity, pli.need_quantity, 0) AS target_quantity,
            COALESCE(pli.completed_quantity, 0) AS completed_quantity,
            COALESCE(pli.display_unit, pli.unit, r.yield_unit, 'each') AS display_unit,
            COALESCE(pli.status, 'todo') AS status,
            COALESCE(pli.hold_reason, '') AS hold_reason,
            r.name AS recipe_name,
            COALESCE(st.name, r.station, 'Unassigned') AS station_name
        FROM prep_list_items pli
        JOIN recipes r ON r.id = pli.recipe_id
        LEFT JOIN stations st ON st.id = pli.station_id
        WHERE COALESCE(pli.status, 'todo') <> 'done'
        ORDER BY pli.id DESC
        """
    ).fetchall()

    if not rows:
        return {"ok": False, "reason": "no_open_items"}

    scored: List[Tuple[float, sqlite3.Row]] = []
    for row in rows:
        recipe_name = str(row["recipe_name"] or "")
        recipe_norm = _normalize_text(recipe_name)
        if not recipe_norm:
            continue
        station_name = str(row["station_name"] or "")
        station_norm = _normalize_text(station_name)

        if station_hint and station_norm and station_norm != _normalize_text(station_hint):
            continue

        score = 0.0
        if recipe_norm in text_norm:
            score += 8.0

        recipe_tokens = [token for token in recipe_norm.split() if len(token) >= 3]
        overlap = sum(1 for token in recipe_tokens if token in text_norm)
        if recipe_tokens:
            score += 4.0 * (overlap / len(recipe_tokens))

        score += SequenceMatcher(None, text_norm, recipe_norm).ratio() * 2.5
        if station_hint and station_norm == _normalize_text(station_hint):
            score += 1.0

        if score > 0.9:
            scored.append((score, row))

    if not scored:
        return {"ok": False, "reason": "no_match"}

    scored.sort(key=lambda item: item[0], reverse=True)
    best_score, best = scored[0]
    if len(scored) > 1 and (best_score - scored[1][0]) < 0.4:
        candidates = [str(item[1]["recipe_name"]) for item in scored[:3]]
        return {"ok": False, "reason": "ambiguous", "candidates": candidates}

    return {"ok": True, "item": dict(best)}


def _update_item_progress(
    con: sqlite3.Connection,
    *,
    item: Dict[str, Any],
    delta_canonical: float,
    actor: str,
) -> Dict[str, Any]:
    item_id = int(item["id"])
    target = float(item.get("target_quantity") or 0.0)
    completed_prev = float(item.get("completed_quantity") or 0.0)
    completed_next = max(0.0, completed_prev + float(delta_canonical))
    if target > 0:
        completed_next = min(completed_next, target)

    status_next = _effective_status(
        status=str(item.get("status") or STATUS_TODO),
        completed_quantity=completed_next,
        target_quantity=target,
    )
    con.execute(
        """
        UPDATE prep_list_items
        SET completed_quantity = ?, status = ?, hold_reason = NULL,
            last_update_at = CURRENT_TIMESTAMP, last_update_by = ?
        WHERE id = ?
        """,
        (
            completed_next,
            status_next,
            actor[:120],
            item_id,
        ),
    )
    remaining = max(0.0, target - completed_next)
    return {
        "item_id": item_id,
        "status": status_next,
        "completed_quantity": completed_next,
        "remaining_quantity": remaining,
        "target_quantity": target,
    }


def is_prep_update_text(text: str) -> bool:
    raw = " ".join(str(text or "").split()).strip().lower()
    if not raw:
        return False
    if "/prep" in raw:
        return False
    if not (
        DONE_WORD_RE.search(raw)
        or "prepped" in raw
        or "prepping" in raw
        or "finished" in raw
        or "done with" in raw
    ):
        return False
    # Needs either a quantity hint, known phrasing, or "station ... done".
    return bool(QTY_UNIT_RE.search(raw) or HALF_WORD_RE.search(raw) or REST_WORD_RE.search(raw) or "station" in raw or "done with" in raw)


def process_natural_update(
    *,
    text: str,
    telegram_chat_id: int,
    telegram_user_id: int,
    display_name: str,
) -> Dict[str, Any]:
    del telegram_user_id  # Reserved for future fine-grained authorization.
    if not is_prep_update_text(text):
        return {"handled": False}

    con = _get_conn()
    try:
        station_hint = _extract_station_hint(text)
        resolved = _resolve_recipe_for_update(con, text=text, station_hint=station_hint)
        if not resolved.get("ok"):
            reason = resolved.get("reason")
            if reason == "ambiguous":
                candidates = resolved.get("candidates") or []
                return {
                    "handled": True,
                    "ok": False,
                    "message": f"Need recipe name: {', '.join(candidates[:3])}",
                    "reason": "ambiguous",
                }
            return {
                "handled": True,
                "ok": False,
                "message": "Could not match that prep item.",
                "reason": str(reason or "no_match"),
            }

        item = dict(resolved["item"])
        target = float(item.get("target_quantity") or 0.0)
        completed = float(item.get("completed_quantity") or 0.0)
        remaining = max(0.0, target - completed)
        qty_info = _parse_quantity_phrase(text)

        if qty_info and "canonical_value" in qty_info:
            delta = float(qty_info["canonical_value"])
            delta_unit = str(qty_info["canonical_unit"])
            # Align to item canonical domain when target is known.
            if remaining > 0 and delta_unit == "each" and str(item.get("display_unit") or "").lower() in {"qt", "quart", "quarts"}:
                # quantity unit mismatch; keep conservative and fail closed.
                return {
                    "handled": True,
                    "ok": False,
                    "message": "Quantity unit mismatch for this prep item.",
                    "reason": "unit_mismatch",
                }
        else:
            fraction = 1.0
            if qty_info and "fraction" in qty_info:
                fraction = float(qty_info["fraction"])
            if remaining <= 0:
                delta = 0.0
            else:
                delta = max(0.0, remaining * fraction)
            delta_unit = "auto"

        updated = _update_item_progress(
            con,
            item=item,
            delta_canonical=delta,
            actor=display_name,
        )
        con.commit()

        display_unit = str(item.get("display_unit") or "each").strip().lower()
        if qty_info and qty_info.get("display_original"):
            qty_display = str(qty_info["display_original"])
        elif delta > 0 and display_unit:
            qty_display = _convert_canonical_to_display(
                canonical_value=delta,
                canonical_unit="ml" if display_unit in {"qt", "quart", "quarts", "pt", "pint", "pints", "gal", "gallon", "gallons", "l"} else ("each" if display_unit in PAN_LIKE_UNITS or display_unit in COUNT_LIKE_UNITS else "g"),
                display_unit=display_unit,
            )
        else:
            qty_display = "updated"

        station_name = str(item.get("station_name") or "Unassigned")
        recipe_name = str(item.get("recipe_name") or "Prep item")
        return {
            "handled": True,
            "ok": True,
            "message": f"✓ {recipe_name} updated — {qty_display} complete ({station_name})",
            "item_id": int(item["id"]),
            "status": updated["status"],
            "delta": delta,
            "delta_unit": delta_unit,
        }
    finally:
        con.close()


def _resolve_recipe(con: sqlite3.Connection, recipe_name: str) -> Optional[Dict[str, Any]]:
    name = " ".join(str(recipe_name or "").split()).strip()
    if not name:
        return None
    exact = con.execute(
        "SELECT id, name, yield_unit, station FROM recipes WHERE LOWER(name) = LOWER(?) LIMIT 1",
        (name,),
    ).fetchone()
    if exact:
        return dict(exact)
    rows = con.execute(
        "SELECT id, name, yield_unit, station FROM recipes WHERE LOWER(name) LIKE ? ORDER BY name LIMIT 20",
        (f"%{name.lower()}%",),
    ).fetchall()
    if not rows:
        return None
    scored: List[Tuple[float, sqlite3.Row]] = []
    target = _normalize_text(name)
    for row in rows:
        candidate = _normalize_text(str(row["name"] or ""))
        score = SequenceMatcher(None, target, candidate).ratio()
        if target in candidate:
            score += 0.4
        scored.append((score, row))
    scored.sort(key=lambda item: item[0], reverse=True)
    if not scored:
        return None
    return dict(scored[0][1])


def add_item(
    *,
    recipe_name: str,
    qty: Any,
    unit: str,
    actor: str,
) -> Dict[str, Any]:
    con = _get_conn()
    try:
        recipe = _resolve_recipe(con, recipe_name)
        if not recipe:
            return {"ok": False, "error": "Recipe not found."}

        station_name = str(recipe.get("station") or "Unassigned").strip() or "Unassigned"
        station_id = _ensure_station_id(con, station_name)
        prefs = _station_unit_preferences()
        preferred = prefs.get(_normalize_text(station_name))
        display_unit = preferred or str(unit or recipe.get("yield_unit") or "each").strip().lower()

        canonical_unit = "each"
        canonical_value: float
        normalized_display = f"{qty} {unit}"
        if display_unit in PAN_LIKE_UNITS:
            canonical_value = float(qty)
            canonical_unit = "each"
        else:
            parse_unit = MASS_VOLUME_UNITS.get(str(unit).strip().lower(), str(unit).strip().lower())
            if parse_unit in COUNT_LIKE_UNITS:
                canonical_value = float(qty)
                canonical_unit = "each"
            else:
                try:
                    normalized = normalize_quantity(qty, parse_unit, display_original=normalized_display)
                    canonical_value = float(normalized["canonical_value"])
                    canonical_unit = str(normalized["canonical_unit"])
                    normalized_display = str(normalized["display_original"])
                except UnitNormalizationError as exc:
                    return {"ok": False, "error": str(exc)}

        existing = con.execute(
            """
            SELECT id, COALESCE(target_quantity, need_quantity, 0) AS target_quantity
            FROM prep_list_items
            WHERE recipe_id = ? AND COALESCE(status,'todo') <> 'done'
            ORDER BY id DESC LIMIT 1
            """,
            (int(recipe["id"]),),
        ).fetchone()
        if existing:
            target_next = float(existing["target_quantity"] or 0.0) + canonical_value
            con.execute(
                """
                UPDATE prep_list_items
                SET target_quantity = ?, need_quantity = ?, display_unit = ?, station_id = ?,
                    last_update_at = CURRENT_TIMESTAMP, last_update_by = ?, status = ?, hold_reason = NULL
                WHERE id = ?
                """,
                (
                    target_next,
                    target_next,
                    display_unit,
                    station_id,
                    actor[:120],
                    STATUS_TODO,
                    int(existing["id"]),
                ),
            )
            item_id = int(existing["id"])
        else:
            cur = con.execute(
                """
                INSERT INTO prep_list_items (
                    recipe_id, need_quantity, target_quantity, completed_quantity,
                    unit, display_unit, station_id, status, last_update_at, last_update_by
                ) VALUES (?, ?, ?, 0, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                """,
                (
                    int(recipe["id"]),
                    canonical_value,
                    canonical_value,
                    canonical_unit,
                    display_unit,
                    station_id,
                    STATUS_TODO,
                    actor[:120],
                ),
            )
            item_id = int(cur.lastrowid)

        con.commit()
        return {
            "ok": True,
            "item_id": item_id,
            "recipe_name": str(recipe["name"]),
            "station_name": station_name,
            "display_original": normalized_display,
        }
    finally:
        con.close()


def mark_done(*, item_id: int, actor: str) -> Dict[str, Any]:
    con = _get_conn()
    try:
        row = con.execute(
            """
            SELECT id, COALESCE(target_quantity, need_quantity, 0) AS target_quantity
            FROM prep_list_items
            WHERE id = ?
            """,
            (int(item_id),),
        ).fetchone()
        if not row:
            return {"ok": False, "error": "Prep item not found."}
        target = float(row["target_quantity"] or 0.0)
        con.execute(
            """
            UPDATE prep_list_items
            SET status = 'done', completed_quantity = ?, hold_reason = NULL,
                last_update_at = CURRENT_TIMESTAMP, last_update_by = ?
            WHERE id = ?
            """,
            (target, actor[:120], int(item_id)),
        )
        con.commit()
        return {"ok": True}
    finally:
        con.close()


def hold_item(*, item_id: int, actor: str, reason: Optional[str]) -> Dict[str, Any]:
    con = _get_conn()
    try:
        cur = con.execute(
            """
            UPDATE prep_list_items
            SET hold_reason = ?, status = ?, last_update_at = CURRENT_TIMESTAMP, last_update_by = ?
            WHERE id = ?
            """,
            (
                " ".join(str(reason or "on hold").split())[:300],
                STATUS_TODO,
                actor[:120],
                int(item_id),
            ),
        )
        con.commit()
        if cur.rowcount <= 0:
            return {"ok": False, "error": "Prep item not found."}
        return {"ok": True}
    finally:
        con.close()


def assign_item(*, item_id: int, staff_name: str, actor: str) -> Dict[str, Any]:
    con = _get_conn()
    try:
        target = _normalize_text(staff_name)
        rows = con.execute("SELECT id, name, role FROM staff WHERE is_active = 1 ORDER BY name").fetchall()
        if not rows:
            return {"ok": False, "error": "No active staff found."}
        scored: List[Tuple[float, sqlite3.Row]] = []
        for row in rows:
            candidate = _normalize_text(str(row["name"] or ""))
            if not candidate:
                continue
            score = SequenceMatcher(None, target, candidate).ratio()
            if target in candidate or candidate in target:
                score += 0.35
            scored.append((score, row))
        scored.sort(key=lambda item: item[0], reverse=True)
        if not scored or scored[0][0] < 0.62:
            return {"ok": False, "error": "Staff not found."}
        winner = scored[0][1]
        cur = con.execute(
            """
            UPDATE prep_list_items
            SET assigned_staff_id = ?, last_update_at = CURRENT_TIMESTAMP, last_update_by = ?
            WHERE id = ?
            """,
            (int(winner["id"]), actor[:120], int(item_id)),
        )
        con.commit()
        if cur.rowcount <= 0:
            return {"ok": False, "error": "Prep item not found."}
        return {"ok": True, "staff_name": str(winner["name"])}
    finally:
        con.close()


def get_items(*, station_name: Optional[str] = None, include_done: bool = True) -> List[Dict[str, Any]]:
    con = _get_conn()
    try:
        params: List[Any] = []
        where_parts = ["1=1"]
        if not include_done:
            where_parts.append("COALESCE(pli.status, 'todo') <> 'done'")
        if station_name:
            where_parts.append("LOWER(COALESCE(st.name, r.station, 'Unassigned')) = LOWER(?)")
            params.append(" ".join(station_name.split()))
        rows = con.execute(
            f"""
            SELECT
                pli.id,
                pli.recipe_id,
                r.name AS recipe_name,
                COALESCE(st.name, r.station, 'Unassigned') AS station_name,
                pli.station_id,
                COALESCE(pli.target_quantity, pli.need_quantity, 0) AS target_quantity,
                COALESCE(pli.completed_quantity, 0) AS completed_quantity,
                COALESCE(pli.display_unit, pli.unit, r.yield_unit, 'each') AS display_unit,
                COALESCE(pli.status, 'todo') AS status,
                pli.assigned_staff_id,
                s.name AS assigned_staff_name,
                pli.last_update_at,
                pli.last_update_by,
                COALESCE(pli.hold_reason, '') AS hold_reason
            FROM prep_list_items pli
            JOIN recipes r ON r.id = pli.recipe_id
            LEFT JOIN stations st ON st.id = pli.station_id
            LEFT JOIN staff s ON s.id = pli.assigned_staff_id
            WHERE {" AND ".join(where_parts)}
            ORDER BY LOWER(COALESCE(st.name, r.station, 'Unassigned')), r.name, pli.id DESC
            """,
            tuple(params),
        ).fetchall()
        items = [dict(row) for row in rows]
        for item in items:
            target = float(item.get("target_quantity") or 0.0)
            completed = float(item.get("completed_quantity") or 0.0)
            item["status"] = _effective_status(
                status=str(item.get("status") or STATUS_TODO),
                completed_quantity=completed,
                target_quantity=target,
            )
            remaining = max(0.0, target - completed)
            item["remaining_quantity"] = remaining
            item["remaining_display"] = _convert_canonical_to_display(
                canonical_value=remaining,
                canonical_unit="ml" if str(item.get("display_unit") or "").lower() in {"qt", "quart", "quarts", "pt", "pint", "pints", "gal", "gallon", "gallons", "l"} else ("each" if str(item.get("display_unit") or "").lower() in PAN_LIKE_UNITS or str(item.get("display_unit") or "").lower() in COUNT_LIKE_UNITS else "g"),
                display_unit=str(item.get("display_unit") or ""),
            )
        return items
    finally:
        con.close()


def grouped_by_station(*, station_name: Optional[str] = None, include_done: bool = True) -> List[Dict[str, Any]]:
    items = get_items(station_name=station_name, include_done=include_done)
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for item in items:
        key = str(item.get("station_name") or "Unassigned")
        grouped.setdefault(key, []).append(item)
    ordered = []
    for station in sorted(grouped.keys(), key=lambda s: s.lower()):
        ordered.append({"station_name": station, "items": grouped[station]})
    return ordered


def summary_by_station() -> List[Dict[str, Any]]:
    grouped = grouped_by_station(include_done=True)
    summary: List[Dict[str, Any]] = []
    for bucket in grouped:
        open_count = 0
        done_count = 0
        for item in bucket["items"]:
            status = str(item.get("status") or STATUS_TODO)
            if status == STATUS_DONE:
                done_count += 1
            else:
                open_count += 1
        summary.append(
            {
                "station_name": bucket["station_name"],
                "open_count": open_count,
                "done_count": done_count,
            }
        )
    return summary


def auto_generate_if_empty() -> Dict[str, Any]:
    con = _get_conn()
    try:
        open_count = int(
            con.execute(
                "SELECT COUNT(*) FROM prep_list_items WHERE COALESCE(status,'todo') <> 'done'"
            ).fetchone()[0]
        )
        if open_count > 0:
            return {"generated": 0, "open_count": open_count}

        rows = con.execute(
            """
            SELECT r.id, r.name, r.station, COALESCE(r.par_level, 0) AS par_level,
                   COALESCE(r.yield_unit, 'each') AS yield_unit
            FROM recipes r
            WHERE r.is_active = 1 AND COALESCE(r.par_level, 0) > 0
            ORDER BY r.name
            """
        ).fetchall()

        generated = 0
        prefs = _station_unit_preferences()
        for row in rows:
            station_name = str(row["station"] or "Unassigned").strip() or "Unassigned"
            station_id = _ensure_station_id(con, station_name)
            display_unit = prefs.get(_normalize_text(station_name)) or str(row["yield_unit"] or "each").strip().lower()

            par_level = float(row["par_level"] or 0.0)
            if par_level <= 0:
                continue

            canonical_value = par_level
            canonical_unit = "each"
            if display_unit not in PAN_LIKE_UNITS and display_unit not in COUNT_LIKE_UNITS:
                try:
                    normalized = normalize_quantity(par_level, MASS_VOLUME_UNITS.get(display_unit, display_unit), display_original=f"{par_level} {display_unit}")
                    canonical_value = float(normalized["canonical_value"])
                    canonical_unit = str(normalized["canonical_unit"])
                except UnitNormalizationError:
                    canonical_value = par_level
                    canonical_unit = "each"

            con.execute(
                """
                INSERT INTO prep_list_items (
                    recipe_id, need_quantity, target_quantity, completed_quantity, unit, display_unit,
                    station_id, status, last_update_at, last_update_by
                ) VALUES (?, ?, ?, 0, ?, ?, ?, 'todo', CURRENT_TIMESTAMP, 'autonomy')
                """,
                (
                    int(row["id"]),
                    canonical_value,
                    canonical_value,
                    canonical_unit,
                    display_unit,
                    station_id,
                ),
            )
            generated += 1

        con.commit()
        return {"generated": generated, "open_count": generated}
    finally:
        con.close()


def behind_service_snapshot() -> Dict[str, Any]:
    items = get_items(include_done=False)
    behind = [item for item in items if str(item.get("status") or STATUS_TODO) in {STATUS_TODO, STATUS_IN_PROGRESS}]
    return {
        "open_items": len(behind),
        "stations": len({str(item.get("station_name") or "Unassigned") for item in behind}),
    }

