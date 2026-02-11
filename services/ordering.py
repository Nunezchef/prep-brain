import json
import logging
import re
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from services import brain, lexicon, mailer, memory
from services.invoice_ingest import normalize_item_name
from services.units import UnitNormalizationError, normalize_quantity

logger = logging.getLogger(__name__)

ORDER_HEAD_RE = re.compile(r"^(add|order|put)\s+(.+)$", re.IGNORECASE)
ORDER_QTY_FIRST_RE = re.compile(
    r"^(?P<qty>\d+(?:\.\d+)?)\s*(?P<unit>fl\s*oz|oz\s*fl|#|lb|lbs|kg|g|mg|ml|l|qt|pt|gal|oz|cs|case|cases|ea|each|pcs?|ct|doz)?\s+(?P<item>.+)$",
    re.IGNORECASE,
)
ORDER_QTY_LAST_RE = re.compile(
    r"^(?P<item>.+?)\s+(?P<qty>\d+(?:\.\d+)?)(?:\s+(?P<unit>fl\s*oz|oz\s*fl|#|lb|lbs|kg|g|mg|ml|l|qt|pt|gal|oz|cs|case|cases|ea|each|pcs?|ct|doz))?$",
    re.IGNORECASE,
)
ORDER_TRAIL_RE = re.compile(r"\bon\s+the\s+order\b", re.IGNORECASE)

UNIT_ALIASES = {
    "#": "lb",
    "lbs": "lb",
    "cases": "case",
    "ea": "each",
}


def _get_conn():
    return memory.get_conn()


def _clean_unit(value: Optional[str]) -> str:
    unit = (value or "unit").strip().lower()
    if not unit:
        unit = "unit"
    return UNIT_ALIASES.get(unit, unit)


def _quantity_display(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.3f}".rstrip("0").rstrip(".")


def is_order_intent_text(text: str) -> bool:
    cleaned = " ".join(str(text or "").split()).strip().lower()
    if not cleaned:
        return False
    if cleaned.startswith(("add ", "order ", "put ")):
        return True
    return bool(ORDER_TRAIL_RE.search(cleaned) and re.search(r"\d", cleaned))


def _normalize_payload(payload: str) -> str:
    compact = " ".join(payload.split()).strip()
    compact = ORDER_TRAIL_RE.sub("", compact).strip()
    return compact


def _parse_order_intent_rules(text: str, restaurant_tag: Optional[str] = None) -> Optional[Dict[str, Any]]:
    cleaned = " ".join(str(text or "").split()).strip()
    if not cleaned:
        return None

    head = ORDER_HEAD_RE.match(cleaned)
    if head:
        payload = _normalize_payload(head.group(2).strip())
    elif ORDER_TRAIL_RE.search(cleaned):
        payload = _normalize_payload(cleaned)
    else:
        return None

    match = ORDER_QTY_FIRST_RE.match(payload)
    if not match:
        match = ORDER_QTY_LAST_RE.match(payload)
    if not match:
        return None

    item_name = " ".join((match.group("item") or "").split()).strip()
    if not item_name:
        return None

    qty_raw = str(match.group("qty") or "").strip()
    unit_raw = _clean_unit(match.groupdict().get("unit"))
    if not qty_raw or not unit_raw:
        return None

    display_original = f"{qty_raw}{'#' if unit_raw == 'lb' and str(match.groupdict().get('unit') or '').strip() == '#' else f' {unit_raw}'}"
    try:
        normalized_qty = normalize_quantity(
            qty_raw,
            unit_raw,
            display_original=display_original,
            restaurant_tag=restaurant_tag,
        )
    except UnitNormalizationError:
        return None

    normalized = normalize_item_name(item_name)
    return {
        "quantity": float(normalized_qty["canonical_value"]),
        "unit": str(normalized_qty["canonical_unit"]),
        "input_quantity": float(normalized_qty["input_quantity"]),
        "input_unit": str(normalized_qty["normalized_unit"]),
        "display_original": str(normalized_qty["display_original"]),
        "display_pretty": str(normalized_qty["display_pretty"]),
        "item_name": item_name,
        "normalized_item_name": normalized,
        "raw_text": cleaned,
    }


def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    content = str(text or "").strip()
    if not content:
        return None
    if "```json" in content:
        content = content.split("```json", 1)[1].split("```", 1)[0].strip()
    elif "```" in content:
        content = content.split("```", 1)[1].split("```", 1)[0].strip()
    try:
        parsed = json.loads(content)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        pass
    start = content.find("{")
    end = content.rfind("}")
    if start >= 0 and end > start:
        try:
            parsed = json.loads(content[start : end + 1])
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None
    return None


def _parse_order_intent_ollama(text: str, restaurant_tag: Optional[str] = None) -> Optional[Dict[str, Any]]:
    # Reject prompts without numeric quantity to avoid quantity hallucination.
    if not re.search(r"\d", str(text or "")):
        return None

    alias_text = lexicon.replace_aliases_in_text(text, restaurant_tag=restaurant_tag)
    prompt = (
        "Extract structured ordering intent. Return JSON only.\n"
        "Schema: {\"action\":\"add_to_order\",\"item\":\"string\",\"qty\":\"number\",\"unit\":\"string\","
        "\"notes\":\"string|null\",\"vendor_hint\":\"string|null\"}\n"
        "Rules: never invent quantity; if unknown set null.\n"
        f"INPUT: {alias_text}"
    )
    parsed = _extract_json_object(brain.chat([("user", prompt)]))
    if not parsed:
        return None

    action = str(parsed.get("action") or "").strip().lower()
    if action not in {"add_to_order", "order"}:
        return None
    qty = parsed.get("qty")
    unit = str(parsed.get("unit") or "").strip()
    item_name = " ".join(str(parsed.get("item") or "").split()).strip()
    if qty in (None, "") or not unit or not item_name:
        return None

    try:
        normalized_qty = normalize_quantity(
            qty,
            unit,
            display_original=f"{qty} {unit}",
            restaurant_tag=restaurant_tag,
        )
    except UnitNormalizationError:
        return None

    return {
        "quantity": float(normalized_qty["canonical_value"]),
        "unit": str(normalized_qty["canonical_unit"]),
        "input_quantity": float(normalized_qty["input_quantity"]),
        "input_unit": str(normalized_qty["normalized_unit"]),
        "display_original": str(normalized_qty["display_original"]),
        "display_pretty": str(normalized_qty["display_pretty"]),
        "item_name": item_name,
        "normalized_item_name": normalize_item_name(item_name),
        "raw_text": " ".join(str(text or "").split()),
        "notes": str(parsed.get("notes") or "").strip() or None,
        "vendor_hint": str(parsed.get("vendor_hint") or "").strip() or None,
    }


def parse_order_text(text: str, restaurant_tag: Optional[str] = None) -> Optional[Dict[str, Any]]:
    parsed = _parse_order_intent_rules(text=text, restaurant_tag=restaurant_tag)
    if parsed:
        return parsed
    return _parse_order_intent_ollama(text=text, restaurant_tag=restaurant_tag)


def _get_vendor_row(con, vendor_id: int) -> Optional[Dict[str, Any]]:
    row = con.execute(
        "SELECT id, name, email, contact_name, cutoff_time, ordering_method FROM vendors WHERE id = ? LIMIT 1",
        (int(vendor_id),),
    ).fetchone()
    return dict(row) if row else None


def _all_vendor_options(con, limit: int = 5) -> List[Dict[str, Any]]:
    rows = con.execute(
        "SELECT id AS vendor_id, name AS vendor_name, 0.0 AS score FROM vendors ORDER BY name LIMIT ?",
        (int(limit),),
    ).fetchall()
    return [dict(row) for row in rows]


def _vendor_candidates_for_item(con, normalized_item_name: str, limit: int = 5) -> List[Dict[str, Any]]:
    if not normalized_item_name:
        return []

    rows = con.execute(
        """
        SELECT
            a.vendor_id,
            v.name AS vendor_name,
            a.score,
            a.purchase_count,
            a.last_seen_at
        FROM vendor_item_affinity a
        JOIN vendors v ON v.id = a.vendor_id
        WHERE a.normalized_item_name = ?
        ORDER BY a.score DESC, a.purchase_count DESC, a.last_seen_at DESC
        LIMIT ?
        """,
        (normalized_item_name, int(limit)),
    ).fetchall()

    if rows:
        return [dict(row) for row in rows]

    partial_rows = con.execute(
        """
        SELECT
            a.vendor_id,
            v.name AS vendor_name,
            a.score,
            a.purchase_count,
            a.last_seen_at
        FROM vendor_item_affinity a
        JOIN vendors v ON v.id = a.vendor_id
        WHERE a.normalized_item_name LIKE ?
        ORDER BY a.score DESC, a.purchase_count DESC, a.last_seen_at DESC
        LIMIT ?
        """,
        (f"%{normalized_item_name}%", int(limit)),
    ).fetchall()
    return [dict(row) for row in partial_rows]


def _is_clear_affinity_winner(candidates: List[Dict[str, Any]]) -> bool:
    if not candidates:
        return False
    if len(candidates) == 1:
        return True

    top = float(candidates[0].get("score") or 0.0)
    second = float(candidates[1].get("score") or 0.0)
    if top <= 0:
        return False
    if top - second >= 0.35:
        return True

    ratio = top / max(second, 0.0001)
    return ratio >= 1.25


def _set_chat_vendor_context(con, telegram_chat_id: int, vendor_id: int) -> None:
    con.execute(
        """
        INSERT INTO chat_vendor_context (telegram_chat_id, last_vendor_id, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(telegram_chat_id)
        DO UPDATE SET last_vendor_id = excluded.last_vendor_id, updated_at = CURRENT_TIMESTAMP
        """,
        (int(telegram_chat_id), int(vendor_id)),
    )


def resolve_vendor_for_item(
    *,
    normalized_item_name: str,
    telegram_chat_id: int,
    explicit_vendor_id: Optional[int] = None,
) -> Dict[str, Any]:
    con = _get_conn()
    try:
        if explicit_vendor_id is not None:
            vendor = _get_vendor_row(con, int(explicit_vendor_id))
            if not vendor:
                return {
                    "resolved": False,
                    "reason": "vendor_not_found",
                    "candidates": _all_vendor_options(con, limit=5),
                }
            return {
                "resolved": True,
                "vendor_id": int(vendor["id"]),
                "vendor_name": vendor["name"],
                "reason": "explicit_vendor",
                "candidates": [],
            }

        candidates = _vendor_candidates_for_item(con, normalized_item_name=normalized_item_name, limit=5)
        if candidates and _is_clear_affinity_winner(candidates):
            top = candidates[0]
            return {
                "resolved": True,
                "vendor_id": int(top["vendor_id"]),
                "vendor_name": top["vendor_name"],
                "reason": "vendor_item_affinity",
                "candidates": candidates,
            }

        if candidates:
            return {
                "resolved": False,
                "reason": "ambiguous_affinity",
                "candidates": candidates,
            }

        last_ctx = con.execute(
            "SELECT last_vendor_id FROM chat_vendor_context WHERE telegram_chat_id = ? LIMIT 1",
            (int(telegram_chat_id),),
        ).fetchone()
        if last_ctx and last_ctx["last_vendor_id"]:
            vendor = _get_vendor_row(con, int(last_ctx["last_vendor_id"]))
            if vendor:
                return {
                    "resolved": True,
                    "vendor_id": int(vendor["id"]),
                    "vendor_name": vendor["name"],
                    "reason": "chat_last_vendor",
                    "candidates": [],
                }

        return {
            "resolved": False,
            "reason": "no_vendor_match",
            "candidates": _all_vendor_options(con, limit=5),
        }
    finally:
        con.close()


def add_routed_order(
    *,
    telegram_chat_id: int,
    added_by: str,
    item_name: str,
    normalized_item_name: str,
    quantity: float,
    unit: str,
    canonical_value: Optional[float] = None,
    canonical_unit: Optional[str] = None,
    display_original: Optional[str] = None,
    display_pretty: Optional[str] = None,
    vendor_id: int,
) -> Dict[str, Any]:
    con = _get_conn()
    try:
        cur = con.execute(
            """
            INSERT INTO shopping_list (
                item_name,
                quantity,
                unit,
                added_by,
                vendor_id,
                normalized_item_name,
                canonical_value,
                canonical_unit,
                display_original,
                display_pretty,
                status,
                ordered_at,
                telegram_chat_id,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', NULL, ?, CURRENT_TIMESTAMP)
            """,
            (
                item_name,
                float(quantity),
                _clean_unit(unit),
                added_by,
                int(vendor_id),
                normalized_item_name or None,
                float(canonical_value if canonical_value is not None else quantity),
                str(canonical_unit or unit),
                str(display_original or f"{_quantity_display(float(quantity))} {unit}"),
                str(display_pretty or f"{_quantity_display(float(quantity))} {unit}"),
                int(telegram_chat_id),
            ),
        )
        _set_chat_vendor_context(con, int(telegram_chat_id), int(vendor_id))

        vendor = _get_vendor_row(con, int(vendor_id))
        con.commit()

        return {
            "ok": True,
            "shopping_item_id": int(cur.lastrowid),
            "vendor_id": int(vendor_id),
            "vendor_name": (vendor or {}).get("name", f"Vendor {vendor_id}"),
            "item_name": item_name,
            "quantity": float(quantity),
            "quantity_display": _quantity_display(float(quantity)),
            "unit": _clean_unit(unit),
            "display_original": str(display_original or ""),
            "display_pretty": str(display_pretty or ""),
        }
    finally:
        con.close()


def route_order_text(
    *,
    text: str,
    telegram_chat_id: int,
    added_by: str,
    explicit_vendor_id: Optional[int] = None,
    restaurant_tag: Optional[str] = None,
) -> Dict[str, Any]:
    parsed = parse_order_text(text=text, restaurant_tag=restaurant_tag)
    if not parsed:
        return {"ok": False, "error": "Could not parse order text."}

    routing = resolve_vendor_for_item(
        normalized_item_name=parsed["normalized_item_name"],
        telegram_chat_id=telegram_chat_id,
        explicit_vendor_id=explicit_vendor_id,
    )
    if not routing.get("resolved"):
        return {
            "ok": False,
            "needs_vendor": True,
            "reason": routing.get("reason"),
            "candidates": routing.get("candidates", []),
            "parsed": parsed,
        }

    created = add_routed_order(
        telegram_chat_id=telegram_chat_id,
        added_by=added_by,
        item_name=parsed["item_name"],
        normalized_item_name=parsed["normalized_item_name"],
        quantity=float(parsed["quantity"]),
        unit=parsed["unit"],
        canonical_value=float(parsed.get("quantity") or 0.0),
        canonical_unit=str(parsed.get("unit") or "each"),
        display_original=str(parsed.get("display_original") or ""),
        display_pretty=str(parsed.get("display_pretty") or ""),
        vendor_id=int(routing["vendor_id"]),
    )
    created["routing_reason"] = routing.get("reason")
    return created


def _pending_vendor_items(con, vendor_id: int) -> List[Dict[str, Any]]:
    rows = con.execute(
        """
        SELECT id, item_name, quantity, unit, display_original, created_at
        FROM shopping_list
        WHERE vendor_id = ? AND status = 'pending'
        ORDER BY created_at ASC, id ASC
        """,
        (int(vendor_id),),
    ).fetchall()
    return [dict(row) for row in rows]


def build_vendor_email_draft(vendor_id: int) -> Dict[str, Any]:
    con = _get_conn()
    try:
        vendor = _get_vendor_row(con, int(vendor_id))
        if not vendor:
            return {"ok": False, "error": "Vendor not found."}

        items = _pending_vendor_items(con, int(vendor_id))
        if not items:
            return {"ok": False, "error": "No pending items for this vendor."}

        today = datetime.now().strftime("%Y-%m-%d")
        subject = f"Order Request - {vendor['name']} - {today}"

        lines = [
            f"Hello {(vendor.get('contact_name') or vendor['name']).strip()},",
            "",
            "Please prepare the following order:",
            "",
        ]
        for item in items:
            display_original = str(item.get("display_original") or "").strip()
            if display_original:
                lines.append(f"- {display_original} {item.get('item_name')}")
            else:
                qty = _quantity_display(float(item.get("quantity") or 0.0))
                unit = _clean_unit(str(item.get("unit") or "unit"))
                lines.append(f"- {qty} {unit} {item.get('item_name')}")

        lines.extend(
            [
                "",
                "Please confirm availability and ETA.",
                "",
                "Thank you,",
                "Kitchen",
            ]
        )

        body = "\n".join(lines).strip()
        return {
            "ok": True,
            "vendor_id": int(vendor_id),
            "vendor_name": vendor["name"],
            "vendor_email": vendor.get("email"),
            "cutoff_time": vendor.get("cutoff_time"),
            "ordering_method": vendor.get("ordering_method"),
            "subject": subject,
            "body": body,
            "items": items,
            "items_count": len(items),
        }
    finally:
        con.close()


def send_vendor_draft(vendor_id: int) -> Dict[str, Any]:
    draft = build_vendor_email_draft(vendor_id)
    if not draft.get("ok"):
        return draft

    to_email = (draft.get("vendor_email") or "").strip()
    if to_email:
        result = mailer.send_email(to_email=to_email, subject=draft["subject"], body=draft["body"])
        if result.get("success"):
            con = _get_conn()
            try:
                con.execute(
                    """
                    UPDATE shopping_list
                    SET status = 'ordered', ordered_at = CURRENT_TIMESTAMP
                    WHERE vendor_id = ? AND status = 'pending'
                    """,
                    (int(vendor_id),),
                )
                con.commit()
            finally:
                con.close()
            return {
                "ok": True,
                "sent": True,
                "message": result.get("message", "Email sent."),
                "draft": draft,
            }

    preview = mailer.get_email_preview(
        to_email=to_email or "(vendor email not set)",
        subject=draft["subject"],
        body=draft["body"],
    )
    return {
        "ok": True,
        "sent": False,
        "message": "SMTP unavailable or vendor email missing.",
        "draft": draft,
        "preview": preview,
    }


def get_pending_orders(limit: int = 200) -> List[Dict[str, Any]]:
    con = _get_conn()
    try:
        rows = con.execute(
            """
            SELECT
                s.id,
                s.item_name,
                s.quantity,
                s.unit,
                s.display_original,
                s.display_pretty,
                s.vendor_id,
                s.status,
                s.created_at,
                v.name AS vendor_name
            FROM shopping_list s
            LEFT JOIN vendors v ON v.id = s.vendor_id
            WHERE s.status = 'pending'
            ORDER BY COALESCE(v.name, 'ZZZ'), s.item_name ASC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        con.close()


def _parse_time(value: str) -> Optional[time]:
    raw = " ".join(str(value or "").split()).strip().lower()
    if not raw:
        return None

    candidates = [raw, raw.replace(".", ":")]
    fmts = ["%H:%M", "%H:%M:%S", "%I:%M%p", "%I:%M %p", "%I%p"]
    for candidate in candidates:
        for fmt in fmts:
            try:
                return datetime.strptime(candidate, fmt).time()
            except ValueError:
                continue
    return None


def _is_in_quiet_hours(now: datetime, quiet_hours: Optional[Dict[str, str]]) -> bool:
    if not quiet_hours or not isinstance(quiet_hours, dict):
        return False

    start = _parse_time(str(quiet_hours.get("start") or ""))
    end = _parse_time(str(quiet_hours.get("end") or ""))
    if not start or not end:
        return False

    now_t = now.time()
    if start <= end:
        return start <= now_t < end
    return now_t >= start or now_t < end


def get_due_cutoff_reminders(
    *,
    reminder_offsets_minutes: List[int],
    quiet_hours: Optional[Dict[str, str]] = None,
    now: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    current = now or datetime.now()
    if _is_in_quiet_hours(current, quiet_hours):
        return []

    offsets = sorted({int(x) for x in reminder_offsets_minutes if int(x) >= 0}, reverse=True)
    if not offsets:
        return []

    con = _get_conn()
    try:
        vendors = con.execute(
            """
            SELECT id, name, cutoff_time
            FROM vendors
            WHERE cutoff_time IS NOT NULL AND TRIM(cutoff_time) != ''
            ORDER BY name
            """
        ).fetchall()

        reminders: List[Dict[str, Any]] = []
        reminder_date = current.date().isoformat()

        for vendor in vendors:
            vendor_id = int(vendor["id"])
            cutoff_t = _parse_time(vendor["cutoff_time"])
            if not cutoff_t:
                continue

            pending_count = con.execute(
                "SELECT COUNT(*) AS c FROM shopping_list WHERE vendor_id = ? AND status = 'pending'",
                (vendor_id,),
            ).fetchone()[0]
            if int(pending_count or 0) <= 0:
                continue

            cutoff_dt = datetime.combine(current.date(), cutoff_t)
            for offset in offsets:
                remind_at = cutoff_dt - timedelta(minutes=offset)
                if not (remind_at <= current < remind_at + timedelta(minutes=5)):
                    continue

                sent = con.execute(
                    """
                    SELECT 1 FROM vendor_cutoff_reminders
                    WHERE vendor_id = ? AND reminder_date = ? AND offset_minutes = ?
                    LIMIT 1
                    """,
                    (vendor_id, reminder_date, offset),
                ).fetchone()
                if sent:
                    continue

                reminders.append(
                    {
                        "vendor_id": vendor_id,
                        "vendor_name": vendor["name"],
                        "offset_minutes": offset,
                        "pending_count": int(pending_count or 0),
                        "cutoff_time": vendor["cutoff_time"],
                        "reminder_date": reminder_date,
                    }
                )

        return reminders
    finally:
        con.close()


def mark_cutoff_reminder_sent(vendor_id: int, reminder_date: str, offset_minutes: int) -> None:
    con = _get_conn()
    try:
        con.execute(
            """
            INSERT OR IGNORE INTO vendor_cutoff_reminders (vendor_id, reminder_date, offset_minutes, sent_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (int(vendor_id), str(reminder_date), int(offset_minutes)),
        )
        con.commit()
    finally:
        con.close()


def pending_chat_ids_for_vendor(vendor_id: int) -> List[int]:
    con = _get_conn()
    try:
        rows = con.execute(
            """
            SELECT DISTINCT telegram_chat_id
            FROM shopping_list
            WHERE vendor_id = ? AND status = 'pending' AND telegram_chat_id IS NOT NULL
            """,
            (int(vendor_id),),
        ).fetchall()
        ids: List[int] = []
        for row in rows:
            try:
                ids.append(int(row["telegram_chat_id"]))
            except Exception:
                continue
        return ids
    finally:
        con.close()


def forget_vendor(vendor_id: int, remove_files: bool = True) -> Dict[str, Any]:
    con = _get_conn()
    image_paths: List[str] = []
    try:
        rows = con.execute(
            "SELECT image_path FROM invoice_ingests WHERE vendor_id = ?",
            (int(vendor_id),),
        ).fetchall()
        image_paths = [str(row["image_path"] or "").strip() for row in rows if str(row["image_path"] or "").strip()]

        deleted_lines = con.execute(
            "DELETE FROM invoice_line_items WHERE invoice_ingest_id IN (SELECT id FROM invoice_ingests WHERE vendor_id = ?)",
            (int(vendor_id),),
        ).rowcount
        deleted_ingests = con.execute(
            "DELETE FROM invoice_ingests WHERE vendor_id = ?",
            (int(vendor_id),),
        ).rowcount
        deleted_affinity = con.execute(
            "DELETE FROM vendor_item_affinity WHERE vendor_id = ?",
            (int(vendor_id),),
        ).rowcount
        cleared_context = con.execute(
            "DELETE FROM chat_vendor_context WHERE last_vendor_id = ?",
            (int(vendor_id),),
        ).rowcount

        con.execute(
            "UPDATE shopping_list SET vendor_id = NULL WHERE vendor_id = ?",
            (int(vendor_id),),
        )
        deleted_vendor = con.execute("DELETE FROM vendors WHERE id = ?", (int(vendor_id),)).rowcount

        con.commit()
    finally:
        con.close()

    removed_files = 0
    if remove_files:
        for value in image_paths:
            try:
                path = Path(value)
                if path.exists():
                    path.unlink()
                    removed_files += 1
            except Exception:
                continue

    return {
        "ok": True,
        "vendor_id": int(vendor_id),
        "deleted_vendor": int(deleted_vendor or 0),
        "deleted_ingests": int(deleted_ingests or 0),
        "deleted_line_items": int(deleted_lines or 0),
        "deleted_affinity": int(deleted_affinity or 0),
        "cleared_chat_context": int(cleared_context or 0),
        "removed_files": int(removed_files),
    }
