import json
import logging
import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from services import memory
from services.command_runner import CommandRunner
from services.units import UnitNormalizationError, normalize_quantity

logger = logging.getLogger(__name__)

QTY_UNIT_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(#|lb|lbs|kg|g|oz|cs|case|ea|ct|doz)?\b", re.I)
MONEY_RE = re.compile(r"\$?\s*(\d{1,5}(?:,\d{3})*(?:\.\d{1,2})?)")
INVOICE_NUMBER_RE = re.compile(r"(?i)\b(?:invoice|inv|bill)\s*(?:#|no|num|number)?\s*[:\-]?\s*([a-z0-9\-]{3,})\b")
IGNORED_LINE_RE = re.compile(
    r"(?i)\b(subtotal|tax|balance|amount due|total due|cash|change|visa|mastercard|thank you)\b"
)
ITEM_CLEAN_RE = re.compile(r"[^a-z0-9 ]+")

COMMAND_RUNNER = CommandRunner(allowed_commands={"tesseract"})


def _get_conn() -> sqlite3.Connection:
    return memory.get_conn()


def normalize_item_name(text: str) -> str:
    value = (text or "").lower()
    value = ITEM_CLEAN_RE.sub(" ", value)
    tokens = []
    for token in value.split():
        if token in {"lb", "lbs", "kg", "g", "oz", "case", "cs", "ea", "ct", "doz"}:
            continue
        if token.isdigit():
            continue
        if len(token) > 4 and token.endswith("es"):
            token = token[:-2]
        elif len(token) > 3 and token.endswith("s"):
            token = token[:-1]
        tokens.append(token)
    return " ".join(tokens).strip()


def _extract_domain(value: Optional[str]) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if "@" in raw:
        return raw.split("@", 1)[1].lower().strip()
    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    return (parsed.netloc or parsed.path).lower().strip().lstrip("www.")


def _ocr_tesseract(image_path: str) -> str:
    result = COMMAND_RUNNER.run(
        ["tesseract", image_path, "stdout"],
        check=True,
        capture_output=True,
        text=True,
    )
    return (result.stdout or "").strip()


def _guess_vendors(raw_text: str, vendors: List[Dict[str, Any]], top_k: int = 5) -> List[Dict[str, Any]]:
    text = (raw_text or "").lower()
    scored: List[Dict[str, Any]] = []

    for vendor in vendors:
        score = 0.0
        name = (vendor.get("name") or "").strip().lower()
        if not name:
            continue

        if name in text:
            score += 0.75

        for token in [p for p in re.split(r"\W+", name) if len(p) >= 3]:
            if token in text:
                score += 0.12

        email_domain = _extract_domain(vendor.get("email"))
        website_domain = _extract_domain(vendor.get("website"))
        for domain in {email_domain, website_domain}:
            if domain and domain in text:
                score += 0.55

        if score > 0:
            scored.append(
                {
                    "vendor_id": int(vendor["id"]),
                    "vendor_name": vendor["name"],
                    "confidence": min(score, 0.99),
                }
            )

    scored.sort(key=lambda row: row["confidence"], reverse=True)
    return scored[:top_k]


def _extract_item_from_line(line: str) -> str:
    cleaned = MONEY_RE.sub(" ", line)
    cleaned = re.sub(r"\b\d+(?:\.\d+)?\b", " ", cleaned)
    cleaned = ITEM_CLEAN_RE.sub(" ", cleaned)
    cleaned = " ".join(cleaned.split())
    return cleaned.strip()


def parse_invoice_line_items(raw_text: str) -> List[Dict[str, Any]]:
    line_items: List[Dict[str, Any]] = []
    for raw_line in (raw_text or "").splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        if len(line) < 4:
            continue
        if IGNORED_LINE_RE.search(line):
            continue
        if not re.search(r"[A-Za-z]", line):
            continue

        qty: Optional[float] = None
        unit: Optional[str] = None
        canonical_value: Optional[float] = None
        canonical_unit: Optional[str] = None
        display_original: Optional[str] = None
        display_pretty: Optional[str] = None
        qty_match = QTY_UNIT_RE.search(line)
        if qty_match:
            try:
                qty = float(qty_match.group(1))
            except ValueError:
                qty = None
            unit = (qty_match.group(2) or "").lower() or None
            if qty is not None and unit:
                try:
                    normalized_qty = normalize_quantity(
                        qty,
                        unit,
                        display_original=f"{qty:g}{unit if unit == '#' else f' {unit}'}",
                    )
                    canonical_value = float(normalized_qty["canonical_value"])
                    canonical_unit = str(normalized_qty["canonical_unit"])
                    display_original = str(normalized_qty["display_original"])
                    display_pretty = str(normalized_qty["display_pretty"])
                    qty = float(normalized_qty["canonical_value"])
                    unit = str(normalized_qty["canonical_unit"])
                except UnitNormalizationError:
                    pass

        money_matches = MONEY_RE.findall(line)
        unit_cost: Optional[float] = None
        total_cost: Optional[float] = None
        parsed_money: List[float] = []
        for value in money_matches:
            try:
                parsed_money.append(float(value.replace(",", "")))
            except ValueError:
                continue

        if len(parsed_money) >= 2:
            unit_cost = parsed_money[-2]
            total_cost = parsed_money[-1]
        elif len(parsed_money) == 1:
            total_cost = parsed_money[0]
            if qty and qty > 0:
                unit_cost = round(total_cost / qty, 4)

        item_guess = _extract_item_from_line(line)
        normalized = normalize_item_name(item_guess)
        if not normalized and (qty is None and total_cost is None):
            continue

        line_items.append(
            {
                "normalized_item_name": normalized or None,
                "raw_line_text": line,
                "quantity": qty,
                "unit": unit,
                "canonical_value": canonical_value if canonical_value is not None else qty,
                "canonical_unit": canonical_unit if canonical_unit is not None else unit,
                "display_original": display_original,
                "display_pretty": display_pretty,
                "unit_cost": unit_cost,
                "total_cost": total_cost,
            }
        )

    return line_items


def _extract_invoice_number(raw_text: str) -> Optional[str]:
    match = INVOICE_NUMBER_RE.search(raw_text or "")
    if not match:
        return None
    return (match.group(1) or "").strip() or None


def _match_inventory_item_id(con: sqlite3.Connection, normalized_name: Optional[str]) -> Optional[int]:
    if not normalized_name:
        return None
    rows = con.execute("SELECT id, name FROM inventory_items").fetchall()
    for row in rows:
        name_norm = normalize_item_name(row["name"])
        if name_norm and name_norm == normalized_name:
            return int(row["id"])
    for row in rows:
        name_norm = normalize_item_name(row["name"])
        if name_norm and (normalized_name in name_norm or name_norm in normalized_name):
            return int(row["id"])
    return None


def _match_vendor_item_id(
    con: sqlite3.Connection, vendor_id: Optional[int], normalized_name: Optional[str]
) -> Optional[int]:
    if not vendor_id or not normalized_name:
        return None
    rows = con.execute("SELECT id, name FROM vendor_items WHERE vendor_id = ?", (vendor_id,)).fetchall()
    for row in rows:
        if normalize_item_name(row["name"]) == normalized_name:
            return int(row["id"])
    for row in rows:
        name_norm = normalize_item_name(row["name"])
        if name_norm and (normalized_name in name_norm or name_norm in normalized_name):
            return int(row["id"])
    return None


def _update_vendor_item_affinity(con: sqlite3.Connection, vendor_id: int, normalized_item_name: str) -> None:
    if not normalized_item_name:
        return
    row = con.execute(
        """
        SELECT id, purchase_count, score
        FROM vendor_item_affinity
        WHERE normalized_item_name = ? AND vendor_id = ?
        """,
        (normalized_item_name, vendor_id),
    ).fetchone()
    if row:
        purchase_count = int(row["purchase_count"] or 0) + 1
        old_score = float(row["score"] or 0.0)
        new_score = round((old_score * 0.85) + 1.0, 4)
        con.execute(
            """
            UPDATE vendor_item_affinity
            SET purchase_count = ?, score = ?, last_seen_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (purchase_count, new_score, int(row["id"])),
        )
    else:
        con.execute(
            """
            INSERT INTO vendor_item_affinity (normalized_item_name, vendor_id, purchase_count, score, last_seen_at)
            VALUES (?, ?, 1, 1.0, CURRENT_TIMESTAMP)
            """,
            (normalized_item_name, vendor_id),
        )


def _upsert_vendor_item_catalog(
    con: sqlite3.Connection, vendor_id: int, normalized_item_name: str, unit: Optional[str], unit_cost: Optional[float]
) -> None:
    if not normalized_item_name:
        return
    title_name = normalized_item_name.title()
    existing = con.execute(
        "SELECT id FROM vendor_items WHERE vendor_id = ? AND LOWER(name) = LOWER(?) LIMIT 1",
        (vendor_id, title_name),
    ).fetchone()
    if existing:
        if unit_cost is not None or unit:
            con.execute(
                "UPDATE vendor_items SET unit = COALESCE(?, unit), price = COALESCE(?, price), updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (unit, unit_cost, int(existing["id"])),
            )
    else:
        con.execute(
            """
            INSERT INTO vendor_items (vendor_id, name, unit, price, is_active, created_at, updated_at)
            VALUES (?, ?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (vendor_id, title_name, unit, unit_cost),
        )


def _upsert_order_guide_item(
    con: sqlite3.Connection, vendor_id: int, normalized_item_name: str, unit_cost: Optional[float]
) -> None:
    if not normalized_item_name:
        return
    title_name = normalized_item_name.title()
    row = con.execute(
        "SELECT id FROM order_guide_items WHERE vendor_id = ? AND LOWER(item_name) = LOWER(?) LIMIT 1",
        (vendor_id, title_name),
    ).fetchone()
    if row:
        if unit_cost is not None:
            con.execute(
                "UPDATE order_guide_items SET price = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (unit_cost, int(row["id"])),
            )
    else:
        con.execute(
            """
            INSERT INTO order_guide_items (vendor_id, item_name, price, is_active, updated_at)
            VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP)
            """,
            (vendor_id, title_name, unit_cost or 0.0),
        )


def _insert_receiving_from_lines(
    con: sqlite3.Connection,
    vendor_id: int,
    invoice_number: Optional[str],
    line_items: List[Dict[str, Any]],
) -> None:
    for item in line_items:
        if not item.get("normalized_item_name"):
            continue
        con.execute(
            """
            INSERT INTO receiving_log (
                vendor_id, invoice_number, item_name, quantity_received, unit, unit_cost, total_cost,
                quality_ok, notes, received_by, received_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, 'invoice_ingest', CURRENT_TIMESTAMP)
            """,
            (
                vendor_id,
                invoice_number,
                item["normalized_item_name"].title(),
                item.get("canonical_value", item.get("quantity")),
                item.get("canonical_unit", item.get("unit")),
                item.get("unit_cost"),
                item.get("total_cost"),
                item.get("raw_line_text"),
            ),
        )


def _apply_vendor_learning(con: sqlite3.Connection, invoice_ingest_id: int, vendor_id: int) -> None:
    rows = con.execute(
        """
        SELECT id, normalized_item_name, unit, unit_cost
        FROM invoice_line_items
        WHERE invoice_ingest_id = ?
        """,
        (invoice_ingest_id,),
    ).fetchall()
    for row in rows:
        normalized = (row["normalized_item_name"] or "").strip()
        if not normalized:
            continue
        _update_vendor_item_affinity(con, vendor_id, normalized)
        _upsert_vendor_item_catalog(con, vendor_id, normalized, row["unit"], row["unit_cost"])
        _upsert_order_guide_item(con, vendor_id, normalized, row["unit_cost"])
        vendor_item_id = _match_vendor_item_id(con, vendor_id, normalized)
        if vendor_item_id:
            con.execute(
                "UPDATE invoice_line_items SET matched_vendor_item_id = ? WHERE id = ?",
                (vendor_item_id, int(row["id"])),
            )


def ingest_invoice_image(
    *,
    image_path: str,
    telegram_chat_id: int,
    telegram_user_id: int,
    vendor_confidence_threshold: float = 0.75,
) -> Dict[str, Any]:
    con = _get_conn()
    try:
        raw_text = _ocr_tesseract(image_path)
        line_items = parse_invoice_line_items(raw_text)
        invoice_number = _extract_invoice_number(raw_text)

        vendors = [dict(row) for row in con.execute("SELECT * FROM vendors ORDER BY name").fetchall()]
        candidates = _guess_vendors(raw_text=raw_text, vendors=vendors, top_k=5)
        best = candidates[0] if candidates else None
        vendor_id = int(best["vendor_id"]) if best and best["confidence"] >= vendor_confidence_threshold else None
        confidence = float(best["confidence"]) if best else 0.0
        vendor_guess_text = best["vendor_name"] if best else None
        status = "parsed" if vendor_id else "pending_vendor"

        cur = con.execute(
            """
            INSERT INTO invoice_ingests (
                vendor_id, vendor_guess_text, confidence, telegram_chat_id, telegram_user_id,
                image_path, raw_ocr_text, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                vendor_id,
                vendor_guess_text,
                confidence,
                telegram_chat_id,
                telegram_user_id,
                image_path,
                raw_text,
                status,
            ),
        )
        ingest_id = int(cur.lastrowid)

        for line in line_items:
            normalized_name = line.get("normalized_item_name")
            inventory_item_id = _match_inventory_item_id(con, normalized_name)
            matched_vendor_item_id = _match_vendor_item_id(con, vendor_id, normalized_name) if vendor_id else None
            con.execute(
                """
                INSERT INTO invoice_line_items (
                    invoice_ingest_id, normalized_item_name, raw_line_text, quantity, unit, canonical_value,
                    canonical_unit, display_original, display_pretty, unit_cost, total_cost,
                    matched_inventory_item_id, matched_vendor_item_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ingest_id,
                    normalized_name,
                    line.get("raw_line_text"),
                    line.get("quantity"),
                    line.get("unit"),
                    line.get("canonical_value"),
                    line.get("canonical_unit"),
                    line.get("display_original"),
                    line.get("display_pretty"),
                    line.get("unit_cost"),
                    line.get("total_cost"),
                    inventory_item_id,
                    matched_vendor_item_id,
                ),
            )

        if vendor_id:
            _apply_vendor_learning(con, ingest_id, vendor_id)
            _insert_receiving_from_lines(con, vendor_id, invoice_number, line_items)

        con.commit()
        return {
            "ok": True,
            "invoice_ingest_id": ingest_id,
            "status": status,
            "vendor_id": vendor_id,
            "vendor_guess_text": vendor_guess_text,
            "confidence": confidence,
            "line_item_count": len(line_items),
            "candidates": candidates,
        }
    except Exception as exc:
        logger.error("Invoice ingest failed: %s", exc)
        try:
            con.execute(
                """
                INSERT INTO invoice_ingests (
                    vendor_id, vendor_guess_text, confidence, telegram_chat_id, telegram_user_id,
                    image_path, raw_ocr_text, status
                ) VALUES (NULL, NULL, 0.0, ?, ?, ?, ?, 'failed')
                """,
                (telegram_chat_id, telegram_user_id, image_path, f"OCR failure: {exc}"),
            )
            con.commit()
        except Exception:
            pass
        return {"ok": False, "error": str(exc)}
    finally:
        con.close()


def assign_vendor(invoice_ingest_id: int, vendor_id: int) -> Dict[str, Any]:
    con = _get_conn()
    try:
        ingest = con.execute(
            "SELECT id, raw_ocr_text FROM invoice_ingests WHERE id = ? LIMIT 1",
            (invoice_ingest_id,),
        ).fetchone()
        if not ingest:
            return {"ok": False, "error": "Invoice ingest not found."}

        invoice_number = _extract_invoice_number(ingest["raw_ocr_text"] or "")
        con.execute(
            """
            UPDATE invoice_ingests
            SET vendor_id = ?, vendor_guess_text = (SELECT name FROM vendors WHERE id = ?),
                confidence = MAX(confidence, 0.9), status = 'parsed'
            WHERE id = ?
            """,
            (vendor_id, vendor_id, invoice_ingest_id),
        )

        rows = con.execute(
            """
            SELECT id, normalized_item_name
            FROM invoice_line_items
            WHERE invoice_ingest_id = ?
            """,
            (invoice_ingest_id,),
        ).fetchall()
        for row in rows:
            vendor_item_id = _match_vendor_item_id(con, vendor_id, row["normalized_item_name"])
            if vendor_item_id:
                con.execute(
                    "UPDATE invoice_line_items SET matched_vendor_item_id = ? WHERE id = ?",
                    (vendor_item_id, int(row["id"])),
                )

        _apply_vendor_learning(con, invoice_ingest_id, vendor_id)
        line_items = [
            dict(row)
            for row in con.execute(
                """
                SELECT normalized_item_name, raw_line_text, quantity, unit, unit_cost, total_cost
                FROM invoice_line_items
                WHERE invoice_ingest_id = ?
                """,
                (invoice_ingest_id,),
            ).fetchall()
        ]
        _insert_receiving_from_lines(con, vendor_id, invoice_number, line_items)
        con.commit()
        return {"ok": True, "invoice_ingest_id": invoice_ingest_id, "vendor_id": vendor_id}
    except Exception as exc:
        logger.error("Assign vendor failed: %s", exc)
        return {"ok": False, "error": str(exc)}
    finally:
        con.close()


def create_vendor_and_assign(invoice_ingest_id: int, vendor_name: str) -> Dict[str, Any]:
    con = _get_conn()
    try:
        cur = con.execute("INSERT INTO vendors (name) VALUES (?)", (vendor_name,))
        vendor_id = int(cur.lastrowid)
        con.commit()
    except sqlite3.IntegrityError:
        row = con.execute("SELECT id FROM vendors WHERE LOWER(name) = LOWER(?)", (vendor_name,)).fetchone()
        if not row:
            con.close()
            return {"ok": False, "error": "Could not create/find vendor."}
        vendor_id = int(row["id"])
    finally:
        con.close()

    return assign_vendor(invoice_ingest_id=invoice_ingest_id, vendor_id=vendor_id)


def forget_invoices(remove_files: bool = True) -> Dict[str, Any]:
    con = _get_conn()
    try:
        image_paths = [
            (row["image_path"] or "").strip()
            for row in con.execute("SELECT image_path FROM invoice_ingests").fetchall()
        ]
        deleted_lines = con.execute("DELETE FROM invoice_line_items").rowcount
        deleted_ingests = con.execute("DELETE FROM invoice_ingests").rowcount
        con.commit()
    finally:
        con.close()

    removed_files = 0
    if remove_files:
        for path in image_paths:
            if not path:
                continue
            try:
                p = Path(path)
                if p.exists():
                    p.unlink()
                    removed_files += 1
            except Exception:
                continue

    return {
        "ok": True,
        "deleted_ingests": int(deleted_ingests or 0),
        "deleted_line_items": int(deleted_lines or 0),
        "removed_files": removed_files,
    }
