import json
import logging
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from services import autonomy, costing, inventory, memory, ops_router, ordering, prep_list, providers, recipes
from services.argparse_simple import split_command_line
from services.commands_registry import (
    get_group_order,
    command_enabled_map,
    command_specs,
    grouped_commands,
    known_roots,
    resolve_root,
)
from services.invoice_ingest import normalize_item_name
from services.notify import alert_required, silent_success, status_line
from services.rag import TIER_1_RECIPE_OPS, normalize_knowledge_tier, rag_engine
from services.tg_format import tg_card, tg_code, tg_escape, tg_kv, tg_list
from services.units import UnitNormalizationError, normalize_quantity
from prep_brain.config import load_config


LOG_DEFAULT_LINES = 20
LOG_MAX_LINES = 200
logger = logging.getLogger(__name__)


@dataclass
class CommandRequest:
    raw: str
    name: str
    args: List[str]
    scope: Optional[str] = None
    detail: bool = False
    json_output: bool = False


@dataclass
class CommandResponse:
    text: str = ""
    silent: bool = False
    needs_vendor_selection: bool = False
    vendor_candidates: Optional[List[Dict[str, Any]]] = None
    pending_order: Optional[Dict[str, Any]] = None


def _load_config() -> Dict[str, Any]:
    return load_config()


def parse_command(text: str) -> Optional[CommandRequest]:
    raw = str(text or "").strip()
    if not raw.startswith("/"):
        return None

    parts = split_command_line(raw)

    if not parts:
        return None

    cmd = parts[0][1:].strip().lower()
    if not cmd:
        return None

    detail = False
    json_output = False
    scope: Optional[str] = None
    args: List[str] = []

    i = 1
    while i < len(parts):
        token = parts[i]
        if token == "--detail":
            detail = True
            i += 1
            continue
        if token == "--json":
            json_output = True
            i += 1
            continue
        if token == "-r" and i + 1 < len(parts):
            scope = parts[i + 1]
            i += 2
            continue

        args.append(token)
        i += 1

    return CommandRequest(
        raw=raw,
        name=cmd,
        args=args,
        scope=scope,
        detail=detail,
        json_output=json_output,
    )


def is_chat_silenced(chat_state: Dict[str, Any]) -> bool:
    until_raw = str(chat_state.get("silenced_until") or "").strip()
    if not until_raw:
        return False

    try:
        until = datetime.fromisoformat(until_raw)
    except Exception:
        chat_state.pop("silenced_until", None)
        return False

    if datetime.now() >= until:
        chat_state.pop("silenced_until", None)
        return False
    return True


def help_text(topic: Optional[str] = None) -> str:
    topic_key = (topic or "").strip().lower()

    if topic_key == "ordering":
        return "\n".join(
            [
                "<b>Ordering</b>",
                "<i>Invoices, vendor routing, and order drafts.</i>",
                "",
                "<b>Natural Language</b>",
                "<code>add 50# white onions</code>",
                "<code>order 2 cs yuzu</code>",
                "",
                "<b>Invoice Photos</b>",
                "Send photo -> OCR parses lines and guesses vendor.",
                "If confidence is low, bot asks one vendor question.",
                "",
                "<b>Cutoff Reminders</b>",
                "Bot sends one concise reminder before vendor cutoff when items are pending.",
                "",
                "<b>Routing Rules</b>",
                "1) vendor_item_affinity score",
                "2) last vendor in this chat",
                "3) one vendor question if ambiguous",
            ]
        )

    if topic_key == "recipes":
        return "\n".join(
            [
                "<b>Recipes</b>",
                "<i>Autonomy handles defaults; chef handles exceptions.</i>",
                "",
                "<b>Flow</b>",
                "House documents become drafts automatically.",
                "Reference documents stay in RAG only and never promote.",
                "",
                "<b>Auto-Promotion</b>",
                "Promotes at confidence >= 0.75 with required fields.",
                "Required: name, method, >=1 ingredient. No invented quantities or steps.",
                "",
                "<b>Silence Policy</b>",
                "Bot stays silent on successful processing.",
                "Bot speaks for errors or required decisions.",
                "",
                "<b>Manual Overrides</b>",
                "<code>/approve &lt;id&gt;</code> <code>/hold &lt;id&gt;</code> <code>/reject &lt;id&gt;</code>",
            ]
        )

    if topic_key == "debug":
        return "\n".join(
            [
                "<b>Debug</b>",
                "<i>Short diagnostics for ingest troubleshooting.</i>",
                "",
                "<b>Commands</b>",
                "<code>/debug on</code> - allow debug output",
                "<code>/debug ingest &lt;id|last&gt;</code> - extraction + chunk summary",
                "<code>/debug chunks &lt;id|last&gt;</code> - chunk and dedupe stats",
                "<code>/debug sample &lt;id|last&gt;</code> - sample chunk previews",
                "<code>/debug sources</code> - last 5 doc_sources rows",
                "<code>/debug db</code> - DB path and table counts",
                "<code>/debug recipe &lt;name|id&gt;</code> - house recipe assembly diagnostics",
                "<code>/debug off</code> - return to quiet mode",
                "",
                "<b>What It Shows</b>",
                "Ingest metrics, chunk counts, dedupe behavior, and vector-write counts.",
                "Turn debug off after troubleshooting to keep console quiet.",
            ]
        )

    enabled = command_enabled_map(_load_config())
    groups = grouped_commands(include_non_default=False)
    group_order = get_group_order()
    group_labels = {
        "Core": "Status &amp; Control",
        "Autonomy": "Autonomy",
        "Knowledge": "Knowledge &amp; Ingestion",
        "Drafts": "Drafts",
        "Recipes": "Recipes",
        "Inventory": "Inventory",
        "Vendors": "Vendors &amp; Ordering",
        "Prep": "Prep-List",
        "Debug": "Debug (Admin)",
    }

    lines: List[str] = [
        "<b>Prep-Brain - Ops Console</b>",
        "<i>Quiet by default. Reports only when needed.</i>",
        "",
    ]
    for group in group_order:
        specs = groups.get(group) or []
        if not specs:
            continue
        lines.append(f"<b>{group_labels.get(group, tg_escape(group))}</b>")
        for spec in specs:
            cmd_text = tg_escape(spec.usage)
            desc = tg_escape(spec.description)
            if not enabled.get(spec.key, True):
                lines.append(f"<code>{cmd_text}</code> - {desc} (disabled)")
            else:
                lines.append(f"<code>{cmd_text}</code> - {desc}")
        lines.append("")

    lines.extend(
        [
            "<b>More Help</b>",
            "<code>/help ordering</code> - invoices and orders",
            "<code>/help recipes</code> - recipe workflow",
            "<code>/help debug</code> - diagnostics",
            "<code>/commands</code> - full canonical command list",
        ]
    )
    return "\n".join(lines)


def _conn() -> sqlite3.Connection:
    return memory.get_conn()


def _json_or_text(req: CommandRequest, payload: Dict[str, Any], fallback_text: str) -> CommandResponse:
    if req.json_output:
        return CommandResponse(text=tg_code(json.dumps(payload, indent=2, ensure_ascii=True)))
    return CommandResponse(text=fallback_text)


def _from_notify(note: Dict[str, object], title: str = "Status") -> CommandResponse:
    if bool(note.get("silent", False)):
        return CommandResponse(silent=True, text="")
    return CommandResponse(text=tg_card(title, [str(note.get("message") or "")]))


IMPLEMENTED_ROOT_HANDLERS = {
    "yes",
    "no",
    "pause",
    "help",
    "commands",
    "status",
    "health",
    "knowledge",
    "prep",
    "autonomy",
    "jobs",
    "job",
    "mode",
    "silence",
    "unsilence",
    "log",
    "debug",
    "sources",
    "source",
    "ingests",
    "ingest",
    "reingest",
    "forget",
    "drafts",
    "draft",
    "approve",
    "hold",
    "reject",
    "next",
    "prev",
    "setname",
    "setyield",
    "setstation",
    "setmethod",
    "seting",
    "adding",
    "deling",
    "noteing",
    "recipes",
    "recipe",
    "price",
    "cost",
    "par",
    "inv",
    "vendor",
    "email",
    "review",
    "send",
    "order",
}


def registry_missing_handlers() -> List[str]:
    known = known_roots()
    return sorted(root for root in known if root not in IMPLEMENTED_ROOT_HANDLERS)


def validate_command_registry_handlers() -> bool:
    missing = registry_missing_handlers()
    if not missing:
        return True
    logger.error("Command registry missing handlers: %s", ", ".join(missing))
    return False


def commands_text() -> str:
    enabled_map = command_enabled_map(_load_config())
    rows = command_specs()
    groups: Dict[str, List[Any]] = {}
    for row in rows:
        groups.setdefault(row.group, []).append(row)

    lines = [
        "<b>Prep-Brain - Canonical Commands</b>",
        "<i>Single source of truth from command registry.</i>",
        "",
    ]
    for group in get_group_order():
        items = groups.get(group) or []
        if not items:
            continue
        lines.append(f"<b>{tg_escape(group)}</b>")
        for spec in items:
            state = "enabled" if enabled_map.get(spec.key, True) else "disabled"
            lines.append(f"<code>{tg_escape(spec.usage)}</code> - {tg_escape(spec.description)} ({state})")
        lines.append("")
    return "\n".join(lines).strip()


def _parse_numeric_arg(value: str) -> Optional[int]:
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _parse_float_arg(value: str) -> Optional[float]:
    try:
        return float(str(value).strip())
    except Exception:
        return None


def _resolve_recipe_id_by_token(token: str) -> Optional[int]:
    target = str(token or "").strip()
    if not target:
        return None
    parsed = _parse_numeric_arg(target)
    if parsed is not None:
        return int(parsed)

    rows = recipes.get_all_recipes()
    lowered = target.lower()
    exact = [row for row in rows if str(row.get("name") or "").lower() == lowered]
    if exact:
        return int(exact[0]["id"])
    partial = [row for row in rows if lowered in str(row.get("name") or "").lower()]
    if partial:
        return int(partial[0]["id"])
    return None


def _resolve_id_arg(args: List[str], chat_state: Dict[str, Any], key: str) -> Tuple[Optional[int], int]:
    if args:
        candidate = _parse_numeric_arg(args[0])
        if candidate is not None:
            return candidate, 1

    previous = chat_state.get(key)
    if previous is None:
        return None, 0
    try:
        return int(previous), 0
    except Exception:
        return None, 0


def _prep_actor_context(telegram_chat_id: int, display_name: str) -> Dict[str, Any]:
    return prep_list.resolve_staff_context(
        telegram_chat_id=int(telegram_chat_id),
        display_name=display_name,
    )


def _require_prep_manager(
    *,
    telegram_chat_id: int,
    display_name: str,
) -> Optional[CommandResponse]:
    actor = _prep_actor_context(telegram_chat_id=telegram_chat_id, display_name=display_name)
    if actor.get("is_privileged"):
        return None
    return CommandResponse(text=tg_card("Prep-List", ["Chef/Sous permissions required."]))


def _draft_missing_fields(draft_row: sqlite3.Row) -> List[str]:
    missing: List[str] = []
    if not str(draft_row["name"] or "").strip():
        missing.append("name")
    if not str(draft_row["method"] or "").strip():
        missing.append("method")
    try:
        ingredients = json.loads(draft_row["ingredients_json"] or "[]")
    except Exception:
        ingredients = []
    if not isinstance(ingredients, list) or not ingredients:
        missing.append("ingredients")
    return missing


def _load_draft(con: sqlite3.Connection, draft_id: int) -> Optional[sqlite3.Row]:
    return con.execute("SELECT * FROM recipe_drafts WHERE id = ?", (int(draft_id),)).fetchone()


def _save_draft_ingredients(con: sqlite3.Connection, draft_id: int, ingredients: List[Dict[str, Any]]) -> None:
    con.execute(
        """
        UPDATE recipe_drafts
        SET ingredients_json = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (json.dumps(ingredients), int(draft_id)),
    )


def _normalize_ingredient_entry(name: str, qty: str, unit: str, note: Optional[str], restaurant_tag: Optional[str]) -> Dict[str, Any]:
    norm = normalize_quantity(
        qty,
        unit,
        display_original=f"{qty} {unit}",
        restaurant_tag=restaurant_tag,
    )
    return {
        "inventory_item_id": None,
        "item_name_text": str(name or "").strip(),
        "quantity": norm["canonical_value"],
        "unit": norm["canonical_unit"],
        "display_original": norm["display_original"],
        "display_pretty": norm["display_pretty"],
        "notes": str(note or "").strip() or None,
    }


def _source_list(limit: int = 20) -> List[Dict[str, Any]]:
    rag_sources = rag_engine.get_sources()
    rag_by_ingest: Dict[str, Dict[str, Any]] = {}
    for row in rag_sources:
        ingest_id = str(row.get("ingest_id") or "").strip()
        if ingest_id:
            rag_by_ingest[ingest_id] = row

    con = _conn()
    try:
        rows = con.execute(
            """
            SELECT
                id,
                ingest_id,
                filename,
                source_type,
                restaurant_tag,
                file_sha256,
                file_size,
                extracted_text_chars,
                chunk_count,
                chunks_added,
                status,
                created_at,
                COALESCE(updated_at, created_at) AS updated_at
            FROM doc_sources
            ORDER BY COALESCE(updated_at, created_at) DESC, id DESC
            LIMIT ?
            """,
            (max(1, min(int(limit), 100)),),
        ).fetchall()
    finally:
        con.close()

    merged: List[Dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        rag_row = rag_by_ingest.get(str(item.get("ingest_id") or "").strip(), {})
        source_id = str(rag_row.get("id") or "").strip()
        doc_status = str(item.get("status") or "unknown")
        merged.append(
            {
                "id": source_id or str(item.get("ingest_id") or ""),
                "source_id": source_id or None,
                "ingest_id": str(item.get("ingest_id") or ""),
                "title": rag_row.get("title") or Path(str(item.get("filename") or "")).stem.replace("_", " ").title(),
                "source_name": item.get("filename"),
                "type": rag_row.get("type") or item.get("source_type"),
                "knowledge_tier": rag_row.get("knowledge_tier") or normalize_knowledge_tier(item.get("source_type")),
                "date_ingested": item.get("created_at"),
                "updated_at": item.get("updated_at"),
                "chunk_count": int(item.get("chunk_count") or 0),
                "status": rag_row.get("status") or ("active" if doc_status in {"ok", "warn"} and source_id else doc_status),
                "ingest_status": doc_status,
                "restaurant_tag": item.get("restaurant_tag"),
                "file_sha256": item.get("file_sha256"),
                "file_size": int(item.get("file_size") or 0),
                "extracted_text_chars": int(item.get("extracted_text_chars") or 0),
                "chunks_added": int(item.get("chunks_added") or 0),
                "warnings": rag_row.get("warnings") or [],
                "can_toggle": bool(source_id),
                "can_delete": True,
            }
        )
    return merged


def _render_sources_short(rows: List[Dict[str, Any]]) -> str:
    lines = []
    for row in rows:
        sid = str(row.get("id") or "")[:8]
        title = str(row.get("title") or row.get("source_name") or "source")
        status = str(row.get("status") or "unknown")
        tier = str(row.get("knowledge_tier") or "n/a")
        lines.append(f"{sid} {status} {tier} {title}")
    return tg_card("Sources", lines[:12])


def _parse_silence_duration(args: List[str]) -> Optional[datetime]:
    if not args:
        return None

    if args[0].lower() == "until" and len(args) >= 2:
        raw = args[1]
        for fmt in ("%H:%M", "%H%M"):
            try:
                parsed_t = datetime.strptime(raw, fmt).time()
                candidate = datetime.combine(datetime.now().date(), parsed_t)
                if candidate <= datetime.now():
                    candidate += timedelta(days=1)
                return candidate
            except ValueError:
                continue
        return None

    match = re.match(r"^(\d+)([mhd])$", str(args[0]).strip().lower())
    if not match:
        return None

    amount = int(match.group(1))
    unit = match.group(2)
    if unit == "m":
        return datetime.now() + timedelta(minutes=amount)
    if unit == "h":
        return datetime.now() + timedelta(hours=amount)
    if unit == "d":
        return datetime.now() + timedelta(days=amount)
    return None


def _read_logs(arg: Optional[str]) -> str:
    cfg = _load_config()
    log_path = Path(str(cfg.get("paths", {}).get("log_file", "logs/prep-brain.log")))
    if not log_path.exists():
        return tg_card("Logs", ["Log file not found."])

    mode = (arg or "").strip().lower()
    lines_to_read = LOG_DEFAULT_LINES
    only_errors = mode == "errors"

    if mode and mode != "errors":
        try:
            lines_to_read = max(1, min(int(mode), LOG_MAX_LINES))
        except ValueError:
            lines_to_read = LOG_DEFAULT_LINES

    content = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    tail = content[-lines_to_read:]
    if only_errors:
        tail = [line for line in tail if "ERROR" in line.upper() or "WARNING" in line.upper()]

    if not tail:
        return tg_card("Logs", ["No matching log lines."])

    return tg_code("\n".join(tail))


def _prep_status_line(item: Dict[str, Any]) -> str:
    name = str(item.get("recipe_name") or "Prep item")
    status = str(item.get("status") or "todo")
    hold_reason = str(item.get("hold_reason") or "").strip()
    assignee = str(item.get("assigned_staff_name") or "").strip()
    if status == "done":
        base = f"• {name} — done"
    else:
        remaining = str(item.get("remaining_display") or "remaining")
        base = f"• {name} — {remaining} remaining"
    if assignee:
        base += f" (assigned: {assignee})"
    if hold_reason:
        base += f" [hold: {hold_reason}]"
    return base


def _render_prep_grouped(grouped: List[Dict[str, Any]], title: str) -> str:
    lines: List[str] = [f"<b>{tg_escape(title)}</b>", ""]
    if not grouped:
        lines.append("No prep items.")
        return "\n".join(lines)
    for bucket in grouped:
        lines.append(f"<b>{tg_escape(str(bucket.get('station_name') or 'Unassigned'))}</b>")
        items = bucket.get("items") or []
        for item in items[:6]:
            lines.append(tg_escape(_prep_status_line(item)))
        if len(items) > 6:
            lines.append(tg_escape(f"• +{len(items) - 6} more"))
        lines.append("")
    return "\n".join(lines).strip()


def _relative_time(iso_text: Optional[str]) -> str:
    raw = str(iso_text or "").strip()
    if not raw:
        return "-"
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return raw[:19]
    now = datetime.now(dt.tzinfo) if dt.tzinfo else datetime.now()
    delta = now - dt
    seconds = max(0, int(delta.total_seconds()))
    if seconds < 60:
        return f"{seconds}s ago"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h ago"
    days = hours // 24
    return f"{days}d ago"


def _debug_enabled(chat_state: Dict[str, Any]) -> bool:
    cfg = _load_config().get("debug", {})
    return bool(chat_state.get("debug_enabled")) or bool(cfg.get("enabled", False))


def _redact_debug_text(value: str) -> str:
    text = str(value or "")
    text = re.sub(r"(?i)\b(token|api[_-]?key|password|secret|authorization)\b\s*[:=]\s*[^\s]+", r"\1=[REDACTED]", text)
    text = re.sub(r"(?i)\bBearer\s+[A-Za-z0-9._\-]+\b", "Bearer [REDACTED]", text)
    return text


def _resolve_ingest_id(chat_state: Dict[str, Any], target: str) -> Optional[str]:
    needle = str(target or "").strip()
    if not needle or needle.lower() == "last":
        last_id = str(chat_state.get("last_ingest_id") or "").strip()
        if last_id:
            return last_id
        rows = _source_list(limit=1)
        if rows:
            return str(rows[0].get("ingest_id") or "")
        return None

    # Accept source_id prefix by mapping through canonical source metadata first.
    for source in _source_list(limit=200):
        source_id = str(source.get("source_id") or source.get("id") or "")
        ingest_id = str(source.get("ingest_id") or "")
        if source_id.startswith(needle) and ingest_id:
            return ingest_id
        if ingest_id.startswith(needle):
            return ingest_id
    return needle


def _load_ingest_report(chat_state: Dict[str, Any], target: str) -> Optional[Dict[str, Any]]:
    ingest_id = _resolve_ingest_id(chat_state, target)
    if not ingest_id:
        return None
    report = rag_engine.load_ingest_report(ingest_id)
    if report:
        chat_state["last_ingest_id"] = str(report.get("ingest_id") or ingest_id)
    return report


def _doc_source_row(ingest_id: str) -> Optional[Dict[str, Any]]:
    if not ingest_id:
        return None
    con = _conn()
    try:
        row = con.execute(
            """
            SELECT ingest_id, filename, source_type, status, extracted_text_chars, chunk_count, chunks_added,
                   created_at, COALESCE(updated_at, created_at) AS updated_at
            FROM doc_sources
            WHERE ingest_id = ? OR ingest_id LIKE ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (ingest_id, f"{ingest_id}%"),
        ).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def _debug_db_report() -> str:
    db_path = memory.get_db_path()
    exists = db_path.exists()
    size_bytes = db_path.stat().st_size if exists else 0
    con = _conn()
    try:
        ingest_jobs = int(con.execute("SELECT COUNT(*) FROM ingest_jobs").fetchone()[0])
        doc_sources = int(con.execute("SELECT COUNT(*) FROM doc_sources").fetchone()[0])
        recipes_count = int(con.execute("SELECT COUNT(*) FROM recipes").fetchone()[0])
        drafts_count = int(con.execute("SELECT COUNT(*) FROM recipe_drafts").fetchone()[0])
    finally:
        con.close()

    lines = [
        f"path {db_path}",
        f"exists {str(exists).lower()} size {size_bytes}",
        f"ingest_jobs {ingest_jobs}",
        f"doc_sources {doc_sources}",
        f"recipes {recipes_count}",
        f"recipe_drafts {drafts_count}",
    ]
    return _debug_card("Debug DB", lines)


def _debug_card(title: str, lines: List[str]) -> str:
    safe_lines = [_redact_debug_text(line) for line in lines if str(line).strip()]
    return tg_card(title, safe_lines[:20])


def _format_debug_ingest_report(report: Dict[str, Any], doc_row: Optional[Dict[str, Any]] = None) -> str:
    raw = report.get("raw_document", {})
    extraction = report.get("extraction_metrics", {})
    chunking = report.get("chunking_metrics", {})
    dedupe = report.get("dedupe_metrics", {})
    vector = report.get("vector_store_metrics", {})
    warnings = report.get("warnings") or []
    status = str(report.get("status") or "unknown").upper()
    lines = [
        f"doc {raw.get('filename', '-')}",
        f"doc_sources {doc_row.get('status') if doc_row else '-'}"
        f" chunks={int((doc_row or {}).get('chunk_count', 0))}"
        f" added={int((doc_row or {}).get('chunks_added', 0))}",
        f"ext {raw.get('file_extension', '-')}, size {int(raw.get('file_size_bytes', 0))} bytes",
        f"sha {str(raw.get('file_sha256', '-'))[:16]}...",
        "DOCX Extract:",
        f"- chars: {int(extraction.get('extracted_text_chars', 0))}",
        f"- lines: {int(extraction.get('extracted_text_lines', 0))}",
        f"- paragraphs: {int(extraction.get('docx_paragraph_count', 0))} ({int(extraction.get('extracted_from_paragraphs_chars', 0))} chars)",
        f"- tables: {int(extraction.get('docx_table_count', 0))} ({int(extraction.get('extracted_from_tables_chars', 0))} chars)",
        f"- table cells: {int(extraction.get('docx_table_cell_count', 0))}",
        f"- images: {int(extraction.get('embedded_image_count', 0))}",
        "Chunking:",
        f"- size {int(chunking.get('chunk_size_config', 0))} overlap {int(chunking.get('chunk_overlap_config', 0))}",
        f"- chunks produced: {int(chunking.get('produced_chunk_count', 0))} (pre-dedupe)",
        f"- dedupe: {int(dedupe.get('pre_dedupe_count', 0))} -> {int(dedupe.get('post_dedupe_count', 0))}",
        "Vector store:",
        f"- added: {int(vector.get('successfully_added_count', 0))}/{int(vector.get('attempted_add_count', 0))}",
        f"Status: {status}",
    ]
    if warnings:
        lines.append(f"Warn: {str(warnings[0])}")
    return _debug_card("Debug Ingest", lines)


def _format_debug_chunks_report(report: Dict[str, Any]) -> str:
    extraction = report.get("extraction_metrics", {})
    chunking = report.get("chunking_metrics", {})
    dedupe = report.get("dedupe_metrics", {})
    vector = report.get("vector_store_metrics", {})
    warnings = report.get("warnings") or []
    lines = [
        f"doc {report.get('raw_document', {}).get('filename', '-')}",
        f"extracted chars: {int(extraction.get('extracted_text_chars', 0))}",
        f"extracted lines: {int(extraction.get('extracted_text_lines', 0))}",
        f"docx p/t/c: {int(extraction.get('docx_paragraph_count', 0))}/{int(extraction.get('docx_table_count', 0))}/{int(extraction.get('docx_table_cell_count', 0))}",
        f"table chars: {int(extraction.get('extracted_from_tables_chars', 0))} | paragraph chars: {int(extraction.get('extracted_from_paragraphs_chars', 0))}",
        f"chunks: {int(chunking.get('produced_chunk_count', 0))}",
        f"avg chars: {float(chunking.get('avg_chunk_chars', 0.0)):.1f}",
        f"min/max chars: {int(chunking.get('min_chunk_chars', 0))}/{int(chunking.get('max_chunk_chars', 0))}",
        f"dedupe: {'on' if dedupe.get('dedupe_enabled') else 'off'}",
        f"dedupe count: {int(dedupe.get('pre_dedupe_count', 0))} -> {int(dedupe.get('post_dedupe_count', 0))}",
        f"vector added: {int(vector.get('successfully_added_count', 0))}",
    ]
    repeated = dedupe.get("top_repeated_hashes") or []
    if repeated:
        top = repeated[0]
        lines.append(f"top repeat: {str(top.get('hash', ''))[:8]} x{int(top.get('count', 0))}")
    produced = int(chunking.get("produced_chunk_count", 0) or 0)
    if produced == 3:
        previews = chunking.get("pre_dedupe_chunk_samples") or report.get("chunk_samples") or []
        for sample in previews[:3]:
            cid = int(sample.get("chunk_id", 0))
            preview = str(sample.get("text_preview", ""))[:300].replace("\n", " ")
            lines.append(f"#{cid} {preview}")
    if warnings:
        lines.append(f"warn: {str(warnings[0])}")
    return _debug_card("Debug Chunks", lines)


def _format_debug_sample_report(report: Dict[str, Any], n: int) -> str:
    samples = report.get("chunk_samples") or []
    lines = [f"doc {report.get('raw_document', {}).get('filename', '-')}"]
    for sample in samples[: max(1, min(n, 10))]:
        cid = int(sample.get("chunk_id", 0))
        heading = str(sample.get("heading", "General"))[:50]
        preview = str(sample.get("text_preview", "")).replace("\n", " ")[:120]
        lines.append(f"#{cid} {heading}: {preview}")
    if len(lines) == 1:
        lines.append("No chunk samples available.")
    return _debug_card("Debug Sample", lines)


def _resolve_debug_recipe_query(req: CommandRequest, chat_state: Dict[str, Any]) -> Optional[str]:
    if len(req.args) >= 2:
        raw_target = " ".join(req.args[1:]).strip()
    else:
        last_recipe_id = chat_state.get("last_recipe_id")
        raw_target = str(last_recipe_id).strip() if last_recipe_id is not None else ""

    if not raw_target:
        return None

    numeric = _parse_numeric_arg(raw_target)
    if numeric is None:
        return raw_target

    con = _conn()
    try:
        row = con.execute("SELECT name FROM recipes WHERE id = ? LIMIT 1", (int(numeric),)).fetchone()
    finally:
        con.close()
    if row and row["name"]:
        return str(row["name"])
    return raw_target


def _format_debug_recipe_report(result: Dict[str, Any]) -> str:
    status = str(result.get("status") or "unknown").upper()
    lines = [
        f"status {status}",
        f"query {result.get('query') or '-'}",
        f"source {result.get('source_title') or result.get('source_name') or '-'}",
        f"recipe {result.get('matched_recipe_name') or '-'}",
        f"chunks used {int(result.get('chunks_used') or 0)}",
    ]
    sections = result.get("sections_detected") or []
    missing = result.get("missing_sections") or []
    lines.append(f"sections {', '.join(str(item) for item in sections) if sections else '-'}")
    lines.append(f"missing {', '.join(str(item) for item in missing) if missing else 'none'}")
    lines.append(f"confidence {float(result.get('confidence') or 0.0):.3f}")
    if result.get("reason"):
        lines.append(f"reason {result.get('reason')}")
    return _debug_card("Debug Recipe", lines)


def _route_order_command(req: CommandRequest, chat_state: Dict[str, Any], chat_id: int, display_name: str) -> CommandResponse:
    if not req.args:
        return CommandResponse(text=tg_card("Usage", ["/order add <qty> <unit> <item>", "/order list", "/order clear [vendor_id]"]))

    action = str(req.args[0]).lower()
    if action == "list":
        rows = ordering.get_pending_orders(limit=200)
        if not rows:
            return CommandResponse(text=tg_card("Order", ["No pending items."]))
        lines = []
        for row in rows[:20]:
            qty = float(row.get("quantity") or 0.0)
            vendor = str(row.get("vendor_name") or "Unassigned")
            lines.append(f"{qty:g} {row.get('unit')} {row.get('item_name')} [{vendor}]")
        return CommandResponse(text=tg_card("Order List", lines))

    if action == "clear":
        vendor_id = _parse_numeric_arg(req.args[1]) if len(req.args) > 1 else None
        con = _conn()
        try:
            if vendor_id is None:
                deleted = con.execute("DELETE FROM shopping_list WHERE status = 'pending'").rowcount
            else:
                deleted = con.execute(
                    "DELETE FROM shopping_list WHERE status = 'pending' AND vendor_id = ?",
                    (int(vendor_id),),
                ).rowcount
            con.commit()
        finally:
            con.close()
        return CommandResponse(text=tg_card("Order", [f"Cleared {int(deleted or 0)} pending items."]))

    order_tokens = req.args[1:] if action == "add" else req.args
    if not order_tokens:
        return CommandResponse(text=tg_card("Usage", ["/order add <qty> <unit> <item>"]))

    text = f"order {' '.join(order_tokens)}"
    routed = ordering.route_order_text(
        text=text,
        telegram_chat_id=chat_id,
        added_by=display_name,
        restaurant_tag=req.scope,
    )
    if routed.get("ok"):
        return _from_notify(
            status_line(
                f"Added {routed['display_original'] or routed['quantity_display']} {routed['item_name']} -> {routed['vendor_name']}"
            ),
            title="Order",
        )

    if routed.get("needs_vendor"):
        _ = silent_success()
        return CommandResponse(
            needs_vendor_selection=True,
            vendor_candidates=routed.get("candidates", []),
            pending_order=routed.get("parsed"),
            text="",
            silent=True,
        )

    return _from_notify(alert_required("Could not parse order request."), title="Order")


def execute_command(
    req: CommandRequest,
    chat_state: Dict[str, Any],
    *,
    telegram_chat_id: int,
    telegram_user_id: int,
    display_name: str,
) -> CommandResponse:
    name = resolve_root(req.name)

    if name not in known_roots():
        return CommandResponse(text="Unknown command. /help")

    if name == "commands":
        return CommandResponse(text=commands_text())

    if name == "help":
        topic = req.args[0] if req.args else None
        return CommandResponse(text=help_text(topic))

    if name in {"yes", "no"}:
        return CommandResponse(text=tg_card("Ops", ["No pending confirmation."]))

    if name == "pause":
        until = _parse_silence_duration(req.args)
        if not until:
            return CommandResponse(text=tg_card("Usage", ["/pause 30m", "/pause 2h", "/pause until 23:00"]))
        chat_state["autonomy_pause_until"] = until.isoformat()
        return CommandResponse(text=tg_card("Autonomy", [f"Paused until {until.strftime('%Y-%m-%d %H:%M')}"]))

    if name == "knowledge":
        rows = _source_list(limit=8)
        counts = {"restaurant_recipes": 0, "general_knowledge": 0, "general_knowledge_web": 0, "unknown": 0}
        for row in rows:
            counts[str(row.get("knowledge_tier") or "unknown")] = counts.get(str(row.get("knowledge_tier") or "unknown"), 0) + 1
        lines = [
            f"sources {len(rows)}",
            f"tier1 recipes {counts.get('restaurant_recipes', 0)}",
            f"tier3 knowledge {counts.get('general_knowledge', 0) + counts.get('general_knowledge_web', 0)}",
        ]
        return CommandResponse(text=tg_card("Knowledge", lines))

    if name == "reingest":
        nested_req = CommandRequest(
            raw=req.raw,
            name="ingest",
            args=list(req.args),
            scope=req.scope,
            detail=req.detail,
            json_output=req.json_output,
        )
        return execute_command(
            nested_req,
            chat_state,
            telegram_chat_id=telegram_chat_id,
            telegram_user_id=telegram_user_id,
            display_name=display_name,
        )

    if name == "status":
        con = _conn()
        try:
            draft_pending = con.execute("SELECT COUNT(*) FROM recipe_drafts WHERE status = 'pending'").fetchone()[0]
            pending_orders = con.execute("SELECT COUNT(*) FROM shopping_list WHERE status = 'pending'").fetchone()[0]
        finally:
            con.close()

        silence_until = chat_state.get("silenced_until")
        mode = str(chat_state.get("mode") or "service")

        lines = [
            f"mode {mode}",
            f"silence {'on' if is_chat_silenced(chat_state) else 'off'}",
            f"drafts {int(draft_pending)} | pending orders {int(pending_orders)}",
        ]
        if req.detail:
            lines.extend([
                f"chat_id {telegram_chat_id}",
                f"user_id {telegram_user_id}",
                f"silenced_until {silence_until or '-'}",
            ])

        payload = {
            "mode": mode,
            "silenced": is_chat_silenced(chat_state),
            "silenced_until": silence_until,
            "drafts_pending": int(draft_pending),
            "pending_orders": int(pending_orders),
            "chat_id": telegram_chat_id,
            "user_id": telegram_user_id,
        }
        return _json_or_text(req, payload, tg_card("Status", lines))

    if name == "health":
        db_ok = False
        rag_ok = False
        try:
            con = _conn()
            con.execute("SELECT 1").fetchone()
            con.close()
            db_ok = True
        except Exception:
            db_ok = False

        try:
            _ = rag_engine.get_sources()
            rag_ok = True
        except Exception:
            rag_ok = False

        lines = [
            f"db {'ok' if db_ok else 'fail'}",
            f"rag {'ok' if rag_ok else 'fail'}",
            f"time {datetime.now().strftime('%H:%M:%S')}",
        ]
        payload = {"db_ok": db_ok, "rag_ok": rag_ok}
        return _json_or_text(req, payload, tg_card("Health", lines))

    if name == "prep":
        args = req.args or []
        actor = _prep_actor_context(telegram_chat_id=telegram_chat_id, display_name=display_name)
        actor_name = str(actor.get("staff_name") or display_name or "Chef")

        if not args:
            grouped = prep_list.grouped_by_station(include_done=True)
            return CommandResponse(text=_render_prep_grouped(grouped, "Prep-List — Today"))

        action = str(args[0]).lower()
        if action == "station":
            station_name = " ".join(args[1:]).strip()
            if not station_name:
                return CommandResponse(text=tg_card("Usage", ["/prep station <station_name>"]))
            grouped = prep_list.grouped_by_station(station_name=station_name, include_done=True)
            return CommandResponse(text=_render_prep_grouped(grouped, f"Prep-List — {station_name}"))

        if action == "status":
            summary = prep_list.summary_by_station()
            if not summary:
                return CommandResponse(text=tg_card("Prep-List", ["No prep items."]))
            lines = [f"• {row['station_name']}: {int(row['open_count'])} open / {int(row['done_count'])} done" for row in summary]
            return CommandResponse(text=tg_card("Prep-List Status", lines[:12]))

        if action == "assign":
            if len(args) < 3:
                return CommandResponse(text=tg_card("Usage", ["/prep assign <item_id> <staff_name>"]))
            denied = _require_prep_manager(telegram_chat_id=telegram_chat_id, display_name=display_name)
            if denied:
                return denied
            item_id = _parse_numeric_arg(args[1])
            if item_id is None:
                return CommandResponse(text=tg_card("Usage", ["/prep assign <item_id> <staff_name>"]))
            staff_name = " ".join(args[2:]).strip()
            result = prep_list.assign_item(item_id=int(item_id), staff_name=staff_name, actor=actor_name)
            if not result.get("ok"):
                return CommandResponse(text=tg_card("Prep-List", [str(result.get("error") or "Assign failed.")]))
            return CommandResponse(text=tg_card("Prep-List", [f"Assigned item {item_id} to {result['staff_name']}."]))

        if action == "hold":
            if len(args) < 2:
                return CommandResponse(text=tg_card("Usage", ["/prep hold <item_id> [reason]"]))
            denied = _require_prep_manager(telegram_chat_id=telegram_chat_id, display_name=display_name)
            if denied:
                return denied
            item_id = _parse_numeric_arg(args[1])
            if item_id is None:
                return CommandResponse(text=tg_card("Usage", ["/prep hold <item_id> [reason]"]))
            reason = " ".join(args[2:]).strip() or "on hold"
            result = prep_list.hold_item(item_id=int(item_id), actor=actor_name, reason=reason)
            if not result.get("ok"):
                return CommandResponse(text=tg_card("Prep-List", [str(result.get("error") or "Hold failed.")]))
            return CommandResponse(text=tg_card("Prep-List", [f"Item {item_id} on hold."]))

        if action == "done":
            if len(args) < 2:
                return CommandResponse(text=tg_card("Usage", ["/prep done <item_id>"]))
            denied = _require_prep_manager(telegram_chat_id=telegram_chat_id, display_name=display_name)
            if denied:
                return denied
            item_id = _parse_numeric_arg(args[1])
            if item_id is None:
                return CommandResponse(text=tg_card("Usage", ["/prep done <item_id>"]))
            result = prep_list.mark_done(item_id=int(item_id), actor=actor_name)
            if not result.get("ok"):
                return CommandResponse(text=tg_card("Prep-List", [str(result.get("error") or "Done update failed.")]))
            return CommandResponse(text=tg_card("Prep-List", [f"Item {item_id} marked done."]))

        if action == "clear" and len(args) >= 2 and str(args[1]).lower() == "done":
            denied = _require_prep_manager(telegram_chat_id=telegram_chat_id, display_name=display_name)
            if denied:
                return denied
            con = _conn()
            try:
                deleted = con.execute(
                    "DELETE FROM prep_list_items WHERE status = 'done'"
                ).rowcount
                con.commit()
            finally:
                con.close()
            return CommandResponse(text=tg_card("Prep-List", [f"Cleared {int(deleted or 0)} done items."]))

        if action == "add":
            if len(args) < 4:
                return CommandResponse(text=tg_card("Usage", ["/prep add <recipe_name> <qty> <unit>"]))
            denied = _require_prep_manager(telegram_chat_id=telegram_chat_id, display_name=display_name)
            if denied:
                return denied
            qty_idx = None
            for idx in range(2, len(args)):
                if _parse_numeric_arg(args[idx]) is not None:
                    qty_idx = idx
                    break
                try:
                    float(args[idx])
                    qty_idx = idx
                    break
                except Exception:
                    continue
            if qty_idx is None or qty_idx >= len(args) - 1:
                return CommandResponse(text=tg_card("Usage", ["/prep add <recipe_name> <qty> <unit>"]))
            recipe_name = " ".join(args[1:qty_idx]).strip()
            try:
                qty = float(args[qty_idx])
            except Exception:
                return CommandResponse(text=tg_card("Usage", ["/prep add <recipe_name> <qty> <unit>"]))
            unit = args[qty_idx + 1]
            result = prep_list.add_item(recipe_name=recipe_name, qty=qty, unit=unit, actor=actor_name)
            if not result.get("ok"):
                return CommandResponse(text=tg_card("Prep-List", [str(result.get("error") or "Add failed.")]))
            return CommandResponse(
                text=tg_card(
                    "Prep-List",
                    [f"Added {result['recipe_name']} ({result['display_original']}) to {result['station_name']}."]
                )
            )

        return CommandResponse(
            text=tg_card(
                "Prep-List",
                [
                    "/prep",
                    "/prep station <station_name>",
                    "/prep status",
                    "/prep assign <item_id> <staff_name>",
                    "/prep hold <item_id> [reason]",
                    "/prep done <item_id>",
                    "/prep add <recipe_name> <qty> <unit>",
                ],
            )
        )

    if name == "autonomy":
        snapshot = autonomy.get_autonomy_status_snapshot()
        running = bool(snapshot.get("is_running"))
        pending_ingests = int(snapshot.get("queue_pending_ingests") or 0)
        pending_drafts = int(snapshot.get("queue_pending_drafts") or 0)

        detail_requested = req.detail or (req.args and req.args[0].lower() == "detail")
        if detail_requested:
            lines = [
                f"running {'yes' if running else 'no'}",
                f"last tick {_relative_time(snapshot.get('last_tick_at'))}",
                f"cycle start {snapshot.get('last_cycle_started_at') or '-'}",
                f"cycle finish {snapshot.get('last_cycle_finished_at') or '-'}",
                f"last action {snapshot.get('last_action') or '-'}",
                f"queue {pending_ingests} ingest | {pending_drafts} drafts",
            ]
            if snapshot.get("last_promoted_recipe_name"):
                lines.append(
                    f"last promoted {snapshot.get('last_promoted_recipe_name')} ({_relative_time(snapshot.get('last_promoted_at'))})"
                )
            if snapshot.get("last_error"):
                lines.append(f"last error {snapshot.get('last_error')}")
            return CommandResponse(text=tg_card("Autonomy Detail", lines[:12]))

        icon = "🟢" if running else "🔴"
        lines = [
            f"{icon} Autonomy: {'running' if running else 'stopped'}",
            f"Last tick: {_relative_time(snapshot.get('last_tick_at'))}",
            f"Queue: {pending_ingests} ingest | {pending_drafts} drafts",
        ]
        return CommandResponse(text="\n".join(lines))

    if name == "jobs":
        jobs = autonomy.list_ingest_jobs(limit=5)
        if not jobs:
            return CommandResponse(text=tg_card("Jobs", ["No ingest jobs yet."]))

        lines: List[str] = []
        for row in jobs[:5]:
            current = int(row.get("progress_current") or 0)
            total = int(row.get("progress_total") or 0)
            pct = int((current / total) * 100) if total > 0 else 0
            progress_text = f" ({pct}%)" if row.get("status") not in {"done", "failed", "needs_review"} and total > 0 else ""
            lines.append(
                f"#{row['id']} {row.get('status')} - {row.get('source_filename')}{progress_text}"
            )
        return CommandResponse(text=tg_card("Ingest Jobs", lines))

    if name == "job":
        if not req.args:
            return CommandResponse(text=tg_card("Usage", ["/job <id>"]))
        job_id = _parse_numeric_arg(req.args[0])
        if job_id is None:
            return CommandResponse(text=tg_card("Usage", ["/job <id>"]))
        row = autonomy.get_ingest_job(int(job_id))
        if not row:
            return CommandResponse(text=tg_card("Job", ["Job not found."]))
        current = int(row.get("progress_current") or 0)
        total = int(row.get("progress_total") or 0)
        lines = [
            f"#{row['id']} {row.get('source_filename')}",
            f"status {row.get('status')}",
            f"progress {current}/{total}",
            f"updated {_relative_time(row.get('updated_at'))}",
        ]
        if row.get("error"):
            lines.append(f"reason {row.get('error')}")
        if row.get("promoted_count") is not None:
            lines.append(
                f"promoted {int(row.get('promoted_count') or 0)} | review {int(row.get('needs_review_count') or 0)}"
            )
        return CommandResponse(text=tg_card("Ingest Job", lines[:12]))

    if name == "mode":
        if not req.args or req.args[0].lower() not in {"service", "admin"}:
            return CommandResponse(text=tg_card("Usage", ["/mode service|admin"]))
        mode = req.args[0].lower()
        chat_state["mode"] = mode
        return CommandResponse(text=tg_card("Mode", [f"set {mode}"]))

    if name == "silence":
        until = _parse_silence_duration(req.args)
        if not until:
            return CommandResponse(text=tg_card("Usage", ["/silence 2h", "/silence until 23:00"]))
        chat_state["silenced_until"] = until.isoformat()
        return CommandResponse(text=tg_card("Silence", [f"until {until.strftime('%Y-%m-%d %H:%M')}"]))

    if name == "unsilence":
        chat_state.pop("silenced_until", None)
        return CommandResponse(text=tg_card("Silence", ["off"]))

    if name == "log":
        arg = req.args[0] if req.args else None
        return CommandResponse(text=_read_logs(arg))

    if name == "debug":
        if not req.args:
            return CommandResponse(
                text=tg_card(
                    "Usage",
                    [
                        "/debug on",
                        "/debug off",
                        "/debug ingest <last|doc_id>",
                        "/debug chunks <last|doc_id>",
                        "/debug sample <last|doc_id> [n=3]",
                        "/debug sources",
                        "/debug db",
                        "/debug recipe <name|id>",
                    ],
                )
            )

        action = req.args[0].lower()
        if action == "on":
            chat_state["debug_enabled"] = True
            return CommandResponse(text=tg_card("Debug", ["enabled"]))
        if action == "off":
            chat_state["debug_enabled"] = False
            return CommandResponse(text=tg_card("Debug", ["disabled"]))

        if not _debug_enabled(chat_state):
            return CommandResponse(text=tg_card("Debug", ["Disabled. Use /debug on."]))

        if action == "recipe":
            recipe_query = _resolve_debug_recipe_query(req, chat_state)
            if not recipe_query:
                return CommandResponse(text=tg_card("Usage", ["/debug recipe <name|id>"]))
            result = rag_engine.debug_house_recipe(query_text=recipe_query, n_results=12, confidence_threshold=0.75)
            return CommandResponse(text=_format_debug_recipe_report(result))

        if action == "sources":
            rows = _source_list(limit=5)
            if not rows:
                return CommandResponse(text=tg_card("Debug Sources", ["No doc_sources rows yet."]))
            lines = [
                f"{str(row.get('ingest_id') or '')[:10]} {row.get('ingest_status', row.get('status'))} {row.get('source_name')}"
                for row in rows[:5]
            ]
            return CommandResponse(text=_debug_card("Debug Sources", lines))

        if action == "db":
            return CommandResponse(text=_debug_db_report())

        target = req.args[1] if len(req.args) > 1 else "last"
        report = _load_ingest_report(chat_state, target)
        if not report:
            if action == "ingest":
                ingest_id = _resolve_ingest_id(chat_state, target) or ""
                doc_row = _doc_source_row(ingest_id)
                if doc_row:
                    lines = [
                        f"ingest_id {str(doc_row.get('ingest_id') or '')[:12]}",
                        f"file {doc_row.get('filename')}",
                        f"status {doc_row.get('status')}",
                        f"chars {int(doc_row.get('extracted_text_chars') or 0)}",
                        f"chunks {int(doc_row.get('chunk_count') or 0)} added {int(doc_row.get('chunks_added') or 0)}",
                        f"updated {doc_row.get('updated_at') or '-'}",
                    ]
                    return CommandResponse(text=_debug_card("Debug Ingest", lines))
            return CommandResponse(text=tg_card("Debug", ["Ingest report not found."]))

        if action == "ingest":
            doc_row = _doc_source_row(str(report.get("ingest_id") or ""))
            return CommandResponse(text=_format_debug_ingest_report(report, doc_row=doc_row))
        if action == "chunks":
            return CommandResponse(text=_format_debug_chunks_report(report))
        if action == "sample":
            n = 3
            for token in req.args[2:]:
                token_lower = token.lower()
                if token_lower.startswith("n="):
                    try:
                        n = int(token_lower.split("=", 1)[1])
                    except ValueError:
                        pass
                else:
                    parsed_n = _parse_numeric_arg(token)
                    if parsed_n is not None:
                        n = parsed_n
            return CommandResponse(text=_format_debug_sample_report(report, n))
        return CommandResponse(text=tg_card("Debug", ["Unknown debug action."]))

    if name == "sources":
        rows = _source_list(limit=25)
        if rows:
            chat_state["last_source_id"] = rows[0].get("source_id") or rows[0].get("id")
            if rows[0].get("ingest_id"):
                chat_state["last_ingest_id"] = rows[0].get("ingest_id")
        payload = {"sources": rows}
        return _json_or_text(req, payload, _render_sources_short(rows))

    if name == "source":
        if len(req.args) < 1:
            return CommandResponse(text=tg_card("Usage", ["/source on <source_id>", "/source off <source_id>"]))

        action = req.args[0].lower()
        if action not in {"on", "off"}:
            return CommandResponse(text=tg_card("Usage", ["/source on <source_id>", "/source off <source_id>"]))

        source_token = req.args[1] if len(req.args) > 1 else str(chat_state.get("last_source_id") or "")
        if not source_token:
            return CommandResponse(text=tg_card("Source", ["No source id provided."]))

        rows = _source_list(limit=200)
        matched = None
        needle = source_token.strip().lower()
        for row in rows:
            row_source = str(row.get("source_id") or "").strip().lower()
            row_id = str(row.get("id") or "").strip().lower()
            row_ingest = str(row.get("ingest_id") or "").strip().lower()
            if row_source == needle or row_id == needle or row_ingest == needle:
                matched = row
                break
            if row_source.startswith(needle) or row_id.startswith(needle) or row_ingest.startswith(needle):
                matched = row
                break

        source_id = str((matched or {}).get("source_id") or source_token).strip()
        if not source_id:
            return CommandResponse(text=tg_card("Source", ["Source is queued and not indexed yet."]))

        ok = rag_engine.toggle_source(source_id, action == "on")
        if not ok:
            return CommandResponse(text=tg_card("Source", ["Source not found."]))

        chat_state["last_source_id"] = source_id
        return CommandResponse(text=tg_card("Source", [f"{source_id[:8]} -> {action}"]))

    if name == "ingests":
        limit = 10
        if req.args:
            try:
                limit = max(1, min(int(req.args[0]), 50))
            except ValueError:
                pass

        rows = _source_list(limit=limit)
        lines = [
            f"{str(row.get('ingest_id') or row.get('id') or '')[:10]} {str(row.get('updated_at') or row.get('date_ingested') or '-')[:19]} {row.get('ingest_status', row.get('status', '-'))} {row.get('title') or row.get('source_name') or ''}"
            for row in rows
        ]
        if rows and rows[0].get("ingest_id"):
            chat_state["last_ingest_id"] = rows[0].get("ingest_id")
        return _json_or_text(req, {"ingests": rows}, tg_card("Ingests", lines or ["None"]))

    if name == "ingest":
        source_token = req.args[0] if req.args else str(chat_state.get("last_source_id") or "")
        if not source_token:
            return CommandResponse(text=tg_card("Ingest", ["No ingest/source id provided."]))

        source = None
        needle = source_token.strip().lower()
        for item in _source_list(limit=500):
            source_id = str(item.get("source_id") or "").strip().lower()
            item_id = str(item.get("id") or "").strip().lower()
            ingest_id = str(item.get("ingest_id") or "").strip().lower()
            if needle in {source_id, item_id, ingest_id} or source_id.startswith(needle) or item_id.startswith(needle) or ingest_id.startswith(needle):
                source = item
                break

        if not source:
            return CommandResponse(text=tg_card("Ingest", ["Source not found."]))

        source_name = str(source.get("source_name") or source.get("filename") or "").strip()
        file_path = Path("data/documents") / source_name
        if not file_path.exists():
            return CommandResponse(text=tg_card("Ingest", [f"File not found: {source_name}"]))

        ok, result = rag_engine.ingest_file(
            str(file_path),
            extra_metadata={
                "source_title": source.get("title"),
                "source_type": source.get("type"),
                "knowledge_tier": source.get("knowledge_tier"),
                "summary": source.get("summary"),
            },
        )
        if not ok:
            return CommandResponse(text=tg_card("Ingest", [f"Failed: {result}"]))

        chat_state["last_source_id"] = source.get("source_id") or source.get("id")
        if isinstance(result, dict) and result.get("ingest_id"):
            chat_state["last_ingest_id"] = result.get("ingest_id")
        return CommandResponse(text=tg_card("Ingest", [f"Re-ran {str(source.get('ingest_id') or source.get('id') or '')[:10]}"]))

    if name == "forget":
        if len(req.args) < 2:
            return CommandResponse(text=tg_card("Usage", ["/forget source <id>", "/forget vendor <id>", "/forget invoices"]))
        target = str(req.args[0]).lower()
        if target == "source":
            source_id = str(req.args[1]).strip()
            if not source_id:
                return CommandResponse(text=tg_card("Usage", ["/forget source <id>"]))
            ok = rag_engine.delete_source(source_id)
            if not ok:
                return CommandResponse(text=tg_card("Source", ["Source not found."]))
            return CommandResponse(text=tg_card("Source", [f"Forgot source {source_id[:12]}."]))
        if target == "vendor":
            vendor_id = _parse_numeric_arg(req.args[1])
            if vendor_id is None:
                return CommandResponse(text=tg_card("Usage", ["/forget vendor <id>"]))
            result = ordering.forget_vendor(vendor_id, remove_files=True)
            return CommandResponse(
                text=tg_card(
                    "Vendor",
                    [f"Forgot vendor {vendor_id}. Removed {int(result.get('deleted_ingests', 0))} invoices."],
                )
            )
        if target == "invoices":
            from services import invoice_ingest

            purged = invoice_ingest.forget_invoices(remove_files=True)
            return CommandResponse(
                text=tg_card(
                    "Invoices",
                    [f"Forgot {int(purged.get('deleted_ingests', 0))} ingests and removed {int(purged.get('removed_files', 0))} files."],
                )
            )
        return CommandResponse(text=tg_card("Usage", ["/forget source <id>", "/forget vendor <id>", "/forget invoices"]))

    if name == "drafts":
        limit = 10
        if req.args:
            first = _parse_numeric_arg(req.args[0])
            if first is not None:
                limit = max(1, min(first, 50))

        tag = req.scope
        con = _conn()
        try:
            if tag:
                rows = con.execute(
                    """
                    SELECT id, name, status, confidence, knowledge_tier, updated_at
                    FROM recipe_drafts
                    WHERE LOWER(name) LIKE ? OR LOWER(COALESCE(source_id,'')) LIKE ?
                    ORDER BY updated_at DESC, id DESC
                    LIMIT ?
                    """,
                    (f"%{tag.lower()}%", f"%{tag.lower()}%", limit),
                ).fetchall()
            else:
                rows = con.execute(
                    """
                    SELECT id, name, status, confidence, knowledge_tier, updated_at
                    FROM recipe_drafts
                    ORDER BY updated_at DESC, id DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
        finally:
            con.close()

        lines = [
            f"{row['id']} {row['status']} {float(row['confidence'] or 0.0):.2f} {row['name']}"
            for row in rows
        ]
        ids = [int(row["id"]) for row in rows]
        if ids:
            chat_state["draft_nav_ids"] = ids
            chat_state["draft_nav_idx"] = 0
            chat_state["last_draft_id"] = ids[0]

        payload = {"drafts": [dict(row) for row in rows]}
        return _json_or_text(req, payload, tg_card("Drafts", lines or ["No drafts"]))

    if name in {"draft", "approve", "hold", "reject", "setname", "setyield", "setstation", "setmethod", "seting", "adding", "deling", "noteing"}:
        con = _conn()
        try:
            draft_id, consumed = _resolve_id_arg(req.args, chat_state, "last_draft_id")
            if draft_id is None:
                return CommandResponse(text=tg_card("Draft", ["No draft id in context."]))

            draft = _load_draft(con, draft_id)
            if not draft:
                return CommandResponse(text=tg_card("Draft", ["Draft not found."]))

            chat_state["last_draft_id"] = int(draft_id)

            if name == "draft":
                try:
                    ingredients = json.loads(draft["ingredients_json"] or "[]")
                except Exception:
                    ingredients = []
                missing = _draft_missing_fields(draft)
                lines = [
                    f"#{draft['id']} {draft['name']}",
                    f"{draft['status']} {float(draft['confidence'] or 0.0):.2f} {draft['knowledge_tier'] or 'tier?'}",
                    f"ingredients {len(ingredients)}",
                ]
                if missing:
                    lines.append(f"missing {', '.join(missing)}")
                if req.detail:
                    lines.append(f"method {str(draft['method'] or '')[:120] or '-'}")
                payload = {"draft": dict(draft), "ingredients": ingredients, "missing": missing}
                return _json_or_text(req, payload, tg_card("Draft", lines))

            if name == "approve":
                tier = normalize_knowledge_tier(draft["knowledge_tier"]) or ""
                if tier != TIER_1_RECIPE_OPS:
                    return CommandResponse(
                        text=tg_card("Boundary", ["General knowledge drafts never promote."])
                    )

                missing = _draft_missing_fields(draft)
                if missing:
                    hint = f"/seting {draft_id} \"Xanthan gum\" 2.5 g" if "ingredients" in missing else ""
                    msg = f"Needs review: {draft['name']} — missing {', '.join(missing)}. {hint}".strip()
                    return CommandResponse(text=tg_card("Needs review", [msg]))

                try:
                    ingredients = json.loads(draft["ingredients_json"] or "[]")
                except Exception:
                    return CommandResponse(text=tg_card("Approve", ["Invalid ingredients payload."]))

                result = recipes.create_recipe(
                    {
                        "name": draft["name"],
                        "yield_amount": draft["yield_amount"],
                        "yield_unit": draft["yield_unit"],
                        "station": draft["station"],
                        "category": draft["category"],
                        "method": draft["method"],
                    },
                    ingredients,
                )
                if "successfully" not in result.lower():
                    return CommandResponse(text=tg_card("Approve", [result]))

                con.execute(
                    """
                    UPDATE recipe_drafts
                    SET status = 'promoted', rejection_reason = NULL, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (int(draft_id),),
                )
                con.commit()
                return CommandResponse(text=tg_card("Approved", [f"Draft {draft_id} promoted."]))

            if name == "hold":
                reason = " ".join(req.args[consumed:]).strip() or "On hold"
                con.execute(
                    "UPDATE recipe_drafts SET status='pending', rejection_reason=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                    (f"hold: {reason}"[:800], int(draft_id)),
                )
                con.commit()
                return CommandResponse(text=tg_card("Draft", [f"{draft_id} on hold."]))

            if name == "reject":
                reason = " ".join(req.args[consumed:]).strip() or "Rejected"
                con.execute(
                    "UPDATE recipe_drafts SET status='rejected', rejection_reason=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                    (reason[:800], int(draft_id)),
                )
                con.commit()
                return CommandResponse(text=tg_card("Draft", [f"{draft_id} rejected."]))

            if name == "setname":
                new_name = " ".join(req.args[consumed:]).strip()
                if not new_name:
                    return CommandResponse(text=tg_card("Usage", ["/setname <id> \"New Name\""]))
                con.execute("UPDATE recipe_drafts SET name=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", (new_name, int(draft_id)))
                con.commit()
                return CommandResponse(text=tg_card("Draft", [f"{draft_id} name updated."]))

            if name == "setyield":
                if len(req.args[consumed:]) < 2:
                    return CommandResponse(text=tg_card("Usage", ["/setyield <id> <amount> <unit>"]))
                amount = req.args[consumed]
                unit = req.args[consumed + 1]
                try:
                    norm = normalize_quantity(amount, unit, display_original=f"{amount} {unit}", restaurant_tag=req.scope)
                except UnitNormalizationError as exc:
                    return CommandResponse(text=tg_card("Yield", [str(exc)]))
                con.execute(
                    "UPDATE recipe_drafts SET yield_amount=?, yield_unit=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                    (norm["canonical_value"], norm["canonical_unit"], int(draft_id)),
                )
                con.commit()
                return CommandResponse(text=tg_card("Draft", [f"{draft_id} yield {norm['display_pretty']}"]))

            if name == "setstation":
                station = " ".join(req.args[consumed:]).strip()
                if not station:
                    return CommandResponse(text=tg_card("Usage", ["/setstation <id> \"Station\""]))
                con.execute("UPDATE recipe_drafts SET station=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", (station, int(draft_id)))
                con.commit()
                return CommandResponse(text=tg_card("Draft", [f"{draft_id} station updated."]))

            if name == "setmethod":
                method = " ".join(req.args[consumed:]).strip()
                if not method:
                    return CommandResponse(text=tg_card("Usage", ["/setmethod <id> \"Method...\""]))
                con.execute("UPDATE recipe_drafts SET method=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", (method, int(draft_id)))
                con.commit()
                return CommandResponse(text=tg_card("Draft", [f"{draft_id} method updated."]))

            # Ingredient edit operations
            try:
                ingredients = json.loads(draft["ingredients_json"] or "[]")
            except Exception:
                ingredients = []
            if not isinstance(ingredients, list):
                ingredients = []

            args_left = req.args[consumed:]
            if name in {"seting", "adding"}:
                if len(args_left) < 3:
                    return CommandResponse(text=tg_card("Usage", [f"/{name} <id> \"Ingredient\" <qty> <unit> [notes]"]))
                ingredient_name = args_left[0]
                qty = args_left[1]
                unit = args_left[2]
                notes = " ".join(args_left[3:]).strip() or None

                try:
                    entry = _normalize_ingredient_entry(
                        ingredient_name,
                        qty,
                        unit,
                        notes,
                        restaurant_tag=req.scope,
                    )
                except UnitNormalizationError as exc:
                    return CommandResponse(text=tg_card("Ingredient", [str(exc)]))

                target_key = normalize_item_name(ingredient_name)
                if name == "seting":
                    replaced = False
                    for idx, item in enumerate(ingredients):
                        if normalize_item_name(str(item.get("item_name_text") or "")) == target_key:
                            ingredients[idx] = entry
                            replaced = True
                            break
                    if not replaced:
                        ingredients.append(entry)
                else:
                    ingredients.append(entry)

                _save_draft_ingredients(con, int(draft_id), ingredients)
                con.commit()
                return CommandResponse(text=tg_card("Draft", [f"{draft_id} ingredient saved: {ingredient_name}"]))

            if name == "deling":
                if len(args_left) < 1:
                    return CommandResponse(text=tg_card("Usage", ["/deling <id> \"Ingredient\""]))
                ingredient_name = args_left[0]
                target_key = normalize_item_name(ingredient_name)
                kept = [
                    item
                    for item in ingredients
                    if normalize_item_name(str(item.get("item_name_text") or "")) != target_key
                ]
                _save_draft_ingredients(con, int(draft_id), kept)
                con.commit()
                return CommandResponse(text=tg_card("Draft", [f"{draft_id} ingredient removed: {ingredient_name}"]))

            if name == "noteing":
                if len(args_left) < 2:
                    return CommandResponse(text=tg_card("Usage", ["/noteing <id> \"Ingredient\" \"note\""]))
                ingredient_name = args_left[0]
                note = " ".join(args_left[1:]).strip()
                target_key = normalize_item_name(ingredient_name)
                changed = False
                for item in ingredients:
                    if normalize_item_name(str(item.get("item_name_text") or "")) == target_key:
                        item["notes"] = note
                        changed = True
                        break
                if not changed:
                    return CommandResponse(text=tg_card("Draft", ["Ingredient not found."]))
                _save_draft_ingredients(con, int(draft_id), ingredients)
                con.commit()
                return CommandResponse(text=tg_card("Draft", [f"{draft_id} note updated."]))

            return CommandResponse(text=tg_card("Draft", ["Unsupported draft action."]))
        finally:
            con.close()

    if name in {"next", "prev"}:
        nav_ids = chat_state.get("draft_nav_ids") or []
        if not nav_ids:
            # hydrate with latest drafts
            con = _conn()
            try:
                rows = con.execute("SELECT id FROM recipe_drafts ORDER BY updated_at DESC, id DESC LIMIT 20").fetchall()
                nav_ids = [int(row["id"]) for row in rows]
            finally:
                con.close()
            if not nav_ids:
                return CommandResponse(text=tg_card("Draft", ["No drafts."]))
            chat_state["draft_nav_ids"] = nav_ids
            chat_state["draft_nav_idx"] = 0

        idx = int(chat_state.get("draft_nav_idx") or 0)
        if name == "next":
            idx = min(idx + 1, len(nav_ids) - 1)
        else:
            idx = max(idx - 1, 0)
        chat_state["draft_nav_idx"] = idx
        target = nav_ids[idx]
        chat_state["last_draft_id"] = target
        nested_req = CommandRequest(raw=req.raw, name="draft", args=[str(target)], detail=req.detail, json_output=req.json_output)
        return execute_command(
            nested_req,
            chat_state,
            telegram_chat_id=telegram_chat_id,
            telegram_user_id=telegram_user_id,
            display_name=display_name,
        )

    if name == "recipes":
        if not req.args or req.args[0].lower() != "new":
            return CommandResponse(text=tg_card("Usage", ["/recipes new [N]"]))
        limit = 10
        if len(req.args) > 1:
            parsed = _parse_numeric_arg(req.args[1])
            if parsed is not None:
                limit = max(1, min(parsed, 50))
        con = _conn()
        try:
            rows = con.execute(
                """
                SELECT id, name, created_at
                FROM recipes
                ORDER BY datetime(created_at) DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        finally:
            con.close()
        lines = [f"{row['id']} {row['name']} ({row['created_at'][:19]})" for row in rows]
        return _json_or_text(req, {"recipes": [dict(row) for row in rows]}, tg_card("New Recipes", lines or ["None"]))

    if name == "recipe":
        if not req.args:
            last_id = chat_state.get("last_recipe_id")
            if last_id is None:
                return CommandResponse(text=tg_card("Usage", ["/recipe find \"query\"", "/recipe <id|name>"]))
            req_args = [str(last_id)]
        else:
            req_args = req.args

        action = req_args[0].lower()
        if action == "find":
            query = " ".join(req_args[1:]).strip()
            if not query:
                return CommandResponse(text=tg_card("Usage", ["/recipe find \"query\""]))

            con = _conn()
            try:
                rows = con.execute(
                    "SELECT id, name, station, is_active FROM recipes WHERE LOWER(name) LIKE ? ORDER BY name LIMIT 20",
                    (f"%{query.lower()}%",),
                ).fetchall()
            finally:
                con.close()

            lines = [f"{row['id']} {'on' if row['is_active'] else 'off'} {row['name']}" for row in rows]
            if rows:
                chat_state["last_recipe_id"] = int(rows[0]["id"])
            return _json_or_text(req, {"recipes": [dict(row) for row in rows]}, tg_card("Recipes", lines or ["No match"]))

        if action == "new":
            return CommandResponse(
                text=tg_card(
                    "Recipe",
                    [
                        "Guided creation is available in admin workflow.",
                        "Use /setname, /setyield, /setmethod, /adding on a draft.",
                    ],
                )
            )

        if action in {"deactivate", "activate"}:
            recipe_id = _parse_numeric_arg(req_args[1]) if len(req_args) > 1 else _parse_numeric_arg(str(chat_state.get("last_recipe_id") or ""))
            if recipe_id is None:
                return CommandResponse(text=tg_card("Usage", [f"/recipe {action} <id>"]))
            con = _conn()
            try:
                cur = con.execute(
                    "UPDATE recipes SET is_active=? WHERE id=?",
                    (1 if action == "activate" else 0, int(recipe_id)),
                )
                con.commit()
            finally:
                con.close()
            if cur.rowcount == 0:
                return CommandResponse(text=tg_card("Recipe", ["Recipe not found."]))
            chat_state["last_recipe_id"] = int(recipe_id)
            return CommandResponse(text=tg_card("Recipe", [f"{recipe_id} {action}d"]))

        target = " ".join(req_args).strip()
        recipe_row = None
        if target.isdigit():
            recipe_row = recipes.get_recipe_details(int(target))
        else:
            all_rows = recipes.get_all_recipes()
            lowered = target.lower()
            exact = [row for row in all_rows if str(row.get("name") or "").lower() == lowered]
            if exact:
                recipe_row = recipes.get_recipe_details(int(exact[0]["id"]))
            else:
                partial = [row for row in all_rows if lowered in str(row.get("name") or "").lower()]
                if partial:
                    recipe_row = recipes.get_recipe_details(int(partial[0]["id"]))

        if not recipe_row:
            return CommandResponse(text=tg_card("Recipe", ["Recipe not found."]))

        chat_state["last_recipe_id"] = int(recipe_row["id"])
        ingredients = recipe_row.get("ingredients", [])
        lines = [
            f"#{recipe_row['id']} {recipe_row['name']}",
            f"yield {recipe_row.get('yield_amount') or '-'} {recipe_row.get('yield_unit') or ''}".strip(),
            f"ingredients {len(ingredients)}",
        ]
        if req.detail:
            for ing in ingredients[:12]:
                qty = ing.get("quantity")
                unit = ing.get("unit") or ""
                display_name_ing = ing.get("display_name") or ing.get("item_name_text") or "ingredient"
                lines.append(f"- {qty:g} {unit} {display_name_ing}" if isinstance(qty, (int, float)) else f"- {display_name_ing}")
        return _json_or_text(req, {"recipe": recipe_row}, tg_card("Recipe", lines))

    if name == "price":
        if len(req.args) < 3 or str(req.args[0]).lower() != "set":
            return CommandResponse(text=tg_card("Usage", ['/price set "name" <amt> [per portion]']))
        payload = req.args[1:]
        amount_idx = -1
        amount_val: Optional[float] = None
        for idx in range(len(payload) - 1, -1, -1):
            parsed_amount = ops_router.parse_currency(payload[idx])
            if parsed_amount is not None:
                amount_idx = idx
                amount_val = parsed_amount
                break
        if amount_idx <= 0 or amount_val is None:
            return CommandResponse(text=tg_card("Price", ["Could not parse amount."]))

        target_name = " ".join(payload[:amount_idx]).strip()
        tail = " ".join(payload[amount_idx + 1 :]).strip().lower()
        unit = "portion" if "portion" in tail else "portion"
        intent = {
            "intent": "update_recipe_sales_price",
            "target_name": target_name,
            "price": float(amount_val),
            "unit": unit,
            "raw_text": req.raw,
            "confirmed": True,
        }
        result = ops_router.execute_ops_intent(
            intent,
            actor_telegram_user_id=int(telegram_user_id),
            actor_display_name=display_name,
        )
        status = str(result.get("status") or "")
        if status == "updated":
            return CommandResponse(
                text=tg_card(
                    "Price",
                    [f"✓ {result.get('recipe_name')} - sales price set to ${float(result.get('price') or 0.0):.2f} / {result.get('unit') or 'portion'}"],
                )
            )
        if status == "needs_choice":
            choices = result.get("choices") or []
            lines = ["Multiple matches found:"]
            for choice in choices[:5]:
                lines.append(f"{choice.get('id')} {choice.get('name')}")
            return CommandResponse(text=tg_card("Price", lines))
        if status == "not_found":
            return CommandResponse(text=f"⚠️ No match for '{tg_escape(target_name)}'. Try /recipe find \"{tg_escape(target_name)}\"")
        return CommandResponse(text=tg_card("Price", [str(result.get("message") or "Could not update price.")]))

    if name == "cost":
        if len(req.args) < 2 or str(req.args[0]).lower() != "refresh":
            return CommandResponse(text=tg_card("Usage", ["/cost refresh <id|name>"]))
        target = " ".join(req.args[1:]).strip()
        recipe_id = _resolve_recipe_id_by_token(target)
        if recipe_id is None:
            return CommandResponse(text=tg_card("Cost", ["Recipe not found."]))
        sync_msg = costing.update_ingredient_costs(int(recipe_id))
        totals = costing.calculate_recipe_cost(int(recipe_id))
        return CommandResponse(
            text=tg_card(
                "Cost",
                [
                    sync_msg,
                    f"total ${float(totals.get('total_cost') or 0.0):.2f}",
                    f"per_yield ${float(totals.get('cost_per_yield') or 0.0):.2f}",
                ],
            )
        )

    if name == "par":
        if len(req.args) < 4 or str(req.args[0]).lower() != "set":
            return CommandResponse(text=tg_card("Usage", ["/par set <recipe|inventory> <id|name> <value>"]))
        kind = str(req.args[1]).lower()
        target = str(req.args[2]).strip()
        value = _parse_float_arg(req.args[3])
        if value is None:
            return CommandResponse(text=tg_card("Par", ["Invalid numeric value."]))
        con = _conn()
        try:
            if kind == "recipe":
                recipe_id = _resolve_recipe_id_by_token(target)
                if recipe_id is None:
                    return CommandResponse(text=tg_card("Par", ["Recipe not found."]))
                con.execute(
                    "UPDATE recipes SET par_level = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (float(value), int(recipe_id)),
                )
                con.commit()
                return CommandResponse(text=tg_card("Par", [f"Recipe {recipe_id} par set to {float(value):g}."]))

            if kind == "inventory":
                inv_cols = [str(row[1]) for row in con.execute("PRAGMA table_info(inventory_items)").fetchall()]
                if "par_level" not in inv_cols:
                    con.execute("ALTER TABLE inventory_items ADD COLUMN par_level REAL DEFAULT 0")
                if target.isdigit():
                    row = con.execute("SELECT id, name FROM inventory_items WHERE id = ?", (int(target),)).fetchone()
                else:
                    row = con.execute(
                        "SELECT id, name FROM inventory_items WHERE LOWER(name) LIKE ? ORDER BY name LIMIT 1",
                        (f"%{target.lower()}%",),
                    ).fetchone()
                if not row:
                    return CommandResponse(text=tg_card("Par", ["Inventory item not found."]))
                con.execute(
                    "UPDATE inventory_items SET par_level = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (float(value), int(row["id"])),
                )
                con.commit()
                return CommandResponse(text=tg_card("Par", [f"{row['name']} par set to {float(value):g}."]))
        finally:
            con.close()
        return CommandResponse(text=tg_card("Par", ["Unsupported scope. Use recipe|inventory."]))

    if name == "inv":
        if not req.args:
            return CommandResponse(text=tg_card("Usage", ["/inv find \"query\"", "/inv <id|name>", "/inv low"]))

        action = req.args[0].lower()
        con = _conn()
        try:
            if action == "find":
                query = " ".join(req.args[1:]).strip().lower()
                rows = con.execute(
                    "SELECT id, name, quantity, unit, cost FROM inventory_items WHERE LOWER(name) LIKE ? ORDER BY name LIMIT 20",
                    (f"%{query}%",),
                ).fetchall()
                lines = [f"{row['id']} {row['name']} {float(row['quantity'] or 0):g} {row['unit']}" for row in rows]
                return _json_or_text(req, {"items": [dict(row) for row in rows]}, tg_card("Inventory", lines or ["No match"]))

            if action == "low":
                rows = con.execute(
                    "SELECT id, name, quantity, unit FROM inventory_items WHERE COALESCE(quantity, 0) <= 0 ORDER BY name LIMIT 30"
                ).fetchall()
                lines = [f"{row['id']} {row['name']} {float(row['quantity'] or 0):g} {row['unit']}" for row in rows]
                return _json_or_text(req, {"items": [dict(row) for row in rows]}, tg_card("Inventory Low", lines or ["None"]))

            if action in {"set", "add", "cost"}:
                if len(req.args) < 3:
                    return CommandResponse(text=tg_card("Usage", [f"/inv {action} <id|name> <value> [unit]"]))
                target = str(req.args[1]).strip()
                value = _parse_float_arg(req.args[2])
                if value is None:
                    return CommandResponse(text=tg_card("Inventory", ["Invalid numeric value."]))
                unit = req.args[3] if len(req.args) > 3 else None

                if target.isdigit():
                    row = con.execute(
                        "SELECT id, name, quantity, unit, cost FROM inventory_items WHERE id = ?",
                        (int(target),),
                    ).fetchone()
                else:
                    row = con.execute(
                        "SELECT id, name, quantity, unit, cost FROM inventory_items WHERE LOWER(name) = ? LIMIT 1",
                        (target.lower(),),
                    ).fetchone()
                    if not row:
                        row = con.execute(
                            "SELECT id, name, quantity, unit, cost FROM inventory_items WHERE LOWER(name) LIKE ? ORDER BY name LIMIT 1",
                            (f"%{target.lower()}%",),
                        ).fetchone()

                if not row:
                    return CommandResponse(text=tg_card("Inventory", ["Item not found."]))

                row_id = int(row["id"])
                row_name = str(row["name"])
                row_unit = str(row["unit"] or "unit")

                if action == "set":
                    next_qty = float(value)
                    next_unit = str(unit or row_unit)
                    con.execute(
                        "UPDATE inventory_items SET quantity = ?, unit = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (next_qty, next_unit, row_id),
                    )
                    con.commit()
                    return CommandResponse(text=tg_card("Inventory", [f"{row_name} set to {next_qty:g} {next_unit}."]))

                if action == "add":
                    delta = float(value)
                    next_qty = float(row["quantity"] or 0.0) + delta
                    con.execute(
                        "UPDATE inventory_items SET quantity = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (next_qty, row_id),
                    )
                    con.commit()
                    return CommandResponse(text=tg_card("Inventory", [f"{row_name} now {next_qty:g} {row_unit}."]))

                next_cost = round(float(value), 2)
                con.execute(
                    "UPDATE inventory_items SET cost = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (next_cost, row_id),
                )
                con.commit()
                return CommandResponse(text=tg_card("Inventory", [f"{row_name} cost set to ${next_cost:.2f}/{row_unit}."]))

            target = " ".join(req.args).strip()
            if target.isdigit():
                row = con.execute(
                    "SELECT id, name, quantity, unit, cost FROM inventory_items WHERE id = ?",
                    (int(target),),
                ).fetchone()
            else:
                row = con.execute(
                    "SELECT id, name, quantity, unit, cost FROM inventory_items WHERE LOWER(name) = ? LIMIT 1",
                    (target.lower(),),
                ).fetchone()
                if not row:
                    row = con.execute(
                        "SELECT id, name, quantity, unit, cost FROM inventory_items WHERE LOWER(name) LIKE ? ORDER BY name LIMIT 1",
                        (f"%{target.lower()}%",),
                    ).fetchone()

            if not row:
                return CommandResponse(text=tg_card("Inventory", ["Item not found."]))

            payload = dict(row)
            lines = [
                f"#{row['id']} {row['name']}",
                f"qty {float(row['quantity'] or 0):g} {row['unit']}",
                f"cost ${float(row['cost'] or 0):.2f}/{row['unit']}",
            ]
            return _json_or_text(req, {"item": payload}, tg_card("Inventory", lines))
        finally:
            con.close()

    if name == "vendor":
        if not req.args:
            last_vendor_id = _parse_numeric_arg(str(chat_state.get("last_vendor_id") or ""))
            if last_vendor_id is not None:
                req_args = [str(last_vendor_id)]
            else:
                return CommandResponse(text=tg_card("Usage", ["/vendor list", "/vendor <id|name>"]))
        else:
            req_args = req.args

        action = req_args[0].lower()
        if action == "list":
            vendors = providers.get_all_vendors()
            lines = [f"{v['id']} {v['name']} cutoff {v.get('cutoff_time') or '-'}" for v in vendors[:30]]
            if vendors:
                chat_state["last_vendor_id"] = int(vendors[0]["id"])
            return _json_or_text(req, {"vendors": vendors}, tg_card("Vendors", lines or ["No vendors"]))
        if action == "new":
            vendor_name = " ".join(req_args[1:]).strip()
            if not vendor_name:
                return CommandResponse(text=tg_card("Usage", ["/vendor new <name>"]))
            message = providers.create_vendor({"name": vendor_name})
            return CommandResponse(text=tg_card("Vendor", [message]))

        target = " ".join(req_args).strip()
        vendor = None
        if target.isdigit():
            vendor = providers.get_vendor(int(target))
        else:
            all_vendors = providers.get_all_vendors()
            lower = target.lower()
            exact = [v for v in all_vendors if str(v.get("name") or "").lower() == lower]
            vendor = exact[0] if exact else None
            if not vendor:
                partial = [v for v in all_vendors if lower in str(v.get("name") or "").lower()]
                vendor = partial[0] if partial else None

        if not vendor:
            return CommandResponse(text=tg_card("Vendor", ["Vendor not found."]))

        chat_state["last_vendor_id"] = int(vendor["id"])
        lines = [
            f"#{vendor['id']} {vendor['name']}",
            f"email {vendor.get('email') or '-'}",
            f"cutoff {vendor.get('cutoff_time') or '-'}",
        ]
        return _json_or_text(req, {"vendor": vendor}, tg_card("Vendor", lines))

    if name in {"email", "review", "send"}:
        if len(req.args) < 2 or req.args[0].lower() != "vendor":
            return CommandResponse(text=tg_card("Usage", [f"/{name} vendor <vendor_id>"]))

        vendor_id = _parse_numeric_arg(req.args[1])
        if vendor_id is None:
            vendor_id = _parse_numeric_arg(str(chat_state.get("last_vendor_id") or ""))
        if vendor_id is None:
            return CommandResponse(text=tg_card("Vendor", ["Vendor id required."]))

        chat_state["last_vendor_id"] = int(vendor_id)

        if name == "email":
            draft = ordering.build_vendor_email_draft(int(vendor_id))
            if not draft.get("ok"):
                return CommandResponse(text=tg_card("Email", [str(draft.get("error") or "Could not build draft")]))
            if req.detail:
                lines = [
                    f"vendor {draft['vendor_name']}",
                    f"items {draft['items_count']}",
                    f"cutoff {draft.get('cutoff_time') or '-'}",
                ]
                return CommandResponse(text=tg_card("Email Draft", lines, [f"/review vendor {vendor_id}", f"/send vendor {vendor_id}"]))
            return CommandResponse(
                text=tg_card(
                    "Email Draft",
                    [f"Draft ready for {draft['vendor_name']} ({draft['items_count']} items)."],
                    [f"/review vendor {vendor_id}", f"/send vendor {vendor_id}"],
                )
            )

        if name == "review":
            draft = ordering.build_vendor_email_draft(int(vendor_id))
            if not draft.get("ok"):
                return CommandResponse(text=tg_card("Review", [str(draft.get("error") or "Could not build draft")]))
            preview = (
                f"TO: {draft.get('vendor_email') or '(vendor email not set)'}\n"
                f"SUBJECT: {draft['subject']}\n"
                "--------------------------------------------------\n"
                f"{draft['body']}\n"
                "--------------------------------------------------\n"
            )
            return CommandResponse(text=tg_code(preview))

        if name == "send":
            result = ordering.send_vendor_draft(int(vendor_id))
            if not result.get("ok"):
                return CommandResponse(text=tg_card("Send", [str(result.get("error") or "Send failed")]))
            if result.get("sent"):
                draft = result.get("draft", {})
                return CommandResponse(text=tg_card("Send", [f"Sent to {draft.get('vendor_name', f'vendor {vendor_id}')}. "]))
            return CommandResponse(text=tg_card("Send", ["SMTP unavailable.", f"/review vendor {vendor_id}"]))

    if name == "order":
        return _route_order_command(req, chat_state, telegram_chat_id, display_name)

    return CommandResponse(text="Unknown command. /help")
