import asyncio
import html
import logging
import os
import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    Defaults,
    MessageHandler,
    filters,
)
from telegram.request import HTTPXRequest

import services.commands as commands
import services.invoice_ingest as invoice_ingest
import services.kitchen_ops as ops
import services.ops_router as ops_router
import services.ordering as ordering
import services.prep_list as prep_list
from services.commands_registry import (
    bind_default_handler,
    get_root_spec,
    known_roots,
    resolve_root,
    validate_registry,
)
from services import autonomy as autonomy_service
from services import memory as memory_service
from services.brain import chat
from services.command_runner import CommandRunner
from services.memory import (
    add_message,
    get_or_create_active_session,
    get_or_create_user,
    get_recent_messages,
    init_db,
)
from services.tg_format import tg_escape, tg_render_answer
from services.transcriber import transcribe_file
from prep_brain.config import load_config, resolve_path

try:
    import fcntl  # type: ignore
except Exception:  # pragma: no cover - non-posix fallback
    fcntl = None  # type: ignore


CONFIG = load_config()

WORKDIR = resolve_path("data/tmp")
WORKDIR.mkdir(parents=True, exist_ok=True)
INVOICE_DIR = resolve_path("data/invoices")
INVOICE_DIR.mkdir(parents=True, exist_ok=True)

TELEGRAM_MESSAGE_LIMIT = 3500
PENDING_ORDER_CONTEXT_KEY = "pending_order_request"
PENDING_INVOICE_VENDOR_KEY = "pending_invoice_vendor_ingest_id"
PENDING_OPS_INTENT_KEY = "pending_ops_intent"
PENDING_OPS_COST_KEY = "pending_ops_cost_clarification"
PENDING_OPS_CONFIRM_KEY = "pending_ops_confirmation"
COMPONENT_TERMS = (
    "sauce",
    "vinaigrette",
    "glaze",
    "custard",
    "stock",
    "broth",
    "dressing",
    "marinade",
    "aioli",
    "syrup",
    "ganache",
    "puree",
)

logger = logging.getLogger(__name__)
COMMAND_RUNNER = CommandRunner()
AUTONOMY_WORKER = None
AUTONOMY_TASK: Optional[asyncio.Task] = None
BOT_LOCK_PATH = resolve_path("run/bot.singleton.lock")
BOT_LOCK_HANDLE = None


def _acquire_bot_singleton() -> bool:
    global BOT_LOCK_HANDLE
    if BOT_LOCK_HANDLE is not None:
        return True
    if fcntl is None:
        BOT_LOCK_HANDLE = open(BOT_LOCK_PATH, "a+")
        return True
    try:
        BOT_LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
        BOT_LOCK_HANDLE = open(BOT_LOCK_PATH, "a+")
        fcntl.flock(BOT_LOCK_HANDLE.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        BOT_LOCK_HANDLE.seek(0)
        BOT_LOCK_HANDLE.truncate(0)
        BOT_LOCK_HANDLE.write(str(os.getpid()))
        BOT_LOCK_HANDLE.flush()
        return True
    except Exception:
        try:
            if BOT_LOCK_HANDLE is not None:
                BOT_LOCK_HANDLE.close()
        except Exception:
            pass
        BOT_LOCK_HANDLE = None
        return False


def _release_bot_singleton() -> None:
    global BOT_LOCK_HANDLE
    if BOT_LOCK_HANDLE is None:
        return
    try:
        if fcntl is not None:
            fcntl.flock(BOT_LOCK_HANDLE.fileno(), fcntl.LOCK_UN)
    except Exception:
        pass
    try:
        BOT_LOCK_HANDLE.close()
    except Exception:
        pass
    BOT_LOCK_HANDLE = None


def _invoice_cfg() -> Dict[str, object]:
    return (
        CONFIG.get("invoice_ingest", {}) if isinstance(CONFIG.get("invoice_ingest"), dict) else {}
    )


def _ordering_cfg() -> Dict[str, object]:
    return CONFIG.get("ordering", {}) if isinstance(CONFIG.get("ordering"), dict) else {}


def _invoice_ingest_enabled() -> bool:
    return bool(_invoice_cfg().get("enabled", True))


def _invoice_vendor_threshold() -> float:
    return float(_invoice_cfg().get("vendor_confidence_threshold", 0.75))


def _job_source_type(source_type: str, knowledge_tier: str) -> str:
    return autonomy_service.normalize_job_source_type(
        source_type=source_type, knowledge_tier=knowledge_tier
    )


def _fmt_bytes(num_bytes: int) -> str:
    mb = num_bytes / (1024 * 1024)
    return f"{mb:.2f}MB"


def _allowed(update: Update) -> bool:
    allowed_ids = _allowed_user_ids()
    if not allowed_ids:
        logger.warning("SECURITY: No allowed_user_ids configured — bot is open to ALL users.")
        return True

    user = update.effective_user
    return bool(user and user.id in allowed_ids)


def _allowed_user_ids() -> set[int]:
    allowed_ids = set(CONFIG["telegram"].get("allowed_user_ids", []))
    if not allowed_ids:
        env_allowed = os.getenv("TELEGRAM_ALLOWED_USER_IDS", "").strip()
        if env_allowed:
            try:
                allowed_ids.update({int(x.strip()) for x in env_allowed.split(",") if x.strip()})
            except ValueError:
                logger.warning("Invalid TELEGRAM_ALLOWED_USER_IDS value.")
    return {int(x) for x in allowed_ids}


def is_admin(update: Update) -> bool:
    user = update.effective_user
    chat_obj = update.effective_chat
    if not user:
        return False

    allowed_ids = _allowed_user_ids()
    if allowed_ids and int(user.id) in allowed_ids:
        return True

    chat_id = int(chat_obj.id) if chat_obj else int(user.id)
    con = memory_service.get_conn()
    try:
        row = con.execute(
            "SELECT role FROM staff WHERE is_active = 1 AND telegram_chat_id = ? LIMIT 1",
            (chat_id,),
        ).fetchone()
    finally:
        con.close()
    if not row:
        return False
    role = str(row["role"] or "").lower()
    return any(token in role for token in ("chef", "sous", "cdc", "head", "admin"))


async def _typing_loop(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, stop_event: asyncio.Event
) -> None:
    while not stop_event.is_set():
        await context.bot.send_chat_action(chat_id=chat_id, action="typing")
        await asyncio.sleep(4)


def _truncate(text: str, limit: int = 220) -> str:
    cleaned = " ".join((text or "").split())
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[:limit].rstrip()}..."


def _strip_markdown(text: str) -> str:
    cleaned = (text or "").replace("\r\n", "\n")
    cleaned = re.sub(r"`{1,3}", "", cleaned)
    cleaned = cleaned.replace("**", "").replace("__", "")
    cleaned = re.sub(r"^#{1,6}\s*", "", cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r"\[(.*?)\]\((.*?)\)", r"\1", cleaned)
    return cleaned.strip()


def _to_plain_text(html_text: str) -> str:
    no_tags = re.sub(r"<[^>]+>", "", html_text or "")
    return html.unescape(no_tags).strip()


def _split_for_telegram(text: str, limit: int = TELEGRAM_MESSAGE_LIMIT) -> List[str]:
    content = (text or "").strip()
    if not content:
        return []

    chunks: List[str] = []
    remaining = content

    while len(remaining) > limit:
        cut = remaining.rfind("\n", 0, limit)
        if cut < int(limit * 0.55):
            cut = remaining.rfind(" ", 0, limit)
        if cut < int(limit * 0.40):
            cut = limit

        piece = remaining[:cut].rstrip()
        if piece:
            chunks.append(piece)
        remaining = remaining[cut:].lstrip()

    if remaining:
        chunks.append(remaining)

    return chunks


def _build_kitchen_card_html(
    title: str,
    summary: str,
    key_points: List[str],
    kitchen_lines: List[str],
    ask: Optional[str] = None,
    max_key_points: Optional[int] = 5,
) -> str:
    safe_title = html.escape((title or "Kitchen Card").strip())
    safe_summary = html.escape(_truncate(summary or "No response generated.", 320))

    bullets: List[str] = []
    for item in key_points:
        line = " ".join((item or "").split()).strip()
        if line:
            bullets.append(line)
    if not bullets:
        bullets = ["No key points available."]
    if max_key_points is not None:
        bullets = bullets[:max_key_points]

    applied: List[str] = []
    for line in kitchen_lines:
        value = " ".join((line or "").split()).strip()
        if value:
            applied.append(value)
    if not applied:
        applied = ["Apply this directly to prep order, timing, and station constraints."]
    applied = applied[:2]

    lines: List[str] = [
        f"<b>{safe_title}</b>",
        safe_summary,
        "",
        "<b>Key points</b>",
    ]
    for bullet in bullets:
        lines.append(f"• {html.escape(bullet)}")

    lines.extend(["", "<b>In the kitchen</b>"])
    for item in applied:
        lines.append(html.escape(item))

    return "\n".join(lines).strip()


def _extract_sentences(text: str) -> List[str]:
    if not text:
        return []
    parts = re.split(r"(?<=[.!?])\s+", " ".join(text.split()))
    return [part.strip() for part in parts if part.strip()]


def _looks_like_card(text: str) -> bool:
    lowered = (text or "").lower()
    return "<b>" in lowered and "key points" in lowered and "in the kitchen" in lowered


def _looks_like_house_recipe_html(text: str) -> bool:
    lowered = (text or "").lower()
    if "<i>source:" not in lowered:
        return False
    if "<b>method</b>" not in lowered:
        return False
    return "• " in (text or "")


def _is_component_recipe_query(query_text: Optional[str]) -> bool:
    lowered = (query_text or "").strip().lower()
    if not lowered:
        return False
    return any(term in lowered for term in COMPONENT_TERMS)


def _looks_like_component_recipe(text: str) -> bool:
    lowered = (text or "").lower()
    return "<b>base recipe</b>" in lowered and "<b>method</b>" in lowered


def _normalize_component_title(raw_text: str, user_query: Optional[str]) -> str:
    cleaned_query = _strip_markdown(user_query or "")
    cleaned_query = re.sub(
        r"(?i)\b(?:what(?:'s| is)?|show|give|send|need|recipe|for|the|our|please|how|to|make)\b",
        " ",
        cleaned_query,
    )
    cleaned_query = " ".join(cleaned_query.split()).strip(" -:?.!,")
    if cleaned_query:
        return _truncate(cleaned_query.title(), 80)

    candidate_lines = [
        line.strip() for line in _strip_markdown(raw_text).split("\n") if line.strip()
    ]
    for line in candidate_lines:
        if len(line.split()) <= 8:
            return _truncate(line.title(), 80)
    return "Component Recipe"


def _format_component_recipe_html(raw_text: str, user_query: Optional[str] = None) -> str:
    if _looks_like_component_recipe(raw_text):
        return (raw_text or "").strip()

    cleaned = _strip_markdown(raw_text)
    lines = [line.strip() for line in cleaned.split("\n") if line.strip()]
    bullet_pattern = re.compile(r"^(?:[-•*]|\d+[\.)])\s+(.+)$")
    section_header_pattern = re.compile(
        r"^(?:base recipe|ingredients?|key points|method|in the kitchen)\b", re.I
    )
    quantity_hint_pattern = re.compile(
        r"(\d|ml|l\b|g\b|kg\b|oz\b|lb\b|tbsp|tsp|cup|%|xgum|xanthan)", re.I
    )

    ingredient_lines: List[str] = []
    in_base_section = False
    method_lines: List[str] = []
    in_method_section = False

    for line in lines:
        normalized = re.sub(r"<[^>]+>", "", line).strip()
        lowered = normalized.lower().strip(":")

        if (
            lowered.startswith("base recipe")
            or lowered.startswith("ingredients")
            or lowered.startswith("key points")
        ):
            in_base_section = True
            in_method_section = False
            continue
        if lowered.startswith("method"):
            in_method_section = True
            in_base_section = False
            continue
        if lowered.startswith("in the kitchen"):
            in_base_section = False
            in_method_section = False
            continue

        bullet_match = bullet_pattern.match(normalized)
        value = bullet_match.group(1).strip() if bullet_match else normalized
        value = " ".join(value.split()).strip()
        if not value:
            continue
        if section_header_pattern.match(value):
            continue

        if in_method_section:
            method_lines.append(value)
            continue

        if in_base_section:
            ingredient_lines.append(value)
            continue

        if quantity_hint_pattern.search(value):
            ingredient_lines.append(value)

    if not ingredient_lines:
        for line in lines:
            match = bullet_pattern.match(line)
            if match:
                candidate = " ".join(match.group(1).split()).strip()
                if candidate and not candidate.lower().startswith("in service"):
                    ingredient_lines.append(candidate)

    deduped_ingredients: List[str] = []
    seen = set()
    for line in ingredient_lines:
        key = line.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped_ingredients.append(line)

    if not deduped_ingredients:
        deduped_ingredients = ["No base recipe lines were found in the retrieved output."]

    method_line = ""
    for line in method_lines:
        candidate = " ".join(line.split()).strip()
        if candidate and not section_header_pattern.match(candidate):
            method_line = candidate
            break
    if not method_line:
        text_blob = " ".join(lines).lower()
        if "xanthan" in text_blob or "xgum" in text_blob:
            method_line = "Blend liquids smooth, then shear in xanthan/Xgum until fully hydrated."
        else:
            method_line = "Blend ingredients until homogeneous and emulsified; hold chilled."
    elif method_line.lower().startswith("combine"):
        text_blob = " ".join(lines).lower()
        if "xanthan" in text_blob or "xgum" in text_blob:
            method_line = "Blend liquids smooth, then shear in xanthan/Xgum until fully hydrated."
        else:
            method_line = "Blend ingredients until homogeneous and emulsified; hold chilled."

    safe_title = html.escape(_normalize_component_title(raw_text=cleaned, user_query=user_query))
    out_lines: List[str] = [
        f"<b>{safe_title}</b>",
        "<b>Base recipe</b>",
    ]
    for ingredient in deduped_ingredients:
        out_lines.append(f"• {html.escape(ingredient)}")
    out_lines.extend(
        [
            "",
            "<b>Method</b>",
            f"• {html.escape(method_line)}",
        ]
    )
    return "\n".join(out_lines).strip()


def _format_assistant_card(raw_text: str, user_query: Optional[str] = None) -> str:
    if _looks_like_house_recipe_html(raw_text):
        return (raw_text or "").strip()

    if _looks_like_card(raw_text) or re.search(r"<(?:b|i|code|pre|u|s|a)(?:\s|>)", raw_text or "", re.I):
        return (raw_text or "").strip()

    rendered = tg_render_answer(raw_text)
    if rendered:
        return rendered

    _ = user_query
    return "Not in my sources yet.\nTry a narrower query or reingest/OCR the source."


async def _send_html_reply(update: Update, formatted_text: str) -> None:
    if not update.message:
        return

    chunks = _split_for_telegram(formatted_text)
    if not chunks:
        chunks = [
            _build_kitchen_card_html(
                title="Kitchen Note",
                summary="No response generated.",
                key_points=[
                    "Try again with one clear question.",
                    "Add station or service context.",
                    "Include constraints if relevant.",
                ],
                kitchen_lines=["I can answer once details are provided."],
            )
        ]

    for chunk in chunks:
        try:
            await update.message.reply_text(
                chunk,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception as exc:
            logger.warning(
                "Failed to send formatted HTML reply, falling back to escaped text: %s", exc
            )
            safe_chunk = html.escape(_to_plain_text(chunk))
            await update.message.reply_text(
                safe_chunk,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )


def _parse_vendor_id(args: List[str]) -> Optional[int]:
    if not args:
        return None
    candidate = args[1] if len(args) >= 2 and args[0].lower() == "vendor" else args[0]
    try:
        return int(candidate)
    except (TypeError, ValueError):
        return None


async def _prompt_vendor_choice_for_order(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    parsed: Dict[str, object],
    candidates: List[Dict[str, object]],
) -> None:
    if not update.message:
        return

    buttons = []
    for candidate in candidates[:5]:
        vendor_id = int(candidate.get("vendor_id"))
        vendor_name = str(candidate.get("vendor_name") or f"Vendor {vendor_id}")
        buttons.append([InlineKeyboardButton(vendor_name, callback_data=f"ordsel:{vendor_id}")])
    buttons.append([InlineKeyboardButton("Cancel", callback_data="ordcancel:0")])

    context.user_data[PENDING_ORDER_CONTEXT_KEY] = parsed
    await update.message.reply_text(
        "Which vendor should I use?",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def _process_order_text(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    *,
    explicit_vendor_id: Optional[int] = None,
    quiet_success: bool = False,
) -> bool:
    if not update.effective_chat or not update.effective_user or not update.message:
        return False

    parsed = ordering.parse_order_text(text)
    if not parsed:
        return False

    routed = ordering.route_order_text(
        text=text,
        telegram_chat_id=int(update.effective_chat.id),
        added_by=update.effective_user.first_name or update.effective_user.full_name or "Chef",
        explicit_vendor_id=explicit_vendor_id,
        restaurant_tag=str(context.chat_data.get("restaurant_tag") or "").strip() or None,
    )
    if routed.get("ok"):
        if not quiet_success:
            await update.message.reply_text(
                f"Added {tg_escape(routed['display_original'] or routed['quantity_display'])} "
                f"{tg_escape(routed['item_name'])} to {tg_escape(routed['vendor_name'])}."
            )
        return True

    if routed.get("needs_vendor"):
        await _prompt_vendor_choice_for_order(
            update,
            context,
            parsed=routed.get("parsed") or parsed,
            candidates=routed.get("candidates", []),
        )
        return True

    await update.message.reply_text("Could not parse that order. Example: add 50# white onions")
    return True


async def _process_prep_update_text(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
) -> bool:
    if not update.effective_chat or not update.effective_user or not update.message:
        return False

    if not prep_list.is_prep_update_text(text):
        return False

    result = prep_list.process_natural_update(
        text=text,
        telegram_chat_id=int(update.effective_chat.id),
        telegram_user_id=int(update.effective_user.id),
        display_name=update.effective_user.full_name or update.effective_user.first_name or "Cook",
    )
    if not result.get("handled"):
        return False

    message = str(result.get("message") or "").strip()
    if message:
        await update.message.reply_text(tg_escape(message))
    return True


def _ops_success_message(result: Dict[str, object]) -> str:
    name = str(result.get("recipe_name") or "Recipe")
    price = float(result.get("price") or 0.0)
    unit = str(result.get("display_unit") or result.get("unit") or "portion")
    return f"✓ {name} - sales price set to ${price:.2f} / {unit}"


async def _prompt_ops_recipe_choice(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    intent: Dict[str, object],
    choices: List[Dict[str, object]],
) -> None:
    if not update.message:
        return

    buttons = []
    for choice in choices[:5]:
        recipe_id = int(choice.get("id"))
        recipe_name = str(choice.get("name") or f"Recipe {recipe_id}")
        buttons.append(
            [
                InlineKeyboardButton(
                    f"{recipe_name} (id {recipe_id})", callback_data=f"opsrec:{recipe_id}"
                )
            ]
        )
    buttons.append([InlineKeyboardButton("Cancel", callback_data="opscancel:0")])

    context.user_data[PENDING_OPS_INTENT_KEY] = intent
    await update.message.reply_text(
        "Which one should I update?",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def _execute_sales_price_update(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    intent: Dict[str, object],
    forced_recipe_id: Optional[int] = None,
) -> bool:
    user = update.effective_user
    if not user or not update.message:
        return False

    actor_name = (user.full_name or user.first_name or "Chef").strip()
    if forced_recipe_id is not None:
        result = ops_router.apply_sales_price_to_recipe_id(
            recipe_id=int(forced_recipe_id),
            price=float(intent.get("price") or 0.0),
            unit=str(intent.get("unit") or "portion"),
            actor_telegram_user_id=int(user.id),
            actor_display_name=actor_name,
            raw_note=str(intent.get("raw_text") or ""),
        )
    else:
        result = ops_router.execute_ops_intent(
            intent,
            actor_telegram_user_id=int(user.id),
            actor_display_name=actor_name,
        )

    status = str(result.get("status") or "")
    if status == "updated":
        await update.message.reply_text(tg_escape(_ops_success_message(result)))
        return True
    if status == "needs_choice":
        await _prompt_ops_recipe_choice(
            update,
            context,
            intent=intent,
            choices=result.get("choices", []),  # type: ignore[arg-type]
        )
        return True
    if status == "not_found":
        target = (
            str(result.get("target_name") or intent.get("target_name") or "").strip() or "query"
        )
        await update.message.reply_text(
            f"⚠️ No match for '{tg_escape(target)}'. Try /recipe find \"ribs\""
        )
        return True
    if status == "validation_error":
        await update.message.reply_text(
            f"⚠️ {tg_escape(str(result.get('message') or 'Invalid value.'))}"
        )
        return True
    if status == "needs_confirmation":
        context.user_data[PENDING_OPS_CONFIRM_KEY] = {
            "intent": "update_recipe_sales_price",
            "target_name": str(result.get("target_name") or ""),
            "price": float(result.get("price") or 0.0),
            "unit": str(result.get("unit") or "portion"),
            "raw_text": str(result.get("raw_text") or ""),
            "confirmed": True,
        }
        await update.message.reply_text(
            f"⚠️ Price looks high (${float(result.get('price') or 0.0):.2f}). Confirm update? /yes /no"
        )
        return True
    if status == "needs_clarification":
        context.user_data[PENDING_OPS_COST_KEY] = {
            "intent": "update_recipe_sales_price",
            "target_name": str(result.get("target_name") or ""),
            "price": float(result.get("price") or 0.0),
            "unit": str(result.get("unit") or "portion"),
            "raw_text": str(result.get("raw_text") or ""),
        }
        await update.message.reply_text(
            "Food cost is calculated from ingredients. Do you mean sales price? /yes /no"
        )
        return True

    return False


async def _process_pending_ops_followup(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
) -> bool:
    pending_confirm = context.user_data.get(PENDING_OPS_CONFIRM_KEY)
    if pending_confirm:
        normalized = str(text or "").strip().lower()
        if normalized in {"/yes", "yes", "y"}:
            context.user_data.pop(PENDING_OPS_CONFIRM_KEY, None)
            return await _execute_sales_price_update(update, context, intent=dict(pending_confirm))
        if normalized in {"/no", "no", "n"}:
            context.user_data.pop(PENDING_OPS_CONFIRM_KEY, None)
            if update.message:
                await update.message.reply_text("No changes made.")
            return True
        if update.message:
            await update.message.reply_text("Confirm update? /yes /no")
        return True

    pending_cost = context.user_data.get(PENDING_OPS_COST_KEY)
    if not pending_cost:
        return False

    normalized = str(text or "").strip().lower()
    if normalized in {"/yes", "yes", "y"}:
        context.user_data.pop(PENDING_OPS_COST_KEY, None)
        return await _execute_sales_price_update(update, context, intent=dict(pending_cost))
    if normalized in {"/no", "no", "n"}:
        context.user_data.pop(PENDING_OPS_COST_KEY, None)
        if update.message:
            await update.message.reply_text("No changes made.")
        return True

    if update.message:
        await update.message.reply_text("Do you mean sales price? /yes /no")
    return True


async def _process_ops_intent_text(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
) -> bool:
    intent = ops_router.detect_ops_intent(text)
    if not intent:
        return False
    return await _execute_sales_price_update(update, context, intent=intent)


async def handle_invoice_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    if not _invoice_ingest_enabled():
        return
    if not update.message or not update.message.photo:
        return
    if update.message.caption and update.message.caption.strip().startswith("/"):
        return

    user = update.effective_user
    chat_obj = update.effective_chat
    if not user or not chat_obj:
        return

    await context.bot.send_chat_action(chat_id=chat_obj.id, action="typing")
    photo = update.message.photo[-1]
    ext = ".jpg"
    path = INVOICE_DIR / f"invoice_{photo.file_unique_id}_{update.message.message_id}{ext}"

    try:
        tg_file = await context.bot.get_file(photo.file_id)
        await tg_file.download_to_drive(custom_path=str(path))
        result = invoice_ingest.ingest_invoice_image(
            image_path=str(path),
            telegram_chat_id=int(chat_obj.id),
            telegram_user_id=int(user.id),
            vendor_confidence_threshold=_invoice_vendor_threshold(),
        )
    except Exception as exc:
        logger.error("Invoice photo ingest failed: %s", exc)
        await update.message.reply_text("Action required: invoice ingestion failed.")
        return

    if not result.get("ok"):
        await update.message.reply_text("Action required: invoice ingestion failed.")
        return

    if result.get("status") != "pending_vendor":
        return

    ingest_id = int(result["invoice_ingest_id"])
    candidates = result.get("candidates", [])
    buttons = []
    for candidate in candidates[:5]:
        vendor_id = int(candidate.get("vendor_id"))
        vendor_name = str(candidate.get("vendor_name") or f"Vendor {vendor_id}")
        buttons.append(
            [InlineKeyboardButton(vendor_name, callback_data=f"invsel:{ingest_id}:{vendor_id}")]
        )
    buttons.append([InlineKeyboardButton("New Vendor", callback_data=f"invnew:{ingest_id}:0")])
    await update.message.reply_text(
        "Which vendor is this?",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def handle_inline_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    query = update.callback_query
    if not query:
        return

    data = str(query.data or "")
    await query.answer()

    try:
        action, part1, part2 = (data.split(":", 2) + ["0", "0"])[:3]
    except Exception:
        return

    if action == "invsel":
        try:
            ingest_id = int(part1)
            vendor_id = int(part2)
        except ValueError:
            return
        result = invoice_ingest.assign_vendor(invoice_ingest_id=ingest_id, vendor_id=vendor_id)
        if not result.get("ok") and query.message:
            await query.message.reply_text("Action required: invoice vendor assignment failed.")
        return

    if action == "invnew":
        try:
            ingest_id = int(part1)
        except ValueError:
            return
        context.user_data[PENDING_INVOICE_VENDOR_KEY] = ingest_id
        if query.message:
            await query.message.reply_text("Which vendor is this?")
        return

    if action == "ordcancel":
        context.user_data.pop(PENDING_ORDER_CONTEXT_KEY, None)
        return

    if action == "ordsel":
        pending = context.user_data.get(PENDING_ORDER_CONTEXT_KEY)
        if (
            not pending
            or not query.message
            or not update.effective_chat
            or not update.effective_user
        ):
            return
        try:
            vendor_id = int(part1)
        except ValueError:
            return
        created = ordering.add_routed_order(
            telegram_chat_id=int(update.effective_chat.id),
            added_by=update.effective_user.first_name or update.effective_user.full_name or "Chef",
            item_name=str(pending.get("item_name") or ""),
            normalized_item_name=str(pending.get("normalized_item_name") or ""),
            quantity=float(pending.get("quantity") or 0.0),
            unit=str(pending.get("unit") or "unit"),
            canonical_value=float(pending.get("quantity") or 0.0),
            canonical_unit=str(pending.get("unit") or "each"),
            display_original=str(pending.get("display_original") or ""),
            display_pretty=str(pending.get("display_pretty") or ""),
            vendor_id=vendor_id,
        )
        context.user_data.pop(PENDING_ORDER_CONTEXT_KEY, None)
        await query.message.reply_text(
            f"Added {tg_escape(created['display_original'] or created['quantity_display'])} "
            f"{tg_escape(created['item_name'])} to {tg_escape(created['vendor_name'])}."
        )
        return

    if action == "opscancel":
        context.user_data.pop(PENDING_OPS_INTENT_KEY, None)
        context.user_data.pop(PENDING_OPS_COST_KEY, None)
        context.user_data.pop(PENDING_OPS_CONFIRM_KEY, None)
        return

    if action == "opsrec":
        pending = context.user_data.get(PENDING_OPS_INTENT_KEY)
        if not pending or not query.message or not update.effective_user:
            return
        try:
            recipe_id = int(part1)
        except ValueError:
            return
        context.user_data.pop(PENDING_OPS_INTENT_KEY, None)
        result = ops_router.apply_sales_price_to_recipe_id(
            recipe_id=recipe_id,
            price=float(pending.get("price") or 0.0),
            unit=str(pending.get("unit") or "portion"),
            actor_telegram_user_id=int(update.effective_user.id),
            actor_display_name=(
                update.effective_user.full_name or update.effective_user.first_name or "Chef"
            ).strip(),
            raw_note=str(pending.get("raw_text") or ""),
        )
        if str(result.get("status")) == "updated":
            await query.message.reply_text(tg_escape(_ops_success_message(result)))
        else:
            await query.message.reply_text("⚠️ Could not apply that update.")


async def cutoff_reminder_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        ordering_config = _ordering_cfg()
        offsets = ordering_config.get("reminder_offsets_minutes", [60, 15])
        quiet_hours = ordering_config.get("quiet_hours")
        reminders = ordering.get_due_cutoff_reminders(
            reminder_offsets_minutes=[int(x) for x in offsets],
            quiet_hours=quiet_hours if isinstance(quiet_hours, dict) else None,
            now=datetime.now(),
        )
        if not reminders:
            return

        default_targets = [
            int(value) for value in CONFIG.get("telegram", {}).get("allowed_user_ids", []) if value
        ]
        for reminder in reminders:
            vendor_id = int(reminder["vendor_id"])
            chat_ids = ordering.pending_chat_ids_for_vendor(vendor_id) or default_targets
            if not chat_ids:
                continue
            message = (
                f"Reminder: {reminder['vendor_name']} cutoff in {reminder['offset_minutes']}m. "
                f"Draft ready ({reminder['pending_count']} items). "
                f"/review vendor {vendor_id} /send vendor {vendor_id}"
            )
            sent = False
            for chat_id in chat_ids:
                try:
                    await context.bot.send_message(chat_id=chat_id, text=message)
                    sent = True
                except Exception as exc:
                    logger.warning("Failed sending cutoff reminder to chat %s: %s", chat_id, exc)
            if sent:
                ordering.mark_cutoff_reminder_sent(
                    vendor_id=vendor_id,
                    reminder_date=str(reminder["reminder_date"]),
                    offset_minutes=int(reminder["offset_minutes"]),
                )
    except Exception as exc:
        logger.error("Cutoff reminder job failed: %s", exc)


async def autonomy_tick_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    global AUTONOMY_WORKER
    try:
        if AUTONOMY_WORKER is None:
            AUTONOMY_WORKER = autonomy_service.AutonomyWorker()
        await AUTONOMY_WORKER.run_background_tick()
    except Exception as exc:
        logger.error("Autonomy tick job failed: %s", exc)


async def _start_autonomy_background() -> None:
    global AUTONOMY_WORKER, AUTONOMY_TASK
    if AUTONOMY_TASK is not None and not AUTONOMY_TASK.done():
        return
    if AUTONOMY_WORKER is None:
        AUTONOMY_WORKER = autonomy_service.AutonomyWorker()
    # Autonomy is always-on for bot runtime, regardless of config toggles.
    AUTONOMY_WORKER.enabled = True
    logger.info("Autonomy worker starting with DB: %s", str(memory_service.get_db_path()))
    AUTONOMY_TASK = asyncio.create_task(AUTONOMY_WORKER.start(), name="prep_brain_autonomy_worker")


async def _stop_autonomy_background() -> None:
    global AUTONOMY_WORKER, AUTONOMY_TASK
    if AUTONOMY_WORKER is not None:
        try:
            AUTONOMY_WORKER.stop()
        except Exception:
            pass
    if AUTONOMY_TASK is not None:
        try:
            await asyncio.wait_for(AUTONOMY_TASK, timeout=5)
        except Exception:
            AUTONOMY_TASK.cancel()
        AUTONOMY_TASK = None


async def _app_post_init(_: Application) -> None:
    await _start_autonomy_background()


async def _app_post_shutdown(_: Application) -> None:
    await _stop_autonomy_background()


def _request_requires_admin(req: commands.CommandRequest) -> bool:
    root = resolve_root(req.name)
    first = str(req.args[0]).lower() if req.args else ""
    second = str(req.args[1]).lower() if len(req.args) > 1 else ""

    if root in {
        "mode",
        "silence",
        "unsilence",
        "log",
        "pause",
        "debug",
        "jobs",
        "job",
        "source",
        "ingest",
        "reingest",
        "forget",
        "price",
        "cost",
        "par",
    }:
        return True
    if root in {
        "approve",
        "hold",
        "reject",
        "setname",
        "setyield",
        "setstation",
        "setmethod",
        "seting",
        "adding",
        "deling",
        "noteing",
    }:
        return True
    if root == "recipe" and first in {"activate", "deactivate"}:
        return True
    if root == "inv" and first in {"set", "add", "cost"}:
        return True
    if root == "vendor" and first == "new":
        return True
    if root == "order" and first == "clear":
        return True
    if root == "prep" and (
        first in {"add", "assign", "hold", "done"} or (first == "clear" and second == "done")
    ):
        return True
    spec = get_root_spec(root)
    return bool(spec.admin_only) if spec else False


async def handle_command_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    if not update.message or not update.message.text:
        return

    command_text = update.message.text.strip().lower()
    if command_text in {"/yes", "/no"} and (
        context.user_data.get(PENDING_OPS_COST_KEY)
        or context.user_data.get(PENDING_OPS_CONFIRM_KEY)
    ):
        handled = await _process_pending_ops_followup(update, context, command_text)
        if handled:
            return

    req = commands.parse_command(update.message.text)
    if not req:
        return

    user = update.effective_user
    chat_obj = update.effective_chat
    telegram_user_id = int(user.id) if user else 0
    display_name = (user.full_name if user else "unknown").strip()
    chat_id = int(chat_obj.id) if chat_obj else telegram_user_id

    # Telegram allows command aliases with @bot_name suffix.
    req.name = req.name.split("@", 1)[0]
    req.name = resolve_root(req.name)
    if req.name not in known_roots():
        await update.message.reply_text("Unknown command. /help")
        return

    if _request_requires_admin(req) and not is_admin(update):
        await update.message.reply_text("Not allowed.")
        return

    stop_event = asyncio.Event()
    typing_task = asyncio.create_task(_typing_loop(context, chat_id, stop_event))
    try:
        response = commands.execute_command(
            req,
            context.chat_data,
            telegram_chat_id=chat_id,
            telegram_user_id=telegram_user_id,
            display_name=display_name or "Chef",
        )
    except Exception as exc:
        logger.exception("Command dispatch failed: %s", exc)
        response = commands.CommandResponse(text="Command failed.")
    finally:
        stop_event.set()
        await typing_task

    if response.needs_vendor_selection and response.pending_order is not None:
        await _prompt_vendor_choice_for_order(
            update,
            context,
            parsed=response.pending_order,
            candidates=response.vendor_candidates or [],
        )
        return

    if response.silent:
        return

    if response.text:
        chunks = _split_for_telegram(response.text)
        if not chunks:
            chunks = [response.text]
        for chunk in chunks:
            await update.message.reply_text(chunk)


async def handle_unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    if not update.message:
        return
    await update.message.reply_text("Unknown command. /help")


async def _registry_dispatch_handler(
    update: Update, context: ContextTypes.DEFAULT_TYPE, args: List[str]
) -> None:
    _ = args
    await handle_command_message(update, context)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    if not update.message or not update.message.text:
        return

    text = update.message.text.strip()
    logger.info("Received text message: %s...", text[:50])

    user = update.effective_user
    chat_obj = update.effective_chat
    telegram_user_id = int(user.id) if user else 0
    display_name = (user.full_name if user else "unknown").strip()
    chat_id = int(chat_obj.id) if chat_obj else telegram_user_id

    pending_invoice_ingest_id = context.user_data.get(PENDING_INVOICE_VENDOR_KEY)
    if pending_invoice_ingest_id:
        result = invoice_ingest.create_vendor_and_assign(
            invoice_ingest_id=int(pending_invoice_ingest_id),
            vendor_name=text,
        )
        context.user_data.pop(PENDING_INVOICE_VENDOR_KEY, None)
        if not result.get("ok"):
            await update.message.reply_text("Action required: vendor assignment failed.")
        return

    handled_pending_ops = await _process_pending_ops_followup(update, context, text)
    if handled_pending_ops:
        return

    handled_ops = await _process_ops_intent_text(update, context, text)
    if handled_ops:
        return

    handled_prep = await _process_prep_update_text(update, context, text)
    if handled_prep:
        return

    if ordering.is_order_intent_text(text):
        handled = await _process_order_text(update, context, text, quiet_success=True)
        if handled:
            return

    get_or_create_user(telegram_user_id, display_name)
    session_id = get_or_create_active_session(chat_id, telegram_user_id)

    add_message(session_id, "user", text)

    stop_event = asyncio.Event()
    typing_task = asyncio.create_task(_typing_loop(context, chat_id, stop_event))

    try:
        history = get_recent_messages(session_id, limit=16)
        logger.info("Calling Brain with history length: %s", len(history))
        chat_mode = str(context.chat_data.get("mode") or "service").strip().lower()
        response_style = "chef_card" if chat_mode == "admin" else "concise"
        answer = chat(history, response_style=response_style, mode=chat_mode)
        logger.info("Brain Answer: %s...", answer[:50])
        formatted_answer = _format_assistant_card(answer, user_query=text)
    except Exception as exc:
        logger.error("Brain Error: %s", exc)
        formatted_answer = _build_kitchen_card_html(
            title="Brain Error",
            summary="I could not complete the response.",
            key_points=[
                "The model call failed during processing.",
                "Your message was saved in session history.",
                "Retry the same question in a moment.",
            ],
            kitchen_lines=["If this repeats, check Ollama status and logs."],
        )
    finally:
        stop_event.set()
        await typing_task

    add_message(session_id, "assistant", _to_plain_text(formatted_answer))

    try:
        await _send_html_reply(update, formatted_answer)
        logger.info("Reply sent successfully.")
    except Exception as exc:
        logger.error("Failed to send reply: %s", exc)


async def handle_voice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        if not _allowed(update):
            return
        if not update.message or not update.message.voice:
            return

        logger.info("Received voice message - Handler Fired")

        voice = update.message.voice
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")

        ogg_path = WORKDIR / f"{voice.file_unique_id}.ogg"
        wav_path = WORKDIR / f"{voice.file_unique_id}.wav"

        logger.info("Downloading voice ID: %s", voice.file_id)
        tg_file = await context.bot.get_file(voice.file_id)
        await tg_file.download_to_drive(custom_path=str(ogg_path))
        logger.info("Download successful: %s", ogg_path)

        logger.info("Converting ogg to wav: %s -> %s", ogg_path, wav_path)
        try:
            COMMAND_RUNNER.run(
                [
                    "ffmpeg",
                    "-y",
                    "-nostdin",
                    "-i",
                    str(ogg_path),
                    "-ac",
                    "1",
                    "-ar",
                    "16000",
                    "-af",
                    "highpass=f=80,lowpass=f=8000,dynaudnorm",
                    str(wav_path),
                ],
                check=True,
                capture_output=True,
            )
            logger.info("Audio conversion successful.")
        except subprocess.CalledProcessError as exc:
            logger.error("FFmpeg conversion failed: %s", exc.stderr)
            await _send_html_reply(
                update,
                _build_kitchen_card_html(
                    title="Audio Processing Error",
                    summary="I could not convert the voice note to WAV.",
                    key_points=[
                        "ffmpeg conversion failed during preprocessing.",
                        "No transcription was generated.",
                        "Please resend the voice note.",
                    ],
                    kitchen_lines=["Use a clear voice note and try again."],
                ),
            )
            return
        except FileNotFoundError:
            logger.error("FFmpeg not found in PATH.")
            await _send_html_reply(
                update,
                _build_kitchen_card_html(
                    title="Server Dependency Missing",
                    summary="ffmpeg is not available on the server.",
                    key_points=[
                        "Voice processing depends on ffmpeg.",
                        "Transcription cannot run without conversion.",
                        "Install ffmpeg and retry.",
                    ],
                    kitchen_lines=["Once ffmpeg is installed, voice notes will work."],
                ),
            )
            return

        logger.info("Starting transcription for %s", wav_path)
        text = transcribe_file(str(wav_path))
        logger.info("Transcribed: %s...", text[:50] if text else "")

        if not text or text == "(Transcription failed)":
            await _send_html_reply(
                update,
                _build_kitchen_card_html(
                    title="Transcription Failed",
                    summary="I could not transcribe the voice note.",
                    key_points=[
                        "No usable transcript was produced.",
                        "The recording may be noisy or unclear.",
                        "Please resend or type the request.",
                    ],
                    kitchen_lines=["Short, clear voice notes usually transcribe best."],
                ),
            )
            return

        handled_pending_ops = await _process_pending_ops_followup(update, context, text)
        if handled_pending_ops:
            return

        handled_ops = await _process_ops_intent_text(update, context, text)
        if handled_ops:
            return

        handled_prep = await _process_prep_update_text(update, context, text)
        if handled_prep:
            return

        user = update.effective_user
        chat_obj = update.effective_chat
        telegram_user_id = int(user.id) if user else 0
        display_name = (user.full_name if user else "unknown").strip()
        chat_id = int(chat_obj.id) if chat_obj else telegram_user_id

        get_or_create_user(telegram_user_id, display_name)
        session_id = get_or_create_active_session(chat_id, telegram_user_id)

        add_message(session_id, "user", text)

        stop_event = asyncio.Event()
        typing_task = asyncio.create_task(_typing_loop(context, chat_id, stop_event))

        try:
            history = get_recent_messages(session_id, limit=16)
            logger.info("Sending context to Ollama...")
            chat_mode = str(context.chat_data.get("mode") or "service").strip().lower()
            response_style = "chef_card" if chat_mode == "admin" else "concise"
            answer = chat(history, response_style=response_style, mode=chat_mode)
            logger.info("Brain Answer: %s...", answer[:50])
            formatted_answer = _format_assistant_card(answer, user_query=text)
        except Exception as exc:
            logger.error("Brain Error: %s", exc)
            formatted_answer = _build_kitchen_card_html(
                title="Brain Error",
                summary="I could not complete the voice request.",
                key_points=[
                    "The model call failed after transcription.",
                    "Your transcript is saved in session history.",
                    "Retry in a moment.",
                ],
                kitchen_lines=["Check Ollama and retry the same question."],
            )
        finally:
            stop_event.set()
            await typing_task

        add_message(session_id, "assistant", _to_plain_text(formatted_answer))

        try:
            await _send_html_reply(update, formatted_answer)
            logger.info("Reply sent successfully.")
        except Exception as exc:
            logger.error("Failed to send reply: %s", exc)

    except Exception as exc:
        logger.error("Error handling voice: %s", exc)
        await _send_html_reply(
            update,
            _build_kitchen_card_html(
                title="Voice Handler Error",
                summary="The voice request failed before completion.",
                key_points=[
                    "An unexpected runtime error occurred.",
                    "No final answer was generated.",
                    "Retry the same request.",
                ],
                kitchen_lines=["If this repeats, check runtime logs."],
            ),
        )
    finally:
        # Clean up temp audio files to prevent disk fill-up
        for tmp in (ogg_path, wav_path):
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass


ALLOWED_DOC_EXTENSIONS = {".pdf", ".txt", ".docx"}


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return

    if not update.message or not update.message.document:
        return

    document = update.message.document
    raw_name = document.file_name or "unknown_file"
    # Sanitize filename: strip directory components to prevent path traversal
    file_name = Path(raw_name).name
    file_size = int(document.file_size or 0)
    mime_type = document.mime_type or "unknown"
    max_mb = int(CONFIG.get("telegram", {}).get("max_document_size_mb", 50))
    max_bytes = max_mb * 1024 * 1024

    logger.info(
        "Received document: name=%s size=%s mime=%s",
        file_name,
        _fmt_bytes(file_size) if file_size else "unknown",
        mime_type,
    )

    # File extension whitelist
    suffix = Path(file_name).suffix.lower()
    if suffix not in ALLOWED_DOC_EXTENSIONS:
        await _send_html_reply(
            update,
            _build_kitchen_card_html(
                title="Unsupported File Type",
                summary=f"Only {', '.join(sorted(ALLOWED_DOC_EXTENSIONS))} files can be ingested.",
                key_points=[
                    f"File: {file_name}",
                    f"Type: {suffix or 'unknown'}",
                ],
                kitchen_lines=["Convert the file to PDF or DOCX and resend."],
            ),
        )
        logger.warning("Rejected document (unsupported type): name=%s suffix=%s", file_name, suffix)
        return

    if file_size and file_size > max_bytes:
        await _send_html_reply(
            update,
            _build_kitchen_card_html(
                title="Document Rejected",
                summary="The file is above the upload limit and was not ingested.",
                key_points=[
                    f"File: {file_name}",
                    f"Size: {_fmt_bytes(file_size)}",
                    f"Limit: {max_mb}MB",
                ],
                kitchen_lines=["Compress or split the PDF, then resend on Telegram."],
            ),
        )
        logger.warning(
            "Rejected document (too large): name=%s size=%s max=%sMB",
            file_name,
            _fmt_bytes(file_size),
            max_mb,
        )
        return

    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")

    docs_dir = resolve_path("data/documents")
    docs_dir.mkdir(parents=True, exist_ok=True)
    local_path = docs_dir / file_name

    try:
        tg_file = await context.bot.get_file(document.file_id)
        await tg_file.download_to_drive(custom_path=str(local_path))
        logger.info("Downloaded document: %s", local_path)

        from services.rag import classify_document_type

        source_title = local_path.stem.replace("_", " ").title()
        source_hint = " ".join(
            [
                source_title,
                file_name,
                str(getattr(document, "caption", "") or ""),
            ]
        ).strip()
        source_type, knowledge_tier = classify_document_type(
            title=source_title,
            source_name=file_name,
            summary=source_hint,
        )
        normalized_job_type = _job_source_type(source_type, knowledge_tier)
        queued = autonomy_service.queue_ingest_job(
            source_filename=file_name,
            source_type=normalized_job_type,
            restaurant_tag=str(context.chat_data.get("restaurant_tag") or "").strip() or None,
        )
        if not queued.get("ok"):
            await _send_html_reply(
                update,
                _build_kitchen_card_html(
                    title="Action Required",
                    summary=f"Could not queue ingest for {file_name}.",
                    key_points=["Try uploading again."],
                    kitchen_lines=[],
                ),
            )
            return

        context.chat_data["last_ingest_job_id"] = int(queued["job_id"])
        context.chat_data["last_ingest_id"] = str(queued["ingest_id"])

        if normalized_job_type == "restaurant_recipes":
            await update.message.reply_text(
                f"📥 Ingest queued: {file_name} — tracking job #{int(queued['job_id'])}"
            )
        else:
            logger.info(
                "Reference/unknown ingest queued silently: file=%s type=%s tier=%s job=%s",
                file_name,
                normalized_job_type,
                knowledge_tier,
                queued["job_id"],
            )

    except Exception as exc:
        logger.error("Error handling document: %s", exc)
        await _send_html_reply(
            update,
            _build_kitchen_card_html(
                title="Action Required",
                summary=f"Document processing failed for {file_name}.",
                key_points=[
                    f"Error: {_truncate(str(exc), 180)}",
                ],
                kitchen_lines=["Retry upload."],
            ),
        )


# --- KITCHEN OPS HANDLERS ---


async def help_chef_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return

    help_text = [
        "/inventory [item] - Check stock",
        "/count <item> <qty> [unit] - Update stock",
        "/waste <item> <qty> <reason> - Log waste",
        "/prep - Show prep list",
        "/prep add <item> - Add prep task",
        "/prep done <id> - Complete prep task",
        "/order <qty> <unit> <item> - Add routed vendor order",
        "/guide - View order guide",
        "/email vendor <id> - Build vendor draft",
        "/review vendor <id> - Show full email",
        "/send vendor <id> - Send or copy/paste draft",
        "/forget vendor <id> - Purge vendor-sensitive data",
        "/forget invoices - Purge invoice ingest data",
        "Send invoice photo - OCR ingest + vendor learning",
        "/86 <item> - Mark out of stock",
        "/cost <item> - Check item cost",
    ]

    await _send_html_reply(
        update,
        _build_kitchen_card_html(
            title="Chef Commands",
            summary="Available kitchen operation tools.",
            key_points=help_text,
            kitchen_lines=["Use these commands to manage the kitchen."],
            max_key_points=12,
        ),
    )


async def inventory_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    args = context.args
    item_name = " ".join(args).strip() if args else None

    items = ops.get_inventory_status(item_name)
    if not items:
        msg = (
            f"No inventory records found for '{item_name}'." if item_name else "Inventory is empty."
        )
        await _send_html_reply(
            update, _build_kitchen_card_html("Inventory", msg, [], ["Use /count to add items."])
        )
        return

    report = []
    for i in items:
        report.append(f"{i['name']}: {i['quantity']} {i['unit']}")

    # If too many, just show top 10 and count
    summary = f"Found {len(items)} items."
    if len(report) > 10:
        summary += f" Showing top 10."

    await _send_html_reply(
        update,
        _build_kitchen_card_html(
            "Inventory Status", summary, report[:10], ["Use /count <item> <qty> to update stock."]
        ),
    )


async def count_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    args = context.args
    if not args or len(args) < 2:
        await _send_html_reply(
            update,
            _build_kitchen_card_html("Usage Error", "Usage: /count <item> <qty> [unit]", [], []),
        )
        return

    # Try parsing: "onions 5 kg" -> name="onions", qty=5, unit="kg"
    # Or "5 kg onions" -> difficult
    # Strategy: Look for the number.

    qty = None
    unit = "unit"
    name_parts = []

    for i, arg in enumerate(args):
        try:
            val = float(arg)
            qty = val
            # If next arg exists and is not a number, it's unit?
            if i + 1 < len(args):
                possible_unit = args[i + 1]
                # If it's a known unit or short string
                if len(possible_unit) < 5 and not any(c.isdigit() for c in possible_unit):
                    unit = possible_unit
                    # name is everything else?
                    # This logic is brittle. Let's stick to strict suffix: <name> <qty> [unit]
        except ValueError:
            pass

    # Fallback to suffix parsing if complex parsing fails or isn't implemented
    try:
        qty = float(args[-1])
        name = " ".join(args[:-1])
    except ValueError:
        try:
            qty = float(args[-2])
            unit = args[-1]
            name = " ".join(args[:-2])
        except (ValueError, IndexError):
            await _send_html_reply(
                update,
                _build_kitchen_card_html(
                    "Usage Error", "Could not parse quantity. Try: /count onions 5 kg", [], []
                ),
            )
            return

    result = ops.update_inventory(name, qty, unit)
    await _send_html_reply(update, _build_kitchen_card_html("Inventory Updated", result, [], []))


async def waste_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    args = context.args
    if len(args) < 3:
        await _send_html_reply(
            update,
            _build_kitchen_card_html("Usage Error", "Usage: /waste <item> <qty> <reason>", [], []),
        )
        return

    # /waste onions 2 kg dropped -> complicated to parse
    # Let's assume: /waste <item> <qty> <reason> where reason is the last word(s)
    # Actually, let's just parse the number and assume everything before is name, everything after is reason?
    # Or simple: item first_arg, qty second_arg, reason rest.
    # But names have spaces.

    # Simpler: assume last arg is reason, 2nd to last is unit, 3rd to last is qty?
    # No, reason can be multi-word.

    # Strict format: /waste <qty> <unit> <item> REASON: <reason> ??? No.
    # Let's try: parse regex for number.

    text = " ".join(args)
    # Find the first number token
    match = re.search(r"(\d+(?:\.\d+)?)", text)
    if not match:
        await _send_html_reply(
            update, _build_kitchen_card_html("Error", "No quantity found.", [], [])
        )
        return

    qty_val = float(match.group(1))
    # split by that number
    parts = text.split(match.group(1), 1)
    name = parts[0].strip()
    remainder = parts[1].strip()

    # Remainder might be "kg dropped on floor"
    remainder_parts = remainder.split(" ", 1)
    if not remainder_parts:
        await _send_html_reply(
            update, _build_kitchen_card_html("Error", "Please provide a reason.", [], [])
        )
        return

    # Heuristic: if first word of remainder is short, it's unit
    unit = "unit"
    reason = remainder
    if len(remainder_parts[0]) <= 3:  # kg, lbs, oz
        unit = remainder_parts[0]
        reason = remainder_parts[1] if len(remainder_parts) > 1 else "waste"

    user = update.effective_user.first_name
    result = ops.log_waste(name, qty_val, reason, user)
    await _send_html_reply(update, _build_kitchen_card_html("Waste Logged", result, [], []))


async def unavailable_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    args = context.args
    if not args:
        return
    name = " ".join(args)
    result = ops.set_item_unavailable(name)
    await _send_html_reply(
        update,
        _build_kitchen_card_html("86'd Item", result, [], ["Front of house notified (mock)."]),
    )


async def prep_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    args = context.args

    if args and args[0].lower() == "add":
        # /prep add <task>
        task = " ".join(args[1:])
        if not task:
            return
        res = ops.add_prep_task(task, update.effective_user.first_name)
        await _send_html_reply(update, _build_kitchen_card_html("Prep Added", res, [], []))
        return

    if args and args[0].lower() == "done":
        # /prep done <id>
        try:
            tid = int(args[1])
            res = ops.complete_prep_task(tid)
            await _send_html_reply(update, _build_kitchen_card_html("Prep Complete", res, [], []))
        except:
            await _send_html_reply(update, _build_kitchen_card_html("Error", "Invalid ID", [], []))
        return

    if args and args[0].lower() == "clear":
        res = ops.clear_completed_prep()
        await _send_html_reply(update, _build_kitchen_card_html("Prep Cleared", res, [], []))
        return

    # List
    tasks = ops.get_prep_list("todo")
    if not tasks:
        await _send_html_reply(
            update,
            _build_kitchen_card_html(
                "Prep List", "No pending prep tasks.", [], ["/prep add <task> to create one."]
            ),
        )
        return

    points = []
    for t in tasks:
        points.append(f"[{t['id']}] {t['task']} ({t['assigned_to'] or 'unassigned'})")

    await _send_html_reply(
        update,
        _build_kitchen_card_html(
            "Prep List",
            f"{len(tasks)} tasks pending.",
            points,
            ["Use /prep done <id> to complete.", "/prep add <task> to add."],
        ),
    )


async def order_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /order <qty> <unit> <item>")
        return

    handled = await _process_order_text(update, context, text=f"order {' '.join(args)}")
    if not handled:
        await update.message.reply_text("Could not parse order. Example: /order 50# white onions")


async def guide_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    items = ordering.get_pending_orders(limit=200)
    if not items:
        await update.message.reply_text("Order guide is empty.")
        return

    points = []
    for item in items[:20]:
        qty = f"{float(item['quantity']):g}"
        vendor_name = item.get("vendor_name") or "Unassigned"
        points.append(f"{qty} {item['unit']} {item['item_name']} [{vendor_name}]")
    await _send_html_reply(
        update,
        _build_kitchen_card_html(
            "Order Guide",
            f"{len(items)} pending items.",
            points,
            ["/email vendor <id> builds a vendor draft."],
        ),
    )


async def cost_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    args = context.args
    if not args:
        return
    name = " ".join(args)
    res = ops.get_item_cost(name)
    await _send_html_reply(update, _build_kitchen_card_html("Cost Check", res, [], []))


async def recipe_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    args = context.args
    if not args:
        await _send_html_reply(update, _build_kitchen_card_html("Usage", "/recipe <name>", [], []))
        return

    # Trigger normal chat but with prefix to ensure it hits recipe logic
    query = "Recipe for: " + " ".join(args)

    # Manually invoke brain chat
    # reusing handle_text logic partially? No, just call chat directly.
    # But we need session handling?
    # Let's just create a new update.message.text and call handle_text?
    # Or cleaner: just duplicate the chat call logic briefly or refactor handle_text.
    # To save space, let's just make a synthetic call to handle_text by modifying the update object in place?
    # No, that's hacky.

    # Refactor: extract chat logic?
    # Or just call chat() here.

    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    session_id = get_or_create_active_session(chat_id, user_id)

    # Dont add user message to history, just the virtual query?
    # Or just treat it as a user message.
    add_message(session_id, "user", query)

    # Notify user we are searching
    status_msg = await update.message.reply_text("Searching for recipe...")

    try:
        history = get_recent_messages(session_id, limit=16)
        chat_mode = str(context.chat_data.get("mode") or "service").strip().lower()
        response_style = "chef_card" if chat_mode == "admin" else "concise"
        answer = chat(history, response_style=response_style, mode=chat_mode)
        fmt = _format_assistant_card(answer, user_query=query)
        add_message(session_id, "assistant", _to_plain_text(fmt))

        await context.bot.delete_message(chat_id, status_msg.message_id)
        await _send_html_reply(update, fmt)
    except Exception as exc:
        await _send_html_reply(update, _build_kitchen_card_html("Error", str(exc), [], []))


async def invoice_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    await update.message.reply_text(
        "Send invoice photo. I ingest silently unless vendor confirmation is needed."
    )


async def email_vendor_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    vendor_id = _parse_vendor_id(context.args)
    if vendor_id is None:
        await update.message.reply_text("Usage: /email vendor <id>")
        return

    draft = ordering.build_vendor_email_draft(vendor_id)
    if not draft.get("ok"):
        await update.message.reply_text(
            f"Action required: {draft.get('error', 'could not build draft')}"
        )
        return
    await update.message.reply_text(
        f"Draft ready for {draft['vendor_name']} ({draft['items_count']} items). "
        f"/review vendor {vendor_id} /send vendor {vendor_id}"
    )


async def review_vendor_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    vendor_id = _parse_vendor_id(context.args)
    if vendor_id is None:
        await update.message.reply_text("Usage: /review vendor <id>")
        return

    draft = ordering.build_vendor_email_draft(vendor_id)
    if not draft.get("ok"):
        await update.message.reply_text(
            f"Action required: {draft.get('error', 'could not build draft')}"
        )
        return

    preview = (
        f"TO: {draft.get('vendor_email') or '(vendor email not set)'}\n"
        f"SUBJECT: {draft['subject']}\n"
        "--------------------------------------------------\n"
        f"{draft['body']}\n"
        "--------------------------------------------------\n"
    )
    chunks = _split_for_telegram(preview, limit=3300)
    for chunk in chunks:
        await update.message.reply_text(f"<pre>{html.escape(chunk)}</pre>", parse_mode="HTML")


async def send_vendor_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    vendor_id = _parse_vendor_id(context.args)
    if vendor_id is None:
        await update.message.reply_text("Usage: /send vendor <id>")
        return

    result = ordering.send_vendor_draft(vendor_id)
    if not result.get("ok"):
        await update.message.reply_text(f"Action required: {result.get('error', 'send failed')}")
        return

    draft = result.get("draft", {})
    if result.get("sent"):
        await update.message.reply_text(
            f"Sent to {draft.get('vendor_name', f'vendor {vendor_id}')} ({draft.get('items_count', 0)} items)."
        )
        return

    await update.message.reply_text(
        f"SMTP unavailable. Copy/paste draft for {draft.get('vendor_name', f'vendor {vendor_id}')}. "
        f"/review vendor {vendor_id}"
    )


async def forget_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /forget vendor <id> OR /forget invoices")
        return

    target = args[0].lower()
    if target == "invoices":
        result = invoice_ingest.forget_invoices(remove_files=True)
        await update.message.reply_text(
            f"Forgot invoices: {result.get('deleted_ingests', 0)} ingests, {result.get('removed_files', 0)} files."
        )
        return

    if target == "vendor":
        vendor_id = _parse_vendor_id(args)
        if vendor_id is None:
            await update.message.reply_text("Usage: /forget vendor <id>")
            return
        result = ordering.forget_vendor(vendor_id, remove_files=True)
        await update.message.reply_text(
            f"Vendor {vendor_id} forgotten. Removed {result.get('deleted_ingests', 0)} invoices."
        )
        return

    await update.message.reply_text("Usage: /forget vendor <id> OR /forget invoices")


def run_bot() -> None:
    global AUTONOMY_WORKER
    env_path = Path(".") / ".env"
    load_dotenv(dotenv_path=env_path)
    init_db()
    if not _acquire_bot_singleton():
        raise RuntimeError("Another Prep-Brain bot instance is already running.")
    bind_default_handler(_registry_dispatch_handler)
    registry_issues = validate_registry()
    if registry_issues:
        for issue in registry_issues:
            logger.error("Command registry issue: %s", issue)
    commands.validate_command_registry_handlers()
    logger.info("Prep-Brain bot DB path: %s", str(memory_service.get_db_path()))

    env_var_name = CONFIG["telegram"].get("bot_token_env_var", "TELEGRAM_BOT_TOKEN")
    token = os.getenv(env_var_name, "").strip()
    if not token:
        raise RuntimeError(f"{env_var_name} is not set. Put it in your .env file.")

    request = HTTPXRequest(
        connect_timeout=30,
        read_timeout=120,
        write_timeout=120,
        pool_timeout=30,
    )

    defaults = Defaults(parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    app = (
        Application.builder()
        .token(token)
        .request(request)
        .defaults(defaults)
        .post_init(_app_post_init)
        .post_shutdown(_app_post_shutdown)
        .build()
    )

    app.add_handler(CallbackQueryHandler(handle_inline_callbacks))
    for root in sorted(known_roots()):
        app.add_handler(CommandHandler(root, handle_command_message))
    app.add_handler(MessageHandler(filters.COMMAND, handle_unknown_command))
    app.add_handler(MessageHandler(filters.VOICE, handle_voice))
    app.add_handler(MessageHandler(filters.PHOTO, handle_invoice_photo))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    if app.job_queue is not None:
        app.job_queue.run_repeating(cutoff_reminder_job, interval=300, first=20)
    else:
        logger.warning(
            "Job queue unavailable; cutoff reminders disabled. Autonomy still runs in background task."
        )

    try:
        app.run_polling(close_loop=False)
    finally:
        _release_bot_singleton()


if __name__ == "__main__":
    run_bot()
