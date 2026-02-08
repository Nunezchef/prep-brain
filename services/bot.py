import asyncio
import html
import logging
import os
import re
import subprocess
from pathlib import Path
from typing import List, Optional

import yaml
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, ContextTypes, MessageHandler, filters
from telegram.request import HTTPXRequest

from services.brain import chat
from services.memory import (
    add_message,
    get_or_create_active_session,
    get_or_create_user,
    get_recent_messages,
    init_db,
)
from services.transcriber import transcribe_file


def load_config() -> dict:
    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)


CONFIG = load_config()

WORKDIR = Path("./data/tmp")
WORKDIR.mkdir(parents=True, exist_ok=True)

TELEGRAM_MESSAGE_LIMIT = 3500
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


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def _fmt_bytes(num_bytes: int) -> str:
    mb = num_bytes / (1024 * 1024)
    return f"{mb:.2f}MB"


def _allowed(update: Update) -> bool:
    allowed_ids = set(CONFIG["telegram"].get("allowed_user_ids", []))
    if not allowed_ids:
        env_allowed = os.getenv("TELEGRAM_ALLOWED_USER_IDS", "").strip()
        if env_allowed:
            try:
                allowed_ids.update({int(x.strip()) for x in env_allowed.split(",") if x.strip()})
            except ValueError:
                pass

    if not allowed_ids:
        return True

    user = update.effective_user
    return bool(user and user.id in allowed_ids)


async def _typing_loop(context: ContextTypes.DEFAULT_TYPE, chat_id: int, stop_event: asyncio.Event) -> None:
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

    if ask:
        ask_line = " ".join(ask.split()).strip()
        if ask_line:
            lines.extend(["", f"<i>Ask:</i> {html.escape(_truncate(ask_line, 140))}"])

    return "\n".join(lines).strip()


def _extract_sentences(text: str) -> List[str]:
    if not text:
        return []
    parts = re.split(r"(?<=[.!?])\s+", " ".join(text.split()))
    return [part.strip() for part in parts if part.strip()]


def _looks_like_card(text: str) -> bool:
    lowered = (text or "").lower()
    return "<b>" in lowered and "key points" in lowered and "in the kitchen" in lowered


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

    candidate_lines = [line.strip() for line in _strip_markdown(raw_text).split("\n") if line.strip()]
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
    section_header_pattern = re.compile(r"^(?:base recipe|ingredients?|key points|method|in the kitchen)\b", re.I)
    quantity_hint_pattern = re.compile(r"(\d|ml|l\b|g\b|kg\b|oz\b|lb\b|tbsp|tsp|cup|%|xgum|xanthan)", re.I)

    ingredient_lines: List[str] = []
    in_base_section = False
    method_lines: List[str] = []
    in_method_section = False

    for line in lines:
        normalized = re.sub(r"<[^>]+>", "", line).strip()
        lowered = normalized.lower().strip(":")

        if lowered.startswith("base recipe") or lowered.startswith("ingredients") or lowered.startswith("key points"):
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
    if _is_component_recipe_query(user_query):
        return _format_component_recipe_html(raw_text=raw_text, user_query=user_query)

    if _looks_like_card(raw_text):
        return (raw_text or "").strip()

    cleaned = _strip_markdown(raw_text)
    if not cleaned:
        return _build_kitchen_card_html(
            title="Kitchen Note",
            summary="I could not generate a response.",
            key_points=[
                "Try the question again with one clear goal.",
                "Include dish, station, or service context.",
                "Keep constraints explicit.",
            ],
            kitchen_lines=["I can answer quickly once the target is specific."],
        )

    lines = [line.strip() for line in cleaned.split("\n") if line.strip()]
    bullet_pattern = re.compile(r"^(?:[-•*]|\d+[\.)])\s+(.+)$")
    title = "Kitchen Note"
    for line in lines:
        candidate = re.sub(r"^[\-•*\d\.)\s]+", "", line).strip(" :-")
        if candidate and len(candidate.split()) <= 12:
            title = _truncate(candidate, 60).title()
            break

    non_bullet_lines = [
        line
        for line in lines
        if not bullet_pattern.match(line)
        and not line.lower().startswith("key points")
        and not line.lower().startswith("in the kitchen")
    ]
    sentence_source = " ".join(non_bullet_lines) if non_bullet_lines else cleaned
    sentences = _extract_sentences(sentence_source)
    summary = sentences[0] if sentences else cleaned

    key_points: List[str] = []
    for line in lines:
        match = bullet_pattern.match(line)
        if match:
            key_points.append(match.group(1).strip())

    if not key_points:
        key_points.extend(sentences[1:6])

    if not key_points:
        key_points.extend(lines[1:6])

    deduped_points: List[str] = []
    seen = set()
    for point in key_points:
        normalized = " ".join(point.split()).strip()
        lowered = normalized.lower()
        if normalized and lowered not in seen:
            deduped_points.append(normalized)
            seen.add(lowered)

    fallback_points = [
        "Clarify quantities, timing, and constraints early.",
        "Keep station workflow and prep order explicit.",
        "Confirm edge cases before service.",
    ]
    for fallback in fallback_points:
        if len(deduped_points) >= 3:
            break
        if fallback.lower() not in seen:
            deduped_points.append(fallback)
            seen.add(fallback.lower())

    kitchen_lines: List[str] = []
    for idx, line in enumerate(lines):
        if line.lower().startswith("in the kitchen"):
            after_colon = line.split(":", 1)[1].strip() if ":" in line else ""
            if after_colon:
                kitchen_lines.append(after_colon)
            for next_line in lines[idx + 1 : idx + 3]:
                if bullet_pattern.match(next_line):
                    continue
                if next_line.lower().startswith("key points"):
                    continue
                if next_line.lower().startswith("in the kitchen"):
                    continue
                kitchen_lines.append(next_line)
            break

    if not kitchen_lines:
        for line in non_bullet_lines[1:3]:
            kitchen_lines.append(line)

    if not kitchen_lines:
        kitchen_lines = [
            "Apply this directly to prep planning and service timing.",
            "Adjust by station load and team constraints.",
        ]

    ask_line: Optional[str] = None
    for sentence in sentences[1:]:
        if sentence.endswith("?"):
            ask_line = sentence
            break

    return _build_kitchen_card_html(
        title=title,
        summary=summary,
        key_points=deduped_points[:5],
        kitchen_lines=kitchen_lines[:2],
        ask=ask_line,
    )


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
            logger.warning("Failed to send formatted HTML reply, falling back to escaped text: %s", exc)
            safe_chunk = html.escape(_to_plain_text(chunk))
            await update.message.reply_text(
                safe_chunk,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )


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

    get_or_create_user(telegram_user_id, display_name)
    session_id = get_or_create_active_session(chat_id, telegram_user_id)

    add_message(session_id, "user", text)

    stop_event = asyncio.Event()
    typing_task = asyncio.create_task(_typing_loop(context, chat_id, stop_event))

    try:
        history = get_recent_messages(session_id, limit=16)
        logger.info("Calling Brain with history length: %s", len(history))
        answer = chat(history)
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
            subprocess.run(
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
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            logger.info("Audio conversion successful.")
        except subprocess.CalledProcessError as exc:
            logger.error("FFmpeg conversion failed: %s", exc.stderr.decode())
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
            answer = chat(history)
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


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return

    if not update.message or not update.message.document:
        return

    document = update.message.document
    file_name = document.file_name or "unknown_file"
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

    docs_dir = Path("./data/documents")
    docs_dir.mkdir(parents=True, exist_ok=True)
    local_path = docs_dir / file_name

    try:
        tg_file = await context.bot.get_file(document.file_id)
        await tg_file.download_to_drive(custom_path=str(local_path))
        logger.info("Downloaded document: %s", local_path)

        await _send_html_reply(
            update,
            _build_kitchen_card_html(
                title="Ingestion Started",
                summary=f"Processing {file_name} for knowledge indexing.",
                key_points=[
                    f"Type: {mime_type}",
                    f"Size: {_fmt_bytes(file_size) if file_size else 'unknown'}",
                    "Pipeline: extract -> OCR(if needed) -> chunks -> embeddings",
                ],
                kitchen_lines=["I will return results when indexing is complete."],
            ),
        )

        from services.rag import rag_engine

        source_title = local_path.stem.replace("_", " ").title()
        extra_metadata = {
            "source_title": source_title,
            "source_type": "document",
            "summary": "Content indexed for semantic retrieval.",
        }

        rag_cfg = CONFIG.get("rag", {})
        ingestion_options = {
            "extract_images": bool(rag_cfg.get("image_processing", {}).get("extract_images", False)),
            "vision_descriptions": bool(rag_cfg.get("vision", {}).get("enabled", False)),
        }

        success, result = rag_engine.ingest_file(
            str(local_path),
            extra_metadata=extra_metadata,
            ingestion_options=ingestion_options,
        )

        if success:
            num_chunks = int(result.get("num_chunks", 0))
            title = result.get("source_title", source_title)
            ocr_applied = bool(result.get("ocr_applied", False))
            image_rich = bool(result.get("image_rich", False))
            images_extracted = int(result.get("images_extracted", 0))
            vision_desc = int(result.get("vision_descriptions_count", 0))
            warnings = result.get("warnings", []) or []

            key_points = [
                f"Entries added: {num_chunks}",
                f"OCR applied: {'yes' if ocr_applied else 'no'}",
                f"Image-rich source: {'yes' if image_rich else 'no'}",
            ]
            if images_extracted:
                key_points.append(f"Extracted images: {images_extracted}")
            if vision_desc:
                key_points.append(f"Vision descriptions indexed: {vision_desc}")

            await _send_html_reply(
                update,
                _build_kitchen_card_html(
                    title="Knowledge Added",
                    summary=f"{title} is now indexed and available for retrieval.",
                    key_points=key_points,
                    kitchen_lines=[
                        "Ask direct questions and I will pull matching chunks from this source.",
                        "Use source-specific questions to get tighter retrieval.",
                    ],
                ),
            )

            if warnings:
                await _send_html_reply(
                    update,
                    _build_kitchen_card_html(
                        title="Ingestion Notes",
                        summary="The document was indexed with warnings.",
                        key_points=[str(w) for w in warnings[:3]],
                        kitchen_lines=["Review notes if retrieval quality looks off."],
                    ),
                )
        else:
            error_msg = result if isinstance(result, str) else "Unknown error"
            logger.warning("Ingestion failed for %s: %s", file_name, error_msg)

            lowered = error_msg.lower()
            if "ocrmypdf" in lowered or "ocr required" in lowered:
                await _send_html_reply(
                    update,
                    _build_kitchen_card_html(
                        title="OCR Required",
                        summary="Ingestion was blocked because this PDF needs OCR.",
                        key_points=[
                            "This file appears image-heavy or scanned.",
                            "OCR is mandatory before indexing.",
                            "Recommended command: ocrmypdf --skip-text input.pdf output_ocr.pdf",
                        ],
                        kitchen_lines=["Run OCR, then resend the OCRed file in Telegram."],
                    ),
                )
            else:
                await _send_html_reply(
                    update,
                    _build_kitchen_card_html(
                        title="Ingestion Failed",
                        summary="The document could not be indexed.",
                        key_points=[
                            f"File: {file_name}",
                            f"Reason: {_truncate(error_msg, 220)}",
                            "No knowledge entries were added.",
                        ],
                        kitchen_lines=["Fix the issue and resend the document."],
                    ),
                )

    except Exception as exc:
        logger.error("Error handling document: %s", exc)
        await _send_html_reply(
            update,
            _build_kitchen_card_html(
                title="Document Handler Error",
                summary="The document request failed before completion.",
                key_points=[
                    f"File: {file_name}",
                    f"Error: {_truncate(str(exc), 220)}",
                    "No final ingestion result was produced.",
                ],
                kitchen_lines=["Retry upload after checking service logs."],
            ),
        )


def run_bot() -> None:
    env_path = Path(".") / ".env"
    load_dotenv(dotenv_path=env_path)
    init_db()

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

    app = Application.builder().token(token).request(request).build()

    app.add_handler(MessageHandler(filters.VOICE, handle_voice))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    run_bot()
