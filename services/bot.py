import os
import asyncio
import subprocess
from telegram.request import HTTPXRequest
from pathlib import Path

from telegram import Update
from telegram.ext import Application, ContextTypes, MessageHandler, filters

from services.transcriber import transcribe_file
from services.memory import (
    init_db,
    get_or_create_user,
    get_or_create_active_session,
    add_message,
    get_recent_messages,
)
from services.brain import chat
import yaml

def load_config():
    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)

CONFIG = load_config()

WORKDIR = Path("./data/tmp")
WORKDIR.mkdir(parents=True, exist_ok=True)


def _allowed(update: Update) -> bool:
    allowed_ids = set(CONFIG["telegram"].get("allowed_user_ids", []))
    if not allowed_ids:
        # Fallback to env var if config is empty, or just allow all?
        # Let's keep existing env var logic as fallback or override?
        # The prompt asked for config to drive this.
        # But let's check config first.
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
    user = update.effective_user
    return bool(user and user.id in allowed_ids)


async def _typing_loop(context: ContextTypes.DEFAULT_TYPE, chat_id: int, stop_event: asyncio.Event) -> None:
    """Keeps Telegram 'typing…' indicator alive while we do slow work."""
    while not stop_event.is_set():
        await context.bot.send_chat_action(chat_id=chat_id, action="typing")
        await asyncio.sleep(4)


import logging

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    if not update.message or not update.message.text:
        return

    text = update.message.text.strip()
    logger.info(f"Received text message: {text[:50]}...")

    # Identify user/session
    user = update.effective_user
    chat_obj = update.effective_chat
    telegram_user_id = int(user.id) if user else 0
    display_name = (user.full_name if user else "unknown").strip()
    chat_id = int(chat_obj.id) if chat_obj else telegram_user_id

    get_or_create_user(telegram_user_id, display_name)
    session_id = get_or_create_active_session(chat_id, telegram_user_id)

    # Store user message
    add_message(session_id, "user", text)

    # Ask Ollama with typing indicator
    stop_event = asyncio.Event()
    typing_task = asyncio.create_task(_typing_loop(context, chat_id, stop_event))

    try:
        history = get_recent_messages(session_id, limit=16)
        logger.info(f"Calling Brain with history length: {len(history)}")
        answer = chat(history)
        logger.info(f"Brain Answer: {answer[:50]}...")
    except Exception as e:
        logger.error(f"Brain Error: {e}")
        answer = "⚠️ Brain Error"
    finally:
        stop_event.set()
        await typing_task

    # Store assistant reply
    add_message(session_id, "assistant", answer)

    # Reply
    try:
        await update.message.reply_text(answer)
        logger.info("Reply sent successfully.")
    except Exception as e:
        logger.error(f"Failed to send reply: {e}")


async def handle_voice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        if not _allowed(update):
            return
        if not update.message or not update.message.voice:
            return
        
        logger.info("Received voice message - Handler Fired")

        voice = update.message.voice
        
        # Send typing action immediately
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")

        ogg_path = WORKDIR / f"{voice.file_unique_id}.ogg"
        wav_path = WORKDIR / f"{voice.file_unique_id}.wav"

        logger.info(f"Downloading voice ID: {voice.file_id}")
        tg_file = await context.bot.get_file(voice.file_id)
        
        # Download
        await tg_file.download_to_drive(custom_path=str(ogg_path))
        logger.info(f"Download successful: {ogg_path}")

        logger.info(f"Converting ogg to wav: {ogg_path} -> {wav_path}")
        
        # Convert Telegram opus/ogg -> 16k mono wav (+ mild cleanup)
        try:
            subprocess.run(
                [
                    "ffmpeg", "-y", "-nostdin",
                    "-i", str(ogg_path),
                    "-ac", "1",
                    "-ar", "16000",
                    "-af", "highpass=f=80,lowpass=f=8000,dynaudnorm",
                    str(wav_path),
                ],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            logger.info("Audio conversion successful.")
        except subprocess.CalledProcessError as e:
            logger.error(f"FFmpeg conversion failed: {e.stderr.decode()}")
            await update.message.reply_text("⚠️ Error processing audio (conversion failed).")
            return
        except FileNotFoundError:
             logger.error("FFmpeg not found in PATH.")
             await update.message.reply_text("⚠️ Error: ffmpeg tools not installed on server.")
             return

        # 1) Transcribe
        logger.info(f"Starting transcription for {wav_path}")
        text = transcribe_file(str(wav_path))
        logger.info(f"Transcribed: {text[:50]}...")

        if not text or text == "(Transcription failed)":
             await update.message.reply_text("⚠️ Could not transcribe audio.")
             return

        # 2) Identify user/session
        user = update.effective_user
        chat_obj = update.effective_chat
        telegram_user_id = int(user.id) if user else 0
        display_name = (user.full_name if user else "unknown").strip()
        chat_id = int(chat_obj.id) if chat_obj else telegram_user_id

        get_or_create_user(telegram_user_id, display_name)
        session_id = get_or_create_active_session(chat_id, telegram_user_id)

        # 3) Store user message
        add_message(session_id, "user", text)

        # 4) Ask Ollama with typing indicator (no extra message)
        stop_event = asyncio.Event()
        typing_task = asyncio.create_task(_typing_loop(context, chat_id, stop_event))

        try:
            history = get_recent_messages(session_id, limit=16)
            logger.info("Sending context to Ollama...")
            answer = chat(history)
            logger.info(f"Brain Answer: {answer[:50]}...")
        except Exception as e:
            logger.error(f"Brain Error: {e}")
            answer = "⚠️ Brain Error (Ollama)"
        finally:
            stop_event.set()
            await typing_task

        # 5) Store assistant reply
        add_message(session_id, "assistant", answer)

        # 6) Reply with brain answer
        try:
            await update.message.reply_text(answer)
            logger.info("Reply sent successfully.")
        except Exception as e:
            logger.error(f"Failed to send reply: {e}")

    except Exception as e:
        logger.error(f"Error handling voice: {e}")
        await update.message.reply_text("⚠️ Error processing voice message.")

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        return
    
    document = update.message.document
    if not document:
        return

    # Check file size (e.g. limit to 20MB)
    if document.file_size and document.file_size > 20 * 1024 * 1024:
        await update.message.reply_text("⚠️ File too large. Max 20MB.")
        return

    file_name = document.file_name or "unknown_file"
    
    # Send typing action
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    
    # Setup paths
    docs_dir = Path("./data/documents")
    docs_dir.mkdir(parents=True, exist_ok=True)
    local_path = docs_dir / file_name
    
    try:
        tg_file = await context.bot.get_file(document.file_id)
        await tg_file.download_to_drive(custom_path=str(local_path))
        logger.info(f"Downloaded document: {local_path}")
        
        await update.message.reply_text(f"📥 Processing {file_name}...")
        
        # Trigger RAG Ingestion
        from services.rag import rag_engine
        
        # Determine metadata & summary
        source_title = local_path.stem.replace("_", " ").title()
        
        description_bullets = (
            "• Content indexed for semantic retrieval\n"
            "• Available for context-aware queries"
        )
        impact_text = "This document is now available for context retrieval."
        learned_summary = "Content indexed for semantic retrieval."

        extra_metadata = {
            "source_title": source_title,
            "source_type": "document",
            "summary": learned_summary
        }

        success, result = rag_engine.ingest_file(str(local_path), extra_metadata=extra_metadata)
        
        if success:
            num_chunks = result.get("num_chunks", 0)
            title = result.get("source_title", source_title)

            msg = (
                f"📘 **Knowledge added: {title}**\n\n"
                f"{description_bullets}\n\n"
                f"Entries added: **{num_chunks}**\n"
                f"{impact_text}"
            )
            
            await update.message.reply_text(msg, parse_mode="Markdown")
        else:
             error_msg = result if isinstance(result, str) else "Unknown error"
             await update.message.reply_text(f"⚠️ Ingestion failed: {error_msg}")
             
    except Exception as e:
        logger.error(f"Error handling document: {e}")
        await update.message.reply_text(f"⚠️ Error processing document: {e}")

from telegram.request import HTTPXRequest

from dotenv import load_dotenv

def run_bot() -> None:
    # Explicitly load .env from project root
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

    app = (
        Application.builder()
        .token(token)
        .request(request)
        .build()
    )

    app.add_handler(MessageHandler(filters.VOICE, handle_voice))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    run_bot()