import json
import os
import platform
import signal
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import psutil
import yaml
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from services.brain import chat
from services import autonomy as autonomy_service
from services import lexicon
from services import memory
from services.transcriber import transcribe_file
from prep_brain.config import load_config as pb_load_config

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config.yaml"
APP_STARTED_AT = int(time.time())
memory.init_db()

app = FastAPI(title="Prep-Brain Dashboard API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://0.0.0.0:5173",
        "http://frontend:5173",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)


class ToggleSourcePayload(BaseModel):
    active: bool


class BrainTestPayload(BaseModel):
    prompt: str


def load_config() -> Dict[str, Any]:
    return pb_load_config()


def save_config(config_data: Dict[str, Any]) -> None:
    CONFIG_PATH.write_text(yaml.dump(config_data, sort_keys=False, allow_unicode=True))


def get_path_from_config(name: str, default: str) -> Path:
    config = load_config()
    configured = config.get("paths", {}).get(name, default)
    candidate = Path(configured)
    if not candidate.is_absolute():
        candidate = BASE_DIR / candidate
    return candidate


def get_pid_file() -> Path:
    return get_path_from_config("pid_file", "run/prep-brain.pid")


def get_log_file() -> Path:
    return get_path_from_config("log_file", "logs/prep-brain.log")


def get_ollama_log_file() -> Path:
    return get_path_from_config("ollama_log_file", "logs/ollama.log")


def get_memory_db_path() -> Path:
    return memory.get_db_path()


def parse_log_line(raw_line: str) -> Dict[str, str]:
    line = raw_line.strip()
    if not line:
        return {"ts": "", "message": "", "raw": ""}

    parts = line.split(" - ")
    if len(parts) >= 4:
        return {
            "ts": parts[0].strip(),
            "message": parts[3].strip(),
            "raw": line,
        }

    return {
        "ts": "",
        "message": line,
        "raw": line,
    }


def get_bot_status() -> Dict[str, Any]:
    if os.environ.get("PREP_BRAIN_BOT_CONTROL", "1").strip().lower() in {"0", "false", "no"}:
        snapshot = autonomy_service.get_autonomy_status_snapshot()
        running = bool(snapshot.get("is_running"))
        return {
            "status": "Running" if running else "Stopped",
            "running": running,
            "pid": None,
            "managed_externally": True,
        }

    pid_file = get_pid_file()
    if not pid_file.exists():
        return {"status": "Stopped", "running": False, "pid": None}

    try:
        pid = int(pid_file.read_text().strip())
        os.kill(pid, 0)
        return {"status": "Running", "running": True, "pid": pid}
    except (ValueError, OSError, ProcessLookupError):
        return {"status": "Error (Stale PID)", "running": False, "pid": None}


def start_bot() -> Dict[str, Any]:
    if os.environ.get("PREP_BRAIN_BOT_CONTROL", "1").strip().lower() in {"0", "false", "no"}:
        return {
            "changed": False,
            "message": "Bot is managed by container startup.",
            "status": get_bot_status(),
        }

    current = get_bot_status()
    if current["running"]:
        return {"changed": False, "message": "Bot already running.", "status": current}

    pid_file = get_pid_file()
    log_file = get_log_file()
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    cmd = [sys.executable, "-m", "prep_brain.app"]

    try:
        with open(log_file, "a") as log:
            process = subprocess.Popen(
                cmd,
                stdout=log,
                stderr=log,
                cwd=BASE_DIR,
                preexec_fn=os.setpgrp if os.name != "nt" else None,
            )
            pid_file.write_text(str(process.pid))
    except Exception as e:
        print(f"Failed to start bot: {e}")
        return {
            "changed": False,
            "message": f"Failed to start bot: {e}",
            "status": get_bot_status(),
        }

    time.sleep(0.8)
    return {
        "changed": True,
        "message": "Bot start command sent.",
        "status": get_bot_status(),
    }


def stop_bot() -> Dict[str, Any]:
    if os.environ.get("PREP_BRAIN_BOT_CONTROL", "1").strip().lower() in {"0", "false", "no"}:
        return {
            "changed": False,
            "message": "Bot is managed by container startup.",
            "status": get_bot_status(),
        }

    status = get_bot_status()
    pid = status.get("pid")
    pid_file = get_pid_file()

    if not pid:
        if pid_file.exists():
            pid_file.unlink()
        return {"changed": False, "message": "Bot was not running.", "status": get_bot_status()}

    try:
        os.kill(pid, signal.SIGTERM)
        time.sleep(0.8)
    except ProcessLookupError:
        pass

    try:
        os.kill(pid, 0)
        os.kill(pid, signal.SIGKILL)
    except OSError:
        pass

    if pid_file.exists():
        pid_file.unlink()

    return {
        "changed": True,
        "message": "Bot stop command sent.",
        "status": get_bot_status(),
    }


def restart_bot() -> Dict[str, Any]:
    if os.environ.get("PREP_BRAIN_BOT_CONTROL", "1").strip().lower() in {"0", "false", "no"}:
        return {
            "changed": False,
            "message": "Bot is managed by container startup.",
            "status": get_bot_status(),
        }

    stop_bot()
    time.sleep(0.4)
    return start_bot()


def get_ollama_status() -> Dict[str, Any]:
    # In Docker, we check the host URL
    import requests

    try:
        # Check if Ollama is responsive
        base_url = os.environ.get("OLLAMA_URL") or load_config().get("ollama", {}).get(
            "base_url", "http://localhost:11434"
        )
        resp = requests.get(base_url, timeout=1)
        if resp.status_code == 200:
            return {
                "status": "Running (Host)",
                "running": True,
                "pids": [],
            }  # PIDs irrelevant for remote
    except Exception:
        pass
    return {"status": "Stopped/Unreachable", "running": False, "pids": []}


def start_ollama() -> Dict[str, Any]:
    # We cannot start a host process from Docker easily.
    # We check if it is running, and if not, we return a message telling user to run it on host.
    status = get_ollama_status()
    if status["running"]:
        return {"changed": False, "message": "Ollama is running on host.", "status": status}

    return {
        "changed": False,
        "message": "Cannot start host Ollama from Docker. Run 'ollama serve' on your Mac.",
        "status": status,
    }


def stop_ollama() -> Dict[str, Any]:
    # We cannot stop a host process from Docker easily.
    return {
        "changed": False,
        "message": "Cannot stop host Ollama from Docker. Stop it manually on your Mac.",
        "status": get_ollama_status(),
    }


def get_autonomy_status() -> Dict[str, Any]:
    snapshot = autonomy_service.get_autonomy_status_snapshot()
    running = bool(snapshot.get("is_running"))

    db_path = get_memory_db_path()
    error_count = 0
    if db_path.exists():
        con = sqlite3.connect(db_path)
        try:
            row = con.execute(
                "SELECT COUNT(*) FROM autonomy_log WHERE action LIKE '%error%'"
            ).fetchone()
            error_count = int(row[0]) if row and row[0] is not None else 0
        finally:
            con.close()

    return {
        "status": "Running" if running else "Waiting for heartbeat",
        "running": running,
        "is_always_on": True,
        "last_tick_at": snapshot.get("last_tick_at"),
        "last_cycle_started_at": snapshot.get("last_cycle_started_at"),
        "last_cycle_finished_at": snapshot.get("last_cycle_finished_at"),
        "last_action": snapshot.get("last_action"),
        "last_error": snapshot.get("last_error"),
        "last_error_at": snapshot.get("last_error_at"),
        "queue_pending_drafts": int(snapshot.get("queue_pending_drafts") or 0),
        "queue_pending_ingests": int(snapshot.get("queue_pending_ingests") or 0),
        "last_promoted_recipe_id": snapshot.get("last_promoted_recipe_id"),
        "last_promoted_recipe_name": snapshot.get("last_promoted_recipe_name"),
        "last_promoted_at": snapshot.get("last_promoted_at"),
        "error_count": error_count,
    }


def start_autonomy() -> Dict[str, Any]:
    return {
        "changed": False,
        "message": "Autonomy is managed by bot startup and cannot be started manually.",
        "status": get_autonomy_status(),
    }


def stop_autonomy() -> Dict[str, Any]:
    return {
        "changed": False,
        "message": "Autonomy is always on while the bot is running and cannot be stopped from the dashboard.",
        "status": get_autonomy_status(),
    }


def get_autonomy_logs(limit: int = 50) -> List[Dict[str, Any]]:
    db_path = get_memory_db_path()
    if not db_path.exists():
        return []

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            "SELECT * FROM autonomy_log ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        con.close()


def get_logs(lines: int = 120, level_filter: str = "all") -> List[Dict[str, str]]:
    log_file = get_log_file()
    if not log_file.exists():
        return []

    try:
        with open(log_file, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    except OSError:
        return []

    rows = content.splitlines()[-lines:]

    filtered: List[str] = []
    if level_filter == "errors":
        filtered = [line for line in rows if "ERROR" in line.upper()]
    elif level_filter == "warnings":
        filtered = [line for line in rows if "WARNING" in line.upper()]
    else:
        filtered = rows

    return [parse_log_line(line) for line in reversed(filtered)]


def get_telemetry(bot_running: bool, ollama_running: bool) -> Dict[str, Any]:
    battery = psutil.sensors_battery() if hasattr(psutil, "sensors_battery") else None
    battery_pct = int(round(battery.percent)) if battery and battery.percent is not None else None

    temp_value: Optional[float] = None
    if hasattr(psutil, "sensors_temperatures"):
        try:
            temps = psutil.sensors_temperatures() or {}
            for entries in temps.values():
                for entry in entries:
                    if entry.current is not None:
                        temp_value = float(entry.current)
                        break
                if temp_value is not None:
                    break
        except Exception:
            temp_value = None

    core_temp_estimated = False
    if temp_value is None:
        try:
            # Fallback estimate when host sensors are unavailable (common on macOS).
            cpu = float(psutil.cpu_percent(interval=0.15))
            temp_value = 36.0 + (cpu * 0.42)
            core_temp_estimated = True
        except Exception:
            temp_value = None

    signal_value = (
        98 if (bot_running and ollama_running) else 74 if (bot_running or ollama_running) else 39
    )

    config = load_config()
    position = config.get("runtime", {}).get("position_label", "KITCHEN A2")

    return {
        "battery": battery_pct,
        "core_temp": round(temp_value, 1) if temp_value is not None else None,
        "core_temp_estimated": core_temp_estimated,
        "signal": signal_value,
        "position": position,
    }


def read_sessions() -> List[Dict[str, Any]]:
    db_path = get_memory_db_path()
    if not db_path.exists():
        return []

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    try:
        query = """
        SELECT
            s.id,
            s.title,
            s.is_active,
            s.created_at,
            s.telegram_chat_id,
            s.telegram_user_id,
            COALESCE(u.display_name, 'Unknown') AS display_name,
            (SELECT COUNT(*) FROM messages m WHERE m.session_id = s.id) AS message_count
        FROM sessions s
        LEFT JOIN users u ON s.telegram_user_id = u.telegram_user_id
        ORDER BY s.created_at DESC
        """
        rows = con.execute(query).fetchall()
        return [dict(row) for row in rows]
    finally:
        con.close()


def read_session_messages(session_id: int, limit: int = 200) -> List[Dict[str, Any]]:
    db_path = get_memory_db_path()
    if not db_path.exists():
        return []

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    try:
        rows = con.execute(
            """
            SELECT id, role, content, created_at
            FROM messages
            WHERE session_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (session_id, limit),
        ).fetchall()
        ordered = [dict(row) for row in reversed(rows)]
        return ordered
    finally:
        con.close()


def clear_session_messages(session_id: int) -> int:
    db_path = get_memory_db_path()
    if not db_path.exists():
        return 0

    con = sqlite3.connect(db_path)
    try:
        cur = con.execute("DELETE FROM messages WHERE session_id = ?", (session_id,))
        con.commit()
        return cur.rowcount
    finally:
        con.close()


def get_rag_engine():
    from services.rag import rag_engine

    return rag_engine


@app.get("/api/health")
def health() -> Dict[str, Any]:
    return {"ok": True, "started_at": APP_STARTED_AT}


@app.get("/api/status")
def status() -> Dict[str, Any]:
    bot = get_bot_status()
    ollama = get_ollama_status()
    telemetry = get_telemetry(bot_running=bot["running"], ollama_running=ollama["running"])

    return {
        "bot": bot,
        "ollama": ollama,
        "telemetry": telemetry,
        "uptime_seconds": int(time.time()) - APP_STARTED_AT,
        "processing": bool(bot["running"]),
    }


@app.post("/api/control/bot/start")
def control_bot_start() -> Dict[str, Any]:
    return start_bot()


@app.post("/api/control/bot/stop")
def control_bot_stop() -> Dict[str, Any]:
    return stop_bot()


@app.post("/api/control/bot/restart")
def control_bot_restart() -> Dict[str, Any]:
    return restart_bot()


@app.post("/api/control/ollama/start")
def control_ollama_start() -> Dict[str, Any]:
    return start_ollama()


@app.post("/api/control/ollama/stop")
def control_ollama_stop() -> Dict[str, Any]:
    return stop_ollama()


@app.get("/api/autonomy/status")
def autonomy_status() -> Dict[str, Any]:
    return get_autonomy_status()


@app.post("/api/autonomy/start")
def control_autonomy_start() -> Dict[str, Any]:
    raise HTTPException(
        status_code=409,
        detail="Autonomy is managed by bot startup and cannot be started manually.",
    )


@app.post("/api/autonomy/stop")
def control_autonomy_stop() -> Dict[str, Any]:
    raise HTTPException(
        status_code=409,
        detail="Autonomy is always on while the bot is running and cannot be stopped from the dashboard.",
    )


@app.get("/api/autonomy/logs")
def autonomy_logs(limit: int = 50) -> Dict[str, Any]:
    return {"items": get_autonomy_logs(limit=limit)}


@app.get("/api/logs")
def logs(lines: int = 120, level: str = "all") -> Dict[str, Any]:
    level_filter = level.lower().strip()
    if level_filter not in {"all", "errors", "warnings"}:
        raise HTTPException(status_code=400, detail="Invalid level filter")

    rows = get_logs(lines=max(1, min(lines, 1000)), level_filter=level_filter)
    return {"items": rows}


@app.get("/api/sessions")
def sessions() -> Dict[str, Any]:
    return {"items": read_sessions()}


@app.get("/api/sessions/{session_id}/messages")
def session_messages(session_id: int, limit: int = 200) -> Dict[str, Any]:
    return {"items": read_session_messages(session_id=session_id, limit=max(1, min(limit, 1000)))}


@app.delete("/api/sessions/{session_id}/messages")
def session_messages_clear(session_id: int) -> Dict[str, Any]:
    deleted = clear_session_messages(session_id=session_id)
    return {"deleted": deleted}


@app.get("/api/knowledge")
def knowledge() -> Dict[str, Any]:
    rag_sources = get_rag_engine().get_sources()
    rag_by_ingest = {
        str(item.get("ingest_id") or "").strip(): item
        for item in rag_sources
        if str(item.get("ingest_id") or "").strip()
    }

    db_path = get_memory_db_path()
    if not db_path.exists():
        return {"items": []}

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute("""
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
            """).fetchall()
    finally:
        con.close()

    items: List[Dict[str, Any]] = []
    for row in rows:
        raw = dict(row)
        ingest_id = str(raw.get("ingest_id") or "")
        rag_row = rag_by_ingest.get(ingest_id, {})
        source_id = str(rag_row.get("id") or "").strip()
        ingest_status = str(raw.get("status") or "unknown")
        extracted_text_chars = int(raw.get("extracted_text_chars") or 0)
        table_chars = int(rag_row.get("extracted_from_tables_chars") or 0)
        paragraph_chars = int(rag_row.get("extracted_from_paragraphs_chars") or 0)
        if extracted_text_chars >= 20000:
            text_profile_label = "TEXT-RICH"
        elif table_chars > paragraph_chars and table_chars > 0:
            text_profile_label = "TABLES ONLY"
        else:
            text_profile_label = "LOW TEXT"
        items.append(
            {
                "id": source_id or ingest_id,
                "source_id": source_id or None,
                "ingest_id": ingest_id,
                "source_name": raw.get("filename"),
                "title": rag_row.get("title")
                or Path(str(raw.get("filename") or "")).stem.replace("_", " ").title(),
                "type": rag_row.get("type") or raw.get("source_type"),
                "doc_source_type": raw.get("source_type"),
                "knowledge_tier": rag_row.get("knowledge_tier"),
                "date_ingested": raw.get("created_at"),
                "updated_at": raw.get("updated_at"),
                "chunk_count": int(raw.get("chunk_count") or 0),
                "chunks_added": int(raw.get("chunks_added") or 0),
                "extracted_text_chars": extracted_text_chars,
                "status": rag_row.get("status")
                or ("active" if ingest_status in {"ok", "warn"} and source_id else ingest_status),
                "ingest_status": ingest_status,
                "warnings": rag_row.get("warnings") or [],
                "ocr_required": bool(rag_row.get("ocr_required", False)),
                "ocr_applied": bool(rag_row.get("ocr_applied", False)),
                "image_rich": bool(rag_row.get("image_rich", False)),
                "text_profile_label": text_profile_label,
                "extracted_from_tables_chars": table_chars,
                "extracted_from_paragraphs_chars": paragraph_chars,
                "can_toggle": bool(source_id),
                "can_delete": True,
            }
        )
    return {"items": items}


@app.post("/api/knowledge/{source_id}/toggle")
def knowledge_toggle(source_id: str, payload: ToggleSourcePayload) -> Dict[str, Any]:
    engine = get_rag_engine()
    target = source_id
    ok = engine.toggle_source(target, payload.active)
    if not ok:
        # Allow ingest_id aliases for rows not currently mapped in UI.
        for row in engine.get_sources():
            ingest_id = str(row.get("ingest_id") or "").strip()
            if ingest_id and ingest_id.startswith(source_id):
                target = str(row.get("id") or "")
                ok = engine.toggle_source(target, payload.active)
                if ok:
                    break
    if not ok:
        raise HTTPException(status_code=404, detail="Source not found")
    return {"ok": True}


@app.delete("/api/knowledge/{source_id}")
def knowledge_delete(source_id: str) -> Dict[str, Any]:
    engine = get_rag_engine()
    ok = engine.delete_source(source_id)
    resolved_source_id = source_id
    resolved_ingest_id = None

    if not ok:
        for row in engine.get_sources():
            ingest_id = str(row.get("ingest_id") or "").strip()
            if ingest_id and ingest_id.startswith(source_id):
                resolved_source_id = str(row.get("id") or "")
                resolved_ingest_id = ingest_id
                ok = engine.delete_source(resolved_source_id)
                if ok:
                    break

    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    try:
        if resolved_ingest_id:
            con.execute("DELETE FROM doc_sources WHERE ingest_id = ?", (resolved_ingest_id,))
        else:
            con.execute(
                """
                DELETE FROM doc_sources
                WHERE ingest_id = ?
                   OR ingest_id LIKE ?
                """,
                (source_id, f"{source_id}%"),
            )
        con.commit()
    finally:
        con.close()

    if not ok and not resolved_ingest_id:
        # If source never reached vector store, doc_sources delete above is still valid.
        return {"ok": True}
    if not ok:
        raise HTTPException(status_code=404, detail="Source not found")
    return {"ok": True}


@app.post("/api/knowledge/upload")
async def knowledge_upload(
    file: UploadFile = File(...),
    extract_images: bool = Form(False),
    vision_descriptions: bool = Form(False),
) -> Dict[str, Any]:
    name = Path(file.filename or "uploaded").name
    suffix = Path(name).suffix.lower()

    if suffix not in {".pdf", ".txt", ".docx"}:
        raise HTTPException(
            status_code=400, detail="Only .pdf, .txt, and .docx files are supported"
        )

    target = BASE_DIR / "data" / "documents" / name
    target.parent.mkdir(parents=True, exist_ok=True)

    content = await file.read()
    target.write_bytes(content)

    title = target.stem.replace("_", " ").title()
    extra_meta = {
        "source_title": title,
        "source_type": "upload",
        "summary": f"Uploaded via React dashboard: {title}",
    }

    success, result = get_rag_engine().ingest_file(
        str(target),
        extra_metadata=extra_meta,
        ingestion_options={
            "extract_images": bool(extract_images),
            "vision_descriptions": bool(vision_descriptions),
        },
    )

    if not success:
        raise HTTPException(status_code=400, detail=str(result))

    return result


@app.post("/api/test/brain")
def test_brain(payload: BrainTestPayload) -> Dict[str, Any]:
    prompt = payload.prompt.strip()
    if not prompt:
        raise HTTPException(status_code=400, detail="Prompt is required")

    answer = chat([("user", prompt)])
    return {"answer": answer}


@app.post("/api/test/transcribe")
async def test_transcribe(file: UploadFile = File(...)) -> Dict[str, Any]:
    suffix = Path(file.filename or "sample.wav").suffix or ".wav"
    tmp_dir = BASE_DIR / "data" / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = tmp_dir / f"dashboard_test_{int(time.time() * 1000)}{suffix}"

    content = await file.read()
    tmp_path.write_bytes(content)

    try:
        text = transcribe_file(str(tmp_path))
        return {"text": text}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


@app.get("/api/config")
def config_get() -> Dict[str, Any]:
    return {"config": load_config()}


@app.put("/api/config")
def config_put(config_data: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(config_data, dict):
        raise HTTPException(status_code=400, detail="Config payload must be an object")

    ALLOWED_TOP_KEYS = {
        "memory",
        "ollama",
        "rag",
        "paths",
        "system_prompt",
        "telegram",
        "smtp",
        "runtime",
        "autonomy",
        "invoice_ingest",
        "ordering",
        "lexicon",
        "debug",
    }
    SENSITIVE_PATTERNS = {"token", "password", "secret", "key", "credential"}

    unknown_keys = set(config_data.keys()) - ALLOWED_TOP_KEYS
    if unknown_keys:
        raise HTTPException(status_code=400, detail=f"Unknown config keys: {unknown_keys}")

    # Strip any nested key that looks like a secret value (prevent secret storage in config.yaml)
    def _strip_secrets(d: Any, path: str = "") -> Any:
        if isinstance(d, dict):
            cleaned = {}
            for k, v in d.items():
                key_lower = k.lower()
                if (
                    any(p in key_lower for p in SENSITIVE_PATTERNS)
                    and isinstance(v, str)
                    and v.strip()
                ):
                    continue  # silently strip secret values
                cleaned[k] = _strip_secrets(v, f"{path}.{k}")
            return cleaned
        return d

    sanitized = _strip_secrets(config_data)
    save_config(sanitized)
    return {"ok": True, "config": load_config()}


@app.get("/api/system/info")
def system_info() -> Dict[str, Any]:
    return {
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "api_started_at": APP_STARTED_AT,
        "cwd": str(BASE_DIR),
    }


@app.get("/api/lexicon")
def lexicon_get() -> Dict[str, Any]:
    return {"lexicon": lexicon.get_lexicon_config()}


@app.put("/api/lexicon")
def lexicon_put(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Payload must be an object")
    updated = lexicon.update_lexicon_config(payload)
    return {"ok": True, "lexicon": updated}


# ==============================================================================
# Feature 1: Provider Directory (Vendors)
# ==============================================================================


class Vendor(BaseModel):
    name: str
    contact_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    ordering_window: Optional[str] = None
    cutoff_time: Optional[str] = None
    preferred_method: Optional[str] = None
    notes: Optional[str] = None


def init_db():
    db_path = get_memory_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(db_path)
    try:
        # Create vendors table
        con.execute("""
        CREATE TABLE IF NOT EXISTS vendors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            contact_name TEXT,
            email TEXT,
            phone TEXT,
            ordering_window TEXT,
            cutoff_time TEXT,
            preferred_method TEXT,
            notes TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        con.commit()
    finally:
        con.close()


# Initialize DB on import (or startup)
try:
    init_db()
except Exception as e:
    print(f"DB Init failed: {e}")


@app.get("/api/vendors")
def vendors_list() -> Dict[str, Any]:
    db_path = get_memory_db_path()
    if not db_path.exists():
        return {"items": []}

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute("SELECT * FROM vendors ORDER BY name ASC").fetchall()
        return {"items": [dict(row) for row in rows]}
    finally:
        con.close()


@app.post("/api/vendors")
def vendors_create(vendor: Vendor) -> Dict[str, Any]:
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    try:
        cur = con.execute(
            """
            INSERT INTO vendors (name, contact_name, email, phone, ordering_window, cutoff_time, preferred_method, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                vendor.name,
                vendor.contact_name,
                vendor.email,
                vendor.phone,
                vendor.ordering_window,
                vendor.cutoff_time,
                vendor.preferred_method,
                vendor.notes,
            ),
        )
        con.commit()
        return {"ok": True, "id": cur.lastrowid}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        con.close()


@app.put("/api/vendors/{vendor_id}")
def vendors_update(vendor_id: int, vendor: Vendor) -> Dict[str, Any]:
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    try:
        cur = con.execute(
            """
            UPDATE vendors 
            SET name=?, contact_name=?, email=?, phone=?, ordering_window=?, cutoff_time=?, preferred_method=?, notes=?, updated_at=CURRENT_TIMESTAMP
            WHERE id=?
        """,
            (
                vendor.name,
                vendor.contact_name,
                vendor.email,
                vendor.phone,
                vendor.ordering_window,
                vendor.cutoff_time,
                vendor.preferred_method,
                vendor.notes,
                vendor_id,
            ),
        )
        con.commit()
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Vendor not found")
        return {"ok": True}
    finally:
        con.close()


@app.delete("/api/vendors/{vendor_id}")
def vendors_delete(vendor_id: int) -> Dict[str, Any]:
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    try:
        cur = con.execute("DELETE FROM vendors WHERE id=?", (vendor_id,))
        con.commit()
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Vendor not found")
        return {"ok": True}
    finally:
        con.close()


# ==============================================================================
# Feature 3: Order Guide Builder
# ==============================================================================


class VendorItem(BaseModel):
    vendor_id: Optional[int] = None
    name: str
    item_code: Optional[str] = None
    unit: Optional[str] = None
    price: Optional[float] = None
    category: Optional[str] = None
    is_active: bool = True


def init_db_v3():
    # Helper to extend schema for Feature 3
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    try:
        con.execute("""
        CREATE TABLE IF NOT EXISTS vendor_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vendor_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            item_code TEXT,
            unit TEXT,
            price REAL,
            category TEXT,
            is_active BOOLEAN DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(vendor_id) REFERENCES vendors(id) ON DELETE CASCADE
        )
        """)
        con.commit()
    finally:
        con.close()


# Run init for v3
try:
    init_db_v3()
except Exception as e:
    print(f"DB Init V3 failed: {e}")


@app.get("/api/vendors/{vendor_id}/items")
def vendor_items_list(vendor_id: int) -> Dict[str, Any]:
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            "SELECT * FROM vendor_items WHERE vendor_id=? ORDER BY name ASC", (vendor_id,)
        ).fetchall()
        return {"items": [dict(row) for row in rows]}
    finally:
        con.close()


@app.post("/api/vendors/{vendor_id}/items")
def vendor_items_create(vendor_id: int, item: VendorItem) -> Dict[str, Any]:
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    try:
        cur = con.execute(
            """
            INSERT INTO vendor_items (vendor_id, name, item_code, unit, price, category, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            (
                vendor_id,
                item.name,
                item.item_code,
                item.unit,
                item.price,
                item.category,
                item.is_active,
            ),
        )
        con.commit()
        return {"ok": True, "id": cur.lastrowid}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        con.close()


@app.put("/api/vendors/items/{item_id}")
def vendor_items_update(item_id: int, item: VendorItem) -> Dict[str, Any]:
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    try:
        cur = con.execute(
            """
            UPDATE vendor_items 
            SET name=?, item_code=?, unit=?, price=?, category=?, is_active=?, updated_at=CURRENT_TIMESTAMP
            WHERE id=?
        """,
            (
                item.name,
                item.item_code,
                item.unit,
                item.price,
                item.category,
                item.is_active,
                item_id,
            ),
        )
        con.commit()
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Item not found")
        return {"ok": True}
    finally:
        con.close()


@app.delete("/api/vendors/items/{item_id}")
def vendor_items_delete(item_id: int) -> Dict[str, Any]:
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    try:
        cur = con.execute("DELETE FROM vendor_items WHERE id=?", (item_id,))
        con.commit()
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Item not found")
        return {"ok": True}
    finally:
        con.close()


# ==============================================================================
# Feature 4: Inventory Count Sheets
# ==============================================================================


@app.get("/api/inventory/sheets")
def inventory_sheets() -> Dict[str, Any]:
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        # Fetch all active items
        rows = con.execute("""
            SELECT id, name, unit, category 
            FROM vendor_items 
            WHERE is_active=1 
            ORDER BY category ASC, name ASC
        """).fetchall()
        return {"items": [dict(row) for row in rows]}
    finally:
        con.close()


# ==============================================================================
# Feature 9: Prep Cockpit (Recipes / Par Levels)
# ==============================================================================


class PrepItem(BaseModel):
    name: str
    category: Optional[str] = None
    unit: Optional[str] = "unit"
    par_level: float = 0.0
    on_hand: float = 0.0
    prep_time_minutes: int = 0
    station_id: Optional[int] = None
    allergens: List[str] = []
    is_active: bool = True


class Allergen(BaseModel):
    id: int
    name: str


class Station(BaseModel):
    name: str
    description: Optional[str] = None
    instructions: Optional[str] = None  # Markdown
    is_active: bool = True


class PrepAuditPayload(BaseModel):
    updates: Dict[int, float]  # {id: new_on_hand}


def init_db_v9():
    # Helper to extend schema for Feature 9
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    try:
        con.execute("""
        CREATE TABLE IF NOT EXISTS recipes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            category TEXT,
            unit TEXT DEFAULT 'unit',
            unit TEXT DEFAULT 'unit',
            unit TEXT DEFAULT 'unit',
            par_level REAL DEFAULT 0.0,
            on_hand REAL DEFAULT 0.0,
            prep_time_minutes INTEGER DEFAULT 0,
            station_id INTEGER,
            is_active BOOLEAN DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # Migrations for existing tables
        try:
            con.execute("ALTER TABLE recipes ADD COLUMN category TEXT")
        except Exception:
            pass

        try:
            con.execute("ALTER TABLE recipes ADD COLUMN unit TEXT DEFAULT 'unit'")
        except Exception:
            pass

        try:
            con.execute("ALTER TABLE recipes ADD COLUMN par_level REAL DEFAULT 0.0")
        except Exception:
            pass

        try:
            con.execute("ALTER TABLE recipes ADD COLUMN on_hand REAL DEFAULT 0.0")
        except Exception:
            pass

        try:
            con.execute("ALTER TABLE recipes ADD COLUMN prep_time_minutes INTEGER DEFAULT 0")
        except Exception:
            pass

        try:
            con.execute("ALTER TABLE recipes ADD COLUMN station_id INTEGER")
        except Exception:
            pass

        con.execute("""
        CREATE TABLE IF NOT EXISTS stations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT,
            instructions TEXT,
            is_active BOOLEAN DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)

        con.execute("""
        CREATE TABLE IF NOT EXISTS allergens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        )
        """)

        con.execute("""
        CREATE TABLE IF NOT EXISTS recipe_allergens (
            recipe_id INTEGER,
            allergen_id INTEGER,
            PRIMARY KEY (recipe_id, allergen_id),
            FOREIGN KEY(recipe_id) REFERENCES recipes(id),
            FOREIGN KEY(allergen_id) REFERENCES allergens(id)
        )
        """)

        # Seed Allergens
        count = con.execute("SELECT count(*) FROM allergens").fetchone()[0]
        if count == 0:
            defaults = [
                "Milk",
                "Eggs",
                "Fish",
                "Shellfish",
                "Tree Nuts",
                "Peanuts",
                "Wheat",
                "Soy",
                "Sesame",
            ]
            con.executemany(
                "INSERT INTO allergens (name) VALUES (?)", [(name,) for name in defaults]
            )

        con.commit()
    finally:
        con.close()


# Run init for v9
try:
    init_db_v9()
except Exception as e:
    print(f"DB Init V9 failed: {e}")


@app.get("/api/prep")
def prep_list() -> Dict[str, Any]:
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute("SELECT * FROM recipes ORDER BY category ASC, name ASC").fetchall()
        items = []
        for r in rows:
            d = dict(r)
            # Get allergens for this recipe
            alg_rows = con.execute(
                """
                SELECT a.name 
                FROM recipe_allergens ra 
                JOIN allergens a ON ra.allergen_id = a.id 
                WHERE ra.recipe_id = ?
            """,
                (d["id"],),
            ).fetchall()
            d["allergens"] = [row[0] for row in alg_rows]
            items.append(d)
        return {"items": items}
    finally:
        con.close()


@app.post("/api/prep")
def prep_create(item: PrepItem) -> Dict[str, Any]:
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    try:
        cur = con.execute(
            """
            INSERT INTO recipes (name, category, unit, par_level, on_hand, prep_time_minutes, station_id, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                item.name,
                item.category,
                item.unit,
                item.par_level,
                item.on_hand,
                item.prep_time_minutes,
                item.station_id,
                item.is_active,
            ),
        )
        new_id = cur.lastrowid

        # Handle allergens
        if item.allergens:
            # Resolve names to IDs (or create if not exist? For now assume strict list or create on fly if generic)
            # Let's map names to IDs.
            for alg_name in item.allergens:
                # Find or ignore? Let's simplistic: find ID for name
                row = con.execute("SELECT id FROM allergens WHERE name = ?", (alg_name,)).fetchone()
                if row:
                    con.execute(
                        "INSERT OR IGNORE INTO recipe_allergens (recipe_id, allergen_id) VALUES (?, ?)",
                        (new_id, row[0]),
                    )

        con.commit()
        return {"ok": True, "id": new_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        con.close()


@app.put("/api/prep/{item_id}")
def prep_update(item_id: int, item: PrepItem) -> Dict[str, Any]:
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    try:
        cur = con.execute(
            """
            UPDATE recipes 
            SET name=?, category=?, unit=?, par_level=?, on_hand=?, prep_time_minutes=?, station_id=?, is_active=?, updated_at=CURRENT_TIMESTAMP
            WHERE id=?
        """,
            (
                item.name,
                item.category,
                item.unit,
                item.par_level,
                item.on_hand,
                item.prep_time_minutes,
                item.station_id,
                item.is_active,
                item_id,
            ),
        )

        # Update allergens: delete all, re-insert
        con.execute("DELETE FROM recipe_allergens WHERE recipe_id=?", (item_id,))
        if item.allergens:
            for alg_name in item.allergens:
                row = con.execute("SELECT id FROM allergens WHERE name = ?", (alg_name,)).fetchone()
                if row:
                    con.execute(
                        "INSERT OR IGNORE INTO recipe_allergens (recipe_id, allergen_id) VALUES (?, ?)",
                        (item_id, row[0]),
                    )

        con.commit()
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Item not found")
        return {"ok": True}
    finally:
        con.close()


@app.delete("/api/prep/{item_id}")
def prep_delete(item_id: int) -> Dict[str, Any]:
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    try:
        cur = con.execute("DELETE FROM recipes WHERE id=?", (item_id,))
        con.commit()
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Item not found")
        return {"ok": True}
    finally:
        con.close()


@app.post("/api/prep/audit")
def prep_audit(payload: PrepAuditPayload) -> Dict[str, Any]:
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    try:
        for item_id, new_on_hand in payload.updates.items():
            con.execute(
                """
                UPDATE recipes 
                SET on_hand = ?, updated_at = CURRENT_TIMESTAMP 
                WHERE id = ?
            """,
                (new_on_hand, item_id),
            )
        con.commit()
        return {"ok": True, "updated_count": len(payload.updates)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        con.close()


@app.get("/api/stations")
def station_list() -> Dict[str, Any]:
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute("SELECT * FROM stations ORDER BY name ASC").fetchall()
        return {"items": [dict(row) for row in rows]}
    finally:
        con.close()


@app.get("/api/allergens")
def allergen_list() -> Dict[str, Any]:
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute("SELECT * FROM allergens ORDER BY name ASC").fetchall()
        return {"items": [dict(row) for row in rows]}
    finally:
        con.close()


# ==============================================================================
# Feature 13: Receiving Log
# ==============================================================================


class ReceivingLog(BaseModel):
    date: str  # ISO YYYY-MM-DD
    supplier: str
    invoice_number: Optional[str] = None
    total_amount: float = 0.0
    has_issue: bool = False
    notes: Optional[str] = None


def init_db_v13():
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    try:
        con.execute("""
        CREATE TABLE IF NOT EXISTS receiving_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vendor_id INTEGER,
            invoice_number TEXT,
            item_name TEXT NOT NULL,
            quantity_received REAL,
            unit TEXT,
            unit_cost REAL,
            total_cost REAL,
            temperature_check REAL,
            quality_ok INTEGER DEFAULT 1,
            notes TEXT,
            received_by TEXT,
            received_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        con.commit()
    finally:
        con.close()


# Run init v13
try:
    init_db_v13()
except Exception as e:
    print(f"DB Init V13 failed: {e}")


@app.post("/api/receiving")
def receiving_create(item: ReceivingLog) -> Dict[str, Any]:
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        vendor_row = con.execute(
            "SELECT id FROM vendors WHERE LOWER(name) = LOWER(?) LIMIT 1",
            (item.supplier.strip(),),
        ).fetchone()
        vendor_id = int(vendor_row["id"]) if vendor_row else None
        cur = con.execute(
            """
            INSERT INTO receiving_log (
                vendor_id, invoice_number, item_name, quantity_received, unit, unit_cost, total_cost,
                temperature_check, quality_ok, notes, received_by, received_at
            )
            VALUES (?, ?, ?, NULL, NULL, NULL, ?, NULL, ?, ?, 'dashboard', ?)
        """,
            (
                vendor_id,
                item.invoice_number,
                item.supplier,
                item.total_amount,
                0 if item.has_issue else 1,
                item.notes,
                f"{item.date} 12:00:00",
            ),
        )
        con.commit()
        return {"ok": True, "id": cur.lastrowid}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        con.close()


@app.get("/api/receiving")
def receiving_list() -> Dict[str, Any]:
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute("""
            SELECT
                rl.id,
                DATE(rl.received_at) AS date,
                COALESCE(v.name, rl.item_name) AS supplier,
                rl.invoice_number,
                COALESCE(rl.total_cost, 0.0) AS total_amount,
                CASE WHEN COALESCE(rl.quality_ok, 1) = 1 THEN 0 ELSE 1 END AS has_issue,
                rl.notes,
                rl.received_at AS created_at
            FROM receiving_log rl
            LEFT JOIN vendors v ON v.id = rl.vendor_id
            ORDER BY rl.received_at DESC, rl.id DESC
            LIMIT 50
            """).fetchall()
        return {"items": [dict(row) for row in rows]}
    finally:
        con.close()


@app.post("/api/stations")
def station_create(item: Station) -> Dict[str, Any]:
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    try:
        cur = con.execute(
            """
            INSERT INTO stations (name, description, instructions, is_active)
            VALUES (?, ?, ?, ?)
        """,
            (item.name, item.description, item.instructions, item.is_active),
        )
        con.commit()
        return {"ok": True, "id": cur.lastrowid}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        con.close()


@app.put("/api/stations/{item_id}")
def station_update(item_id: int, item: Station) -> Dict[str, Any]:
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    try:
        cur = con.execute(
            """
            UPDATE stations 
            SET name=?, description=?, instructions=?, is_active=?, updated_at=CURRENT_TIMESTAMP
            WHERE id=?
        """,
            (item.name, item.description, item.instructions, item.is_active, item_id),
        )
        con.commit()
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Item not found")
        return {"ok": True}
    finally:
        con.close()


@app.delete("/api/stations/{item_id}")
def station_delete(item_id: int) -> Dict[str, Any]:
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    try:
        cur = con.execute("DELETE FROM stations WHERE id=?", (item_id,))
        con.commit()
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Item not found")
        return {"ok": True}
    finally:
        con.close()


@app.post("/api/inventory/counts")
def inventory_counts_save(payload: Dict[str, Any]) -> Dict[str, Any]:
    # payload: { "date": "YYYY-MM-DD", "counts": [{ "item_id": 1, "quantity": 10 }, ...] }
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    try:
        date = payload.get("date", datetime.now().strftime("%Y-%m-%d"))
        counts = payload.get("counts", [])

        # Ensure table exists (idempotent for now, ideally in init_db)
        con.execute("""
            CREATE TABLE IF NOT EXISTS inventory_counts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                item_id INTEGER NOT NULL,
                quantity REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(item_id) REFERENCES vendor_items(id)
            )
        """)

        for item in counts:
            con.execute(
                """
                INSERT INTO inventory_counts (date, item_id, quantity)
                VALUES (?, ?, ?)
            """,
                (date, item["item_id"], item["quantity"]),
            )

        con.commit()
        return {"ok": True, "count": len(counts)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        con.close()


# ==============================================================================
# Feature 6: Recipe Database
# ==============================================================================


class Recipe(BaseModel):
    name: str
    yield_amount: Optional[float] = 1.0
    yield_unit: Optional[str] = "portion"
    ingredients: Optional[str] = "[]"  # JSON string of list of dicts
    instructions: Optional[str] = ""
    is_active: bool = True
    sales_price: Optional[float] = 0.0
    recent_sales_count: Optional[int] = 0
    par_level: Optional[float] = 0.0
    on_hand: Optional[float] = 0.0


@app.get("/api/recipes")
def recipes_list() -> Dict[str, Any]:
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        # Ensure table exists
        con.execute("""
            CREATE TABLE IF NOT EXISTS recipes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                yield_amount REAL,
                yield_unit TEXT,
                ingredients TEXT,
                instructions TEXT,
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP,
                sales_price REAL DEFAULT 0,
                recent_sales_count INTEGER DEFAULT 0,
                par_level REAL DEFAULT 0,
                on_hand REAL DEFAULT 0
            )
        """)

        # Simple migration for existing tables
        try:
            con.execute("ALTER TABLE recipes ADD COLUMN sales_price REAL DEFAULT 0")
        except:
            pass
        try:
            con.execute("ALTER TABLE recipes ADD COLUMN recent_sales_count INTEGER DEFAULT 0")
        except:
            pass
        try:
            con.execute("ALTER TABLE recipes ADD COLUMN par_level REAL DEFAULT 0")
        except:
            pass
        try:
            con.execute("ALTER TABLE recipes ADD COLUMN on_hand REAL DEFAULT 0")
        except:
            pass

        rows = con.execute("SELECT * FROM recipes WHERE is_active=1 ORDER BY name ASC").fetchall()
        recipes = [dict(row) for row in rows]

        # Calculate costs
        # 1. Get all vendor items with price
        item_rows = con.execute(
            "SELECT id, price FROM vendor_items WHERE price IS NOT NULL"
        ).fetchall()
        price_map = {row["id"]: row["price"] for row in item_rows}  # { item_id: price }

        # 2. Compute cost for each recipe
        for recipe in recipes:
            cost = 0.0
            try:
                ingredients = (
                    json.loads(recipe["ingredients"]) if recipe["ingredients"] else []
                )  # safe JSON parse
                # Better to use json.loads if we stored as JSON string. The current implementation stores as JSON string "[]".
                import json

                ingredients = json.loads(recipe["ingredients"]) if recipe["ingredients"] else []

                for ing in ingredients:
                    # ing: { "item": "Onion", "qty": "1", "unit": "pc", "item_id": ? }
                    # Wait, the current frontend saves free-text "item".
                    # To support costing, we need to link ingredients to vendor_items.
                    # The current plan said "Assuming units match for MVP".
                    # BUT the frontend "ingredients" is currently just: [{ item: 'Onion', qty: '1', unit: 'pc' }]
                    # It DOES NOT store vendor_item_id.
                    # I need to update the recipe save to try and match items, OR update the frontend to select items.
                    # For now, I will try to match by NAME if item_id is missing, or just skip if no match.
                    # Let's try to match by name for MVP as per plan "mapped to vendor item if possible".
                    pass
            except:
                pass
            recipe["estimated_cost"] = 0.0

        # actually, I need to implement the linking in the frontend or smart-matching here.
        # Let's fetch all items map by Name as well for fallback.
        item_name_map = {
            row["name"].lower(): row["price"]
            for row in con.execute(
                "SELECT name, price FROM vendor_items WHERE price IS NOT NULL"
            ).fetchall()
        }

        result = []
        import json

        for recipe in recipes:
            total_cost = 0.0
            try:
                ingredients = json.loads(recipe["ingredients"]) if recipe["ingredients"] else []
                for ing in ingredients:
                    # Try to find price
                    price = 0.0
                    qty = float(ing.get("qty", 0) or 0)

                    # If we had item_id, use it. But we only have name currently from the UI.
                    name = ing.get("item", "").strip().lower()
                    if name in item_name_map:
                        price = item_name_map[name]

                    total_cost += price * qty
            except Exception:
                pass  # JSON error or other

            recipe["estimated_cost"] = round(total_cost, 2)
            result.append(recipe)

        return {"recipes": result}
    finally:
        con.close()


@app.post("/api/recipes")
def recipes_create(recipe: Recipe) -> Dict[str, Any]:
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    try:
        cur = con.execute(
            """
            INSERT INTO recipes (name, yield_amount, yield_unit, ingredients, instructions, is_active, sales_price, recent_sales_count, par_level, on_hand)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                recipe.name,
                recipe.yield_amount,
                recipe.yield_unit,
                recipe.ingredients,
                recipe.instructions,
                recipe.is_active,
                recipe.sales_price,
                recipe.recent_sales_count,
                recipe.par_level,
                recipe.on_hand,
            ),
        )
        con.commit()
        return {"ok": True, "id": cur.lastrowid}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        con.close()


@app.put("/api/recipes/{recipe_id}")
def recipes_update(recipe_id: int, recipe: Recipe) -> Dict[str, Any]:
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    try:
        cur = con.execute(
            """
            UPDATE recipes 
            SET name=?, yield_amount=?, yield_unit=?, ingredients=?, instructions=?, is_active=?, sales_price=?, recent_sales_count=?, par_level=?, on_hand=?, updated_at=CURRENT_TIMESTAMP
            WHERE id=?
        """,
            (
                recipe.name,
                recipe.yield_amount,
                recipe.yield_unit,
                recipe.ingredients,
                recipe.instructions,
                recipe.is_active,
                recipe.sales_price,
                recipe.recent_sales_count,
                recipe.par_level,
                recipe.on_hand,
                recipe_id,
            ),
        )
        con.commit()
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Recipe not found")
        return {"ok": True}
    finally:
        con.close()


@app.get("/api/menu-engineering")
def menu_engineering() -> Dict[str, Any]:
    # Reuse recipes_list logic to get costs, then add analysis
    recipes_payload = recipes_list()
    recipes = recipes_payload.get("recipes", [])

    if not recipes:
        return {"items": []}

    # Calculate avg profit and avg popularit (popularity is just count for now)
    total_contribution = 0.0
    total_sold = 0

    analyzed = []

    for r in recipes:
        price = float(r.get("sales_price", 0) or 0)
        cost = float(r.get("estimated_cost", 0) or 0)
        count = int(r.get("recent_sales_count", 0) or 0)

        contribution = price - cost
        total_contribution += contribution * count
        total_sold += count

        analyzed.append(
            {
                "id": r["id"],
                "name": r["name"],
                "cost": cost,
                "price": price,
                "margin": contribution,
                "count": count,
                "margin_pc": (contribution / price) if price > 0 else 0,
            }
        )

    avg_contribution = (total_contribution / total_sold) if total_sold > 0 else 0
    # Popularity benchmark: (100% / number of items) * 0.7 (70% rule) involves more complex math.
    # Simple AVG for now:
    avg_count = total_sold / len(recipes)

    # Classify
    final_items = []
    for item in analyzed:
        # High/Low Contribution
        high_profit = item["margin"] >= avg_contribution
        # High/Low Popularity
        high_pop = item["count"] >= avg_count

        classification = "Dog"
        if high_profit and high_pop:
            classification = "Star"
        elif high_profit and not high_pop:
            classification = "Puzzle"
        elif not high_profit and high_pop:
            classification = "Plowhorse"

        item["classification"] = classification
        final_items.append(item)

    return {"items": final_items, "averages": {"margin": avg_contribution, "count": avg_count}}


@app.post("/api/prep-update")
def prep_update(updates: Dict[str, float]) -> Dict[str, Any]:
    # updates: { "recipe_id": on_hand_qty }
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    try:
        for rid_str, qty in updates.items():
            if not rid_str.isdigit():
                continue
            rid = int(rid_str)
            con.execute(
                "UPDATE recipes SET on_hand=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", (qty, rid)
            )
        con.commit()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        con.close()


@app.delete("/api/recipes/{recipe_id}")
def recipes_delete(recipe_id: int) -> Dict[str, Any]:
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    try:
        # Soft delete
        cur = con.execute("UPDATE recipes SET is_active=0 WHERE id=?", (recipe_id,))
        con.commit()
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Recipe not found")
        return {"ok": True}
    finally:
        con.close()


# ==============================================================================
# Feature 2: Email Composer
# ==============================================================================


class DraftEmailPayload(BaseModel):
    vendor_id: int
    context: str


@app.post("/api/composer/draft")
def composer_draft(payload: DraftEmailPayload) -> Dict[str, Any]:
    # 1. Get Vendor
    db_path = get_memory_db_path()
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        row = con.execute("SELECT * FROM vendors WHERE id=?", (payload.vendor_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Vendor not found")
        vendor = dict(row)
    finally:
        con.close()

    # 2. Construct Prompt
    vendor_name = vendor["name"]
    contact_name = vendor["contact_name"] or "Sales Rep"

    prompt = (
        f"Write a professional, concise procurement email to {vendor_name} (Contact: {contact_name}).\n"
        f"Context/Requirements: {payload.context}\n\n"
        "Output format:\n"
        "Subject: [Subject Line]\n"
        "\n"
        "[Email Body]\n"
        "\n"
        "IMPORTANT: Output ONLY the raw email text. Do not include HTML, markdown, or chef-style card formatting. "
        "Keep it business-like and direct."
    )

    # 3. Call Brain
    # Note: chat() enforces chef persona, so we try to override via strong instructions
    response = chat([("user", prompt)])

    # 4. Parse (Simple heuristic)
    subject = "Order Inquiry"
    body = response

    # Try to extract subject if present
    import re

    subject_match = re.search(r"Subject:\s*(.+)", response, re.IGNORECASE)
    if subject_match:
        subject = subject_match.group(1).strip()
        body = response.replace(subject_match.group(0), "").strip()

    return {"vendor_email": vendor["email"] or "", "subject": subject, "body": body}
