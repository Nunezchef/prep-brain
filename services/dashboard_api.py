import os
import platform
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import psutil
import yaml
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from services.brain import chat
from services.transcriber import transcribe_file

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config.yaml"
APP_STARTED_AT = int(time.time())

app = FastAPI(title="Prep-Brain Dashboard API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:4173",
        "http://127.0.0.1:4173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ToggleSourcePayload(BaseModel):
    active: bool


class BrainTestPayload(BaseModel):
    prompt: str


def load_config() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        return {}
    try:
        return yaml.safe_load(CONFIG_PATH.read_text()) or {}
    except Exception:
        return {}


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
    config = load_config()
    db_path = config.get("memory", {}).get("db_path", "data/memory.db")
    path = Path(db_path)
    if not path.is_absolute():
        path = BASE_DIR / path
    return path


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
    current = get_bot_status()
    if current["running"]:
        return {"changed": False, "message": "Bot already running.", "status": current}

    pid_file = get_pid_file()
    log_file = get_log_file()
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    cmd = [sys.executable, "-m", "services.bot"]

    with open(log_file, "a") as log:
        process = subprocess.Popen(
            cmd,
            stdout=log,
            stderr=log,
            cwd=BASE_DIR,
            preexec_fn=os.setpgrp if os.name != "nt" else None,
        )
        pid_file.write_text(str(process.pid))

    time.sleep(0.8)
    return {
        "changed": True,
        "message": "Bot start command sent.",
        "status": get_bot_status(),
    }


def stop_bot() -> Dict[str, Any]:
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
    stop_bot()
    time.sleep(0.4)
    return start_bot()


def _list_ollama_pids() -> List[int]:
    pids: List[int] = []
    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            name = (proc.info.get("name") or "").lower()
            cmdline_parts = proc.info.get("cmdline") or []
            cmdline = " ".join(cmdline_parts).lower()

            is_ollama_binary = name == "ollama" or (cmdline_parts and Path(cmdline_parts[0]).name.lower() == "ollama")
            is_ollama_serve = "ollama serve" in cmdline or (is_ollama_binary and "serve" in cmdline_parts)

            if is_ollama_binary or is_ollama_serve:
                pids.append(int(proc.info["pid"]))
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess, KeyError):
            continue
    return sorted(set(pids))


def get_ollama_status() -> Dict[str, Any]:
    try:
        pids = _list_ollama_pids()
        if pids:
            return {"status": "Running", "running": True, "pids": pids}
        return {"status": "Stopped", "running": False, "pids": []}
    except Exception:
        return {"status": "Unknown", "running": False, "pids": []}


def start_ollama() -> Dict[str, Any]:
    status = get_ollama_status()
    if status["running"]:
        return {"changed": False, "message": "Ollama already running.", "status": status}

    log_file = get_ollama_log_file()
    log_file.parent.mkdir(parents=True, exist_ok=True)

    with open(log_file, "a") as log:
        subprocess.Popen(
            ["ollama", "serve"],
            stdout=log,
            stderr=log,
            cwd=BASE_DIR,
            preexec_fn=os.setpgrp if os.name != "nt" else None,
        )

    time.sleep(1.2)
    return {
        "changed": True,
        "message": "Ollama start command sent.",
        "status": get_ollama_status(),
    }


def stop_ollama() -> Dict[str, Any]:
    status = get_ollama_status()
    pids = status.get("pids", [])

    if not pids:
        return {"changed": False, "message": "Ollama was not running.", "status": status}

    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            continue

    time.sleep(0.8)

    remaining = get_ollama_status().get("pids", [])
    for pid in remaining:
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            continue

    return {
        "changed": True,
        "message": "Ollama stop command sent.",
        "status": get_ollama_status(),
    }


def get_logs(lines: int = 120, level_filter: str = "all") -> List[Dict[str, str]]:
    log_file = get_log_file()
    if not log_file.exists():
        return []

    content = log_file.read_text(errors="replace")
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

    signal_value = 98 if (bot_running and ollama_running) else 74 if (bot_running or ollama_running) else 39

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
    sources = get_rag_engine().get_sources()
    sources.sort(key=lambda source: source.get("date_ingested", ""), reverse=True)
    return {"items": sources}


@app.post("/api/knowledge/{source_id}/toggle")
def knowledge_toggle(source_id: str, payload: ToggleSourcePayload) -> Dict[str, Any]:
    ok = get_rag_engine().toggle_source(source_id, payload.active)
    if not ok:
        raise HTTPException(status_code=404, detail="Source not found")
    return {"ok": True}


@app.delete("/api/knowledge/{source_id}")
def knowledge_delete(source_id: str) -> Dict[str, Any]:
    ok = get_rag_engine().delete_source(source_id)
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
        raise HTTPException(status_code=400, detail="Only .pdf, .txt, and .docx files are supported")

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

    save_config(config_data)
    return {"ok": True, "config": load_config()}


@app.get("/api/system/info")
def system_info() -> Dict[str, Any]:
    return {
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "api_started_at": APP_STARTED_AT,
        "cwd": str(BASE_DIR),
    }
