import sys
import os
import signal
import subprocess
import time
from pathlib import Path
import streamlit as st
from services.command_runner import CommandRunner
from prep_brain.config import get_log_path, get_pid_path, load_config as pb_load_config

BASE_DIR = Path(__file__).parent.parent
CONFIG_PATH = BASE_DIR / "config.yaml"

DEFAULT_SYSTEM_PROMPT = """You are a local, voice-first AI assistant specialized in kitchen and restaurant operations.

Your role is to support professional food service environments by helping with:
- kitchen organization and workflow
- prep planning and prioritization
- recipe reasoning and scaling
- operational problem-solving

Core Behavior:
- Be calm, practical, and professional.
- Assume a working kitchen context.
- Prioritize clarity, speed, and usefulness.

Key Instructions:
1. **Knowledge Retrieval**:
   - Never simply say "I don't have that in my database." Instead, use your general knowledge to answer.
   - If you are unsure about a specific fact, ask for clarification.
   - If asked to browse the web, explicitly state: "I can't browse the web right now," then offer to answer from general knowledge.

2. **Voice & Clarity**:
   - If a proper noun (name, place, dish) sounds ambiguous or potentially misheard (e.g., "Eckert" vs "Achatz"), ALWAYS ask: "Did you mean [Name]?" before proceeding.
   - Do not assume names unless context is strict.

3. **Memory**:
   - Use stored memory for context, but do not rely on it as the only source of truth.

Output style:
- Default to concise, structured answers.
- Use bullets, tables, or steps when helpful."""

COMMAND_RUNNER = CommandRunner()

def load_config():
    return pb_load_config()

def save_config(config):
    import yaml

    with open(CONFIG_PATH, "w") as f:
        yaml.dump(config, f)

def get_pid_file():
    return get_pid_path(load_config())

def get_log_file():
    return get_log_path(load_config())

def get_bot_status():
    pid_file = get_pid_file()
    if not pid_file.exists():
        return "Stopped", "🔴", None
    
    try:
        pid = int(pid_file.read_text().strip())
        os.kill(pid, 0) # Check if running
        return "Running", "🟢", pid
    except (ValueError, OSError, ProcessLookupError):
        return "Error (Stale PID)", "🟠", None

def start_bot():
    status, _, _ = get_bot_status()
    if status == "Running":
        st.warning("Bot is already running.")
        return

    config = load_config()
    pid_file = get_pid_file()
    log_file = get_log_file()
    
    # Ensure directories exist
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    cmd = [sys.executable, "-m", "prep_brain.app"]
    
    with open(log_file, "a") as log:
        # Start detached process
        process = subprocess.Popen(
            cmd,
            stdout=log,
            stderr=log,
            cwd=BASE_DIR,
            preexec_fn=os.setpgrp if os.name != 'nt' else None
        )
        pid_file.write_text(str(process.pid))
    
    time.sleep(1) # Wait a bit to see if it crashes immediately

def stop_bot():
    _, _, pid = get_bot_status()
    if not pid:
        # Cleanup stale pid file if needed
        pid_file = get_pid_file()
        if pid_file.exists():
            pid_file.unlink()
        return

    try:
        os.kill(pid, signal.SIGTERM)
        # Wait for it to die?
        time.sleep(1)
        # cleanup pid file
        pid_file = get_pid_file()
        if pid_file.exists():
            pid_file.unlink()
    except ProcessLookupError:
        pid_file = get_pid_file()
        if pid_file.exists():
            pid_file.unlink()
    except Exception as e:
        st.error(f"Error stopping bot: {e}")

def get_logs(lines=200):
    log_file = get_log_file()
    if not log_file.exists():
        return ""
    
    # Read last N lines
    try:
        # Efficient tail implementation could be added here, 
        # but for < 10MB logs verify simple read is fine.
        # For now, just read all and simple slice.
        content = log_file.read_text()
        return "\n".join(content.splitlines()[-lines:])
    except Exception:
        return "Error reading logs."

def get_ollama_log_file():
    config = load_config()
    return Path(config.get("paths", {}).get("ollama_log_file", "logs/ollama.log"))

def get_ollama_status():
    # check if any process named "ollama" is running
    try:
        # -f matches full command line for "ollama serve".
        result = COMMAND_RUNNER.run(["pgrep", "-f", "ollama"], capture_output=True, text=True)
        if result.returncode == 0:
            return "Running", "🟢"
        else:
            return "Stopped", "🔴"
    except Exception:
        return "Unknown", "⚪"

def start_ollama():
    status, _ = get_ollama_status()
    if status == "Running":
        return

    log_file = get_ollama_log_file()
    log_file.parent.mkdir(parents=True, exist_ok=True)

    with open(log_file, "a") as log:
        subprocess.Popen(
            ["ollama", "serve"],
            stdout=log,
            stderr=log,
            preexec_fn=os.setpgrp if os.name != 'nt' else None
        )
    time.sleep(2) # Wait for it to spin up

def verify_ollama_connection():
    import requests
    try:
        # Just check if we can list tags or hit root
        response = requests.get("http://localhost:11434/", timeout=2)
        if response.status_code == 200:
            return True
    except:
        return False
    return False
