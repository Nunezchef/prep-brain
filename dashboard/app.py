import streamlit as st
import time
import psutil
from datetime import datetime
from dashboard.utils import (
    load_config, save_config, get_bot_status, start_bot, stop_bot, get_logs,
    get_ollama_status, start_ollama
)

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="Prep-Brain Dashboard",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- DATA FETCHING ---
bot_status, bot_icon, pid = get_bot_status()
ollama_status, ollama_icon = get_ollama_status()
config = load_config()

# --- HEADER ---
st.title("🤖 Prep-Brain Dashboard")
st.markdown("---")

# --- STATUS METRICS ---
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Telegram Bot", f"{bot_status} {bot_icon}", f"PID: {pid}" if pid else "Stopped")

with col2:
    st.metric("Ollama Service", f"{ollama_status} {ollama_icon}", "Localhost:11434")

with col3:
    st.metric("Active Model", config["ollama"]["model"])

# --- CONTROLS ---
st.header("Control Panel")
c1, c2, c3 = st.columns(3)

with c1:
    if bot_status == "Running":
         if st.button("⏹ Stop Bot", type="primary", use_container_width=True):
            stop_bot()
            st.rerun()
    else:
        if st.button("▶ Start Bot", type="primary", use_container_width=True):
            start_bot()
            st.rerun()

with c2:
     if st.button("♻️ Restart Bot", use_container_width=True):
        stop_bot()
        time.sleep(1)
        start_bot()
        st.rerun()

with c3:
    if st.button("🚀 Start Ollama", disabled=(ollama_status == "Running"), use_container_width=True):
        start_ollama()
        st.rerun()

# --- LOGS ---
st.header("System Logs")
logs = get_logs(50)
st.text_area("Console Output", logs, height=400)

# Auto-refresh
if bot_status == "Running":
    time.sleep(2)
    st.rerun()
