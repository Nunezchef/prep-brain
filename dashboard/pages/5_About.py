import streamlit as st
import time
from dashboard.utils import (
    load_config, save_config, get_bot_status, start_bot, stop_bot, get_logs,
    get_ollama_status, start_ollama
)

# --- CONFIGURATION ---
st.set_page_config(
    page_title="Prep-Brain Control",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="collapsed" 
)

# --- CUSTOM CSS ---
st.markdown("""
<style>
    /* Global Cleanliness */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    
    /* Card Style for Metrics */
    div[data-testid="metric-container"] {
        background-color: #1E1E1E;
        border: 1px solid #333;
        padding: 15px;
        border-radius: 10px;
        color: #ddd;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    /* Button Styling */
    div.stButton > button:first-child {
        border-radius: 8px;
        font-weight: 600;
        width: 100%;
        border: none;
    }
    
    /* Logs styling */
    .stTextArea textarea {
        background-color: #111;
        color: #0f0;
        font-family: 'Courier New', monospace;
        font-size: 12px;
    }
    
    /* Dividers */
    hr {
        margin-top: 1rem;
        margin-bottom: 1rem;
        border-color: #444;
    }
</style>
""", unsafe_allow_html=True)

# --- HEADER & STATUS ---
c1, c2 = st.columns([3, 1])
with c1:
    st.title("🧠 Prep-Brain Control")
with c2:
    if st.button("🔄 Refresh System", type="secondary"):
        st.rerun()

# Fetch Statuses
bot_status, bot_icon, pid = get_bot_status()
ollama_status, ollama_icon = get_ollama_status()

# --- STATUS CARDS ---
st.markdown("### SYSTEM STATUS")
m1, m2, m3 = st.columns(3)
with m1:
    st.metric("Telegram Bot", f"{bot_status} {bot_icon}", f"PID: {pid}" if pid else "Stopped")
with m2:
    st.metric("Ollama Service", f"{ollama_status} {ollama_icon}", "Localhost:11434")
with m3:
    config = load_config()
    st.metric("Active Model", config["ollama"]["model"])

st.markdown("---")

# --- MAIN CONTROLS ---
col_left, col_right = st.columns([1, 2], gap="large")

with col_left:
    st.subheader("🚀 Actions")
    
    # Bot Actions
    with st.container(border=True):
        st.markdown("**Telegram Bot**")
        b1, b2 = st.columns(2)
        with b1:
            if st.button("▶ START BOT", type="primary", disabled=(bot_status == "Running"), use_container_width=True):
                start_bot()
                st.rerun()
        with b2:
            if st.button("⏹ STOP BOT", type="secondary", disabled=(bot_status != "Running"), use_container_width=True):
                stop_bot()
                st.rerun()
        
        if st.button("♻️ RESTART BOT", use_container_width=True):
            stop_bot()
            time.sleep(1)
            start_bot()
            st.rerun()

    st.write("") # Spacer

    # Ollama Actions
    with st.container(border=True):
        st.markdown("**Ollama Backend**")
        if st.button("Start Ollama Serve", disabled=(ollama_status == "Running"), use_container_width=True):
            start_ollama()
            st.rerun()

    st.write("") # Spacer

    # Quick Settings in an expander to save space
    with st.expander("⚡ Quick Config", expanded=True):
        with st.form("quick_config_form"):
            new_model = st.text_input("Model", config["ollama"]["model"])
            new_temp = st.slider("Temp", 0.0, 1.0, config["ollama"]["temperature"])
            if st.form_submit_button("Update Config"):
                config["ollama"]["model"] = new_model
                config["ollama"]["temperature"] = new_temp
                save_config(config)
                st.success("Saved!")

with col_right:
    st.subheader("📜 Live Logs")
    
    # Log Controls
    lc1, lc2 = st.columns([3, 1])
    with lc1:
        log_filter = st.radio("Show:", ["All", "Active Errors", "Warnings"], horizontal=True, label_visibility="collapsed")
    with lc2:
        pass # Spacer

    # Fetch logs
    raw_logs = get_logs(300)
    filtered_logs = []
    
    for line in raw_logs.splitlines():
        if log_filter == "All":
            filtered_logs.append(line)
        elif log_filter == "Active Errors" and "ERROR" in line.upper():
            filtered_logs.append(line)
        elif log_filter == "Warnings" and "WARNING" in line.upper():
            filtered_logs.append(line)
            
    log_content = "\n".join(filtered_logs)
    
    st.text_area("Console Output", log_content, height=500, label_visibility="collapsed")
    
    st.download_button("💾 Download Log File", raw_logs, "prep-brain.log", mime="text/plain", use_container_width=True)
