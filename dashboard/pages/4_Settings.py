import streamlit as st
import yaml
from pathlib import Path

# --- PAGE CONFIG ---
st.set_page_config(page_title="Settings", page_icon="⚙️", layout="wide")
st.title("⚙️ Settings & Configuration")

CONFIG_PATH = Path("config.yaml")

# --- UTILS ---
def load_config():
    if not CONFIG_PATH.exists():
        return {}
    try:
        with open(CONFIG_PATH, "r") as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        st.error(f"Error loading config: {e}")
        return {}

def save_config_file(data):
    try:
        with open(CONFIG_PATH, "w") as f:
            yaml.dump(data, f, sort_keys=False, allow_unicode=True)
        return True, "Configuration saved successfully!"
    except Exception as e:
        return False, f"Error saving config: {e}"

def save_raw_text(text):
    try:
        # Validate YAML first
        yaml.safe_load(text)
        CONFIG_PATH.write_text(text)
        return True, "Raw config saved!"
    except yaml.YAMLError as e:
        return False, f"Invalid YAML: {e}"

# --- LAYOUT ---
tab_prompt, tab_raw = st.tabs(["🧠 System Prompt", "🛠️ Advanced Config"])

# --- TAB 1: SYSTEM PROMPT ---
with tab_prompt:
    st.markdown("""
    ### System Personality & Behavior
    The **System Prompt** defines how the AI behaves, its tone, and its constraints. 
    Changes here take effect immediately for the next message.
    """)
    
    config = load_config()
    current_prompt = config.get("system_prompt", "")
    
    with st.form("prompt_form"):
        new_prompt = st.text_area("Core Instructions", value=current_prompt, height=400)
        
        c1, c2, c3 = st.columns([1, 1, 4])
        with c1:
            submitted = st.form_submit_button("💾 Save & Apply", type="primary")
        with c2:
            reset = st.form_submit_button("🔄 Reset to Default", type="secondary")
        with c3:
            pass # Spacer

        if submitted:
            config["system_prompt"] = new_prompt
            success, msg = save_config_file(config)
            if success:
                st.success(msg)
            else:
                st.error(msg)
        
        if reset:
            from dashboard.utils import DEFAULT_SYSTEM_PROMPT
            config["system_prompt"] = DEFAULT_SYSTEM_PROMPT
            success, msg = save_config_file(config)
            if success:
                st.success("Reset to default prompt! Refreshing...")
                st.rerun()
            else:
                st.error(msg)
                
    st.info("💡 **Tip:** Keep instructions clear and actionable. Use bullet points for complex rules.")

# --- TAB 2: RAW CONFIG ---
with tab_raw:
    st.markdown("### Raw `config.yaml` Editor")
    st.warning("⚠️ Editing raw configuration can break the bot. Be careful with indentation.")
    
    raw_content = CONFIG_PATH.read_text() if CONFIG_PATH.exists() else ""
    
    new_raw = st.text_area("YAML Editor", value=raw_content, height=600)
    
    if st.button("Save Raw Config", type="secondary"):
        success, msg = save_raw_text(new_raw)
        if success:
            st.success(msg)
            st.rerun()
        else:
            st.error(msg)
