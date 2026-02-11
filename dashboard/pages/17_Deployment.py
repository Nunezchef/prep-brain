import streamlit as st
import subprocess
import yaml
from pathlib import Path

st.set_page_config(page_title="Deployment", page_icon="🚀", layout="wide")

st.title("🚀 Deployment & Run Profiles")

tab_status, tab_config = st.tabs(["📊 System Status", "⚙️ Configuration"])

# --- STATUS TAB ---
with tab_status:
    st.subheader("System Health Check")
    
    # Database
    db_path = Path("data/kitchen.db")
    if db_path.exists():
        size_mb = db_path.stat().st_size / (1024 * 1024)
        st.success(f"✅ Database: {size_mb:.2f} MB")
    else:
        st.error("❌ Database not found")
    
    # Config
    config_path = Path("config.yaml")
    if config_path.exists():
        st.success("✅ config.yaml loaded")
    else:
        st.error("❌ config.yaml missing")
    
    # ChromaDB
    chroma_path = Path("data/chroma_db")
    if chroma_path.exists():
        st.success("✅ ChromaDB directory exists")
    else:
        st.warning("⚠️ ChromaDB not initialized")
    
    # Ollama Check
    try:
        result = subprocess.run(["ollama", "list"], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            models = result.stdout.strip().split("\n")[1:]  # skip header
            st.success(f"✅ Ollama: {len(models)} model(s) available")
            with st.expander("Available Models"):
                st.code(result.stdout)
        else:
            st.warning("⚠️ Ollama available but returned error")
    except FileNotFoundError:
        st.warning("⚠️ Ollama not installed")
    except subprocess.TimeoutExpired:
        st.warning("⚠️ Ollama not responding")
    
    st.divider()
    
    # Dashboard Pages
    pages_dir = Path("dashboard/pages")
    if pages_dir.exists():
        pages = sorted(pages_dir.glob("*.py"))
        st.metric("Dashboard Pages", len(pages))
        with st.expander("All Pages"):
            for p in pages:
                st.text(f"📄 {p.name}")
    
    # Services
    services_dir = Path("services")
    if services_dir.exists():
        services = sorted(services_dir.glob("*.py"))
        st.metric("Service Modules", len([s for s in services if s.name != "__init__.py"]))

# --- CONFIG TAB ---
with tab_config:
    st.subheader("Runtime Configuration")
    
    config_path = Path("config.yaml")
    if config_path.exists():
        with open(config_path) as f:
            config = yaml.safe_load(f)
        
        st.json(config)
        
        st.divider()
        st.caption("Edit config.yaml directly to change settings. Restart the dashboard after changes.")
    else:
        st.error("config.yaml not found.")
    
    st.divider()
    st.subheader("Quick Commands")
    
    st.code("# Start Dashboard\n./run_dashboard.sh\n\n# Run All Tests\n.venv/bin/python -m pytest tests/ -v\n\n# Initialize Database\n.venv/bin/python -c 'from services.memory import init_db; init_db()'", language="bash")
