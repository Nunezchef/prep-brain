import streamlit as st
import requests
import pandas as pd
from datetime import datetime
import time

# --- CONFIG ---
API_BASE = "http://localhost:8000/api"

st.set_page_config(page_title="Station Packs", page_icon="🎒", layout="wide")

st.title("🎒 Station Packs")
st.markdown("Training guides, setup instructions, and station-specific prep lists.")

def get_stations():
    try:
        resp = requests.get(f"{API_BASE}/stations")
        if resp.status_code == 200:
            return resp.json().get("items", [])
        return []
    except:
        return []

def create_station(payload):
    try:
        resp = requests.post(f"{API_BASE}/stations", json=payload)
        return resp.status_code == 200
    except:
        return False

def update_station(item_id, payload):
    try:
        resp = requests.put(f"{API_BASE}/stations/{item_id}", json=payload)
        return resp.status_code == 200
    except:
        return False

def delete_station(item_id):
    try:
        resp = requests.delete(f"{API_BASE}/stations/{item_id}")
        return resp.status_code == 200
    except:
        return False

def get_prep_list():
    try:
        resp = requests.get(f"{API_BASE}/prep")
        if resp.status_code == 200:
            return resp.json().get("items", [])
        return []
    except:
        return []

# --- TABS ---
tab_view, tab_manage = st.tabs(["👁️ Station Viewer", "⚙️ Manage Stations"])

# 1. VIEWER TAB
with tab_view:
    stations = get_stations()
    if not stations:
        st.info("No stations found. Go to 'Manage Stations' to create one.")
    else:
        # Sidebar selection
        c1, c2 = st.columns([1, 3])
        
        station_names = [s["name"] for s in stations if s.get("is_active")]
        selected_name = c1.selectbox("Select Station", options=station_names)
        
        selected_station = next((s for s in stations if s["name"] == selected_name), None)
        
        if selected_station:
            with c2:
                st.header(selected_station["name"])
                if selected_station.get("description"):
                    st.caption(selected_station["description"])
                
                st.markdown("### 📋 Setup Guide")
                st.markdown(selected_station.get("instructions", "No instructions provided."))
                
                st.markdown("---")
                st.markdown("### 🔪 Station Prep List")
                
                all_prep = get_prep_list()
                station_prep = [p for p in all_prep if p.get("station_id") == selected_station["id"] and p.get("is_active")]
                
                if not station_prep:
                    st.info("No prep items assigned to this station.")
                else:
                    df = pd.DataFrame(station_prep)
                    # Calculate need
                    df["Need"] = df["par_level"] - df["on_hand"]
                    df = df[df["Need"] > 0] # Filter only what's needed? Or show all? Let's show all but highlight need.
                    
                    if df.empty:
                        st.success("Station is fully stocked! (Based on Par - On Hand)")
                    else:
                        st.dataframe(
                            df[["name", "unit", "par_level", "on_hand", "Need"]],
                            use_container_width=True,
                            hide_index=True
                        )

# 2. MANAGE TAB
with tab_manage:
    st.header("Manage Stations")
    
    with st.expander("Create New Station"):
        with st.form("create_station"):
            new_name = st.text_input("Station Name (e.g., Grill, Sauté)")
            new_desc = st.text_input("Short Description")
            new_instr = st.text_area("Setup Instructions (Markdown supported)", height=200)
            active = st.checkbox("Active", value=True)
            
            submitted = st.form_submit_button("Create Station")
            if submitted:
                if not new_name:
                    st.error("Name required")
                else:
                    payload = {
                        "name": new_name,
                        "description": new_desc,
                        "instructions": new_instr,
                        "is_active": active
                    }
                    if create_station(payload):
                        st.success(f"Created {new_name}")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("Failed to create")

    st.markdown("### Existing Stations")
    if stations:
        for s in stations:
            with st.expander(f"{s['name']} (Active: {s.get('is_active')})"):
                with st.form(f"edit_{s['id']}"):
                    e_name = st.text_input("Name", value=s["name"])
                    e_desc = st.text_input("Description", value=s.get("description", ""))
                    e_instr = st.text_area("Instructions", value=s.get("instructions", ""), height=150)
                    e_active = st.checkbox("Active", value=s.get("is_active"))
                    
                    c_save, c_del = st.columns([4, 1])
                    if c_save.form_submit_button("Save Changes"):
                        payload = {
                            "name": e_name,
                            "description": e_desc,
                            "instructions": e_instr,
                            "is_active": e_active
                        }
                        if update_station(s["id"], payload):
                            st.success("Updated")
                            time.sleep(0.5)
                            st.rerun()
                    
                    # Delete button relies on session state or callback tricks in forms, easier to move out?
                    # Let's keep it simple.
                
                if st.button("Delete Station", key=f"del_{s['id']}"):
                     if delete_station(s['id']):
                         st.success("Deleted")
                         time.sleep(0.5)
                         st.rerun()
