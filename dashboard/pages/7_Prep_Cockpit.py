import streamlit as st
import requests
import pandas as pd
from datetime import datetime
import time

# --- CONFIG ---
API_BASE = "http://localhost:8000/api"

st.set_page_config(page_title="Prep Cockpit", page_icon="🍳", layout="wide")

st.title("🍳 Prep Cockpit")
st.markdown("Plan production, audit onsite par levels, and manage your prep list.")

# --- HELPERS ---

def get_prep_list():
    try:
        resp = requests.get(f"{API_BASE}/prep")
        if resp.status_code == 200:
            return resp.json().get("items", [])
    except Exception:
        pass
    return []

def create_prep_item(payload):
    try:
        resp = requests.post(f"{API_BASE}/prep", json=payload)
        return resp.status_code == 200
    except Exception:
        return False

def update_prep_item(item_id, payload):
    try:
        resp = requests.put(f"{API_BASE}/prep/{item_id}", json=payload)
        return resp.status_code == 200
    except Exception:
        return False

def delete_prep_item(item_id):
    try:
        resp = requests.delete(f"{API_BASE}/prep/{item_id}")
        return resp.status_code == 200
    except Exception:
        return False

def batch_audit(updates):
    try:
        resp = requests.post(f"{API_BASE}/prep/audit", json={"updates": updates})
        return resp.status_code == 200
    except Exception:
        return False


# --- TABS ---
tab_plan, tab_schedule, tab_audit, tab_manage = st.tabs(["🚀 Plan", "📅 Schedule", "📋 Audit", "⚙️ Manage"])

# 1. PLAN TAB
with tab_plan:
    st.header("Production Need")
    st.caption("Items calculated based on Par - On Hand.")

    items = get_prep_list()
    if not items:
        st.info("No prep items found. Go to 'Manage Items' to add some.")
    else:
        # Calculate needs
        plan_data = []
        for i in items:
            if not i.get("is_active", True):
                continue
            
            par = float(i.get("par_level", 0))
            on_hand = float(i.get("on_hand", 0))
            need = par - on_hand
            
            if need > 0:
                plan_data.append({
                    "Category": i.get("category", "General"),
                    "Item": i.get("name"),
                    "Par": f"{par:g}",
                    "On Hand": f"{on_hand:g}",
                    "Need": f"{need:g}",
                    "Unit": i.get("unit", "unit"),
                    "Prep Time": f"{i.get('prep_time_minutes', 0)}m"
                })
        
        if not plan_data:
            st.success("All par levels met! No prep needed right now.")
        else:
            df_plan = pd.DataFrame(plan_data)
            st.dataframe(
                df_plan, 
                use_container_width=True,
                column_config={
                    "Need": st.column_config.NumberColumn(
                        "TO PREP",
                        help="Amount needed to reach par",
                        format="%.1f"
                    )
                },
                hide_index=True
            )
            
            st.markdown("---")
            if st.button("Refresh Data", key="refresh_plan"):
                st.rerun()

# 2. SCHEDULE TAB
with tab_schedule:
    st.header("Production Schedule")
    
    if not items:
        st.info("No items available.")
    else:
        # User input for service start
        c1, c2 = st.columns([1, 3])
        service_time = c1.time_input("Service Start", value=datetime.strptime("17:00", "%H:%M").time())
        
        # Identify what needs to be prepped
        tasks = []
        now = datetime.now()
        service_dt = datetime.combine(now.date(), service_time)
        
        if service_dt < now:
            # Assume next day if time passed? Or just show past. Let's keep it simple.
            pass

        for i in items:
            if not i.get("is_active", True): continue
            
            need = float(i.get("par_level", 0)) - float(i.get("on_hand", 0))
            if need > 0:
                duration = int(i.get("prep_time_minutes", 0))
                if duration == 0: duration = 30 # Default if not set
                
                # Backward schedule
                start_dt = service_dt - pd.Timedelta(minutes=duration)
                
                tasks.append({
                    "Task": f"{i['name']} ({need:g} {i.get('unit')})",
                    "Start": start_dt,
                    "Finish": service_dt,
                    "Duration": duration,
                    "Category": i.get("category")
                })
        
        if not tasks:
            st.success("Nothing to schedule!")
        else:
            df_tasks = pd.DataFrame(tasks)
            df_tasks_sorted = df_tasks.sort_values("Start")
            
            # Simple Table View
            st.markdown("### Timeline")
            
            st.dataframe(
                df_tasks_sorted[["Start", "Task", "Duration", "Category"]].style.format({
                    "Start": lambda x: x.strftime("%H:%M"),
                    "Duration": "{} mins"
                }),
                use_container_width=True,
                hide_index=True
            )
            
            # Gantt-like viz using Altair
            import altair as alt
            
            chart = alt.Chart(df_tasks).mark_bar().encode(
                x='Start',
                x2='Finish',
                y=alt.Y('Task', sort=alt.EncodingSortField(field="Start", order="ascending")),
                color='Category',
                tooltip=['Task', 'Start', 'Finish', 'Duration']
            ).properties(height=len(df_tasks)*40 + 50)
            
            st.altair_chart(chart, use_container_width=True)


# 3. AUDIT TAB
with tab_audit:
    st.header("Daily Audit")
    st.caption("Quickly update 'On Hand' counts to generate the plan.")

    if not items:
        st.info("No items to audit.")
    else:
        with st.form("audit_form"):
            # Group by category
            df = pd.DataFrame(items)
            categories = sorted(df["category"].fillna("General").unique())
            
            updates = {}
            
            for cat in categories:
                st.subheader(cat)
                cat_items = [x for x in items if (x.get("category") or "General") == cat and x.get("is_active")]
                
                cols = st.columns(3)
                for idx, item in enumerate(cat_items):
                    col = cols[idx % 3]
                    with col:
                        new_val = st.number_input(
                            f"{item['name']} ({item.get('unit')})",
                            min_value=0.0,
                            value=float(item.get("on_hand", 0)),
                            key=f"audit_{item['id']}",
                            step=1.0
                        )
                        updates[item['id']] = new_val
            
            st.markdown("---")
            submitted = st.form_submit_button("Save Audit Counts", type="primary")
            if submitted:
                if batch_audit(updates):
                    st.success("Audit saved! Check the Plan tab.")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Failed to save audit.")

# Helper
def get_stations():
    try:
        resp = requests.get(f"{API_BASE}/stations")
        if resp.status_code == 200:
            return resp.json().get("items", [])
        return []
    except:
        return []

def get_allergens():
    try:
        resp = requests.get(f"{API_BASE}/allergens")
        if resp.status_code == 200:
            return resp.json().get("items", [])
        return []
    except:
        return []

# 4. MANAGE TAB
with tab_manage:
    st.header("Manage Prep Items")
    
    stations = get_stations()
    station_map = {s["id"]: s["name"] for s in stations}
    station_options = ["None"] + [s["name"] for s in stations]

    allergens_list = get_allergens()
    allergen_names = [a["name"] for a in allergens_list]

    with st.expander("Add New Item"):
        with st.form("add_prep_item"):
            c1, c2 = st.columns(2)
            new_name = c1.text_input("Item Name")
            new_cat = c2.text_input("Category", value="General")
            
            c3, c4, c5 = st.columns(3)
            new_unit = c3.text_input("Unit", value="unit")
            new_par = c4.number_input("Par Level", min_value=0.0, step=1.0)
            new_oh = c5.number_input("Current On Hand", min_value=0.0, step=1.0)
            
            c6, c7 = st.columns(2)
            new_time = c6.number_input("Prep Time (mins)", min_value=0, step=5, value=30)
            new_station_name = c7.selectbox("Station", options=station_options)
            
            new_allergens = st.multiselect("Allergens", options=allergen_names)

            active = st.checkbox("Active", value=True)
            
            submitted = st.form_submit_button("Create Item")
            if submitted:
                if not new_name:
                    st.error("Name required.")
                else:
                    # Resolve station ID
                    s_id = None
                    if new_station_name != "None":
                         s_id = next((s["id"] for s in stations if s["name"] == new_station_name), None)

                    payload = {
                        "name": new_name,
                        "category": new_cat,
                        "unit": new_unit,
                        "par_level": new_par,
                        "on_hand": new_oh,
                        "prep_time_minutes": new_time,
                        "station_id": s_id,
                        "allergens": new_allergens,
                        "is_active": active
                    }
                    if create_prep_item(payload):
                        st.success(f"Created {new_name}")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("Failed to create item.")

    st.markdown("### Existing Items")
    
    # Editable DataFrame for quick tweaks (Streamlit 1.23+)
    if items:
        df_edit = pd.DataFrame(items)
        # Add friendly station name column for display? 
        # Editing dropdowns in data_editor is harder for dynamic foreign keys.
        # We'll just show station_id for now or let users edit specific fields.
        
        # Let's keep it simple: Basic fields in grid, advanced (station) via form below or ignoring station in grid edit.
        df_edit = df_edit[["id", "name", "category", "unit", "par_level", "prep_time_minutes", "is_active"]]
        
        edited_df = st.data_editor(
            df_edit,
            key="manage_editor",
            use_container_width=True,
            hide_index=True,
            disabled=["id"],
            column_config={
                "prep_time_minutes": st.column_config.NumberColumn("Prep Mins"),
                "is_active": st.column_config.CheckboxColumn("Active?"),
            }
        )
        
        # Check for changes
        if st.button("Save Changes"):
            error_count = 0
            for index, row in edited_df.iterrows():
                # find original
                orig = next((x for x in items if x["id"] == row["id"]), None)
                if not orig: 
                    continue
                
                # check diff
                if (row["name"] != orig["name"] or 
                    row["category"] != orig["category"] or 
                    row["unit"] != orig["unit"] or 
                    row["par_level"] != orig["par_level"] or 
                    row["prep_time_minutes"] != orig.get("prep_time_minutes") or
                    row["is_active"] != orig["is_active"]):
                    
                    payload = {
                        "name": row["name"],
                        "category": row["category"],
                        "unit": row["unit"],
                        "par_level": float(row["par_level"]),
                        "on_hand": orig.get("on_hand", 0), # keep existing
                        "prep_time_minutes": int(row["prep_time_minutes"] or 0),
                        "station_id": orig.get("station_id"), # keep existing
                        "is_active": bool(row["is_active"])
                    }
                    if not update_prep_item(row["id"], payload):
                        error_count += 1
            
            if error_count == 0:
                st.success("All changes saved.")
                time.sleep(0.5)
                st.rerun()
            else:
                st.warning(f"{error_count} items failed to update.")
        
        st.markdown("### Delete Item")
        del_c1, del_c2 = st.columns([3, 1])
        item_to_del = del_c1.selectbox("Select item to delete", options=items, format_func=lambda x: f"{x['name']} ({x['category']})")
        if del_c2.button("Delete (Permanent)", type="primary"):
            if item_to_del:
                if delete_prep_item(item_to_del['id']):
                    st.success(f"Deleted {item_to_del['name']}")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Failed to delete.")

