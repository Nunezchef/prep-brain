import streamlit as st
import requests
import pandas as pd
from datetime import date
import time

# --- CONFIG ---
API_BASE = "http://localhost:8000/api"

st.set_page_config(page_title="Receiving Log", page_icon="📦", layout="wide")

st.title("📦 Receiving Log")
st.markdown("Track invoices and delivery issues.")

# --- HELPERS ---
def get_logs():
    try:
        resp = requests.get(f"{API_BASE}/receiving")
        if resp.status_code == 200:
            return resp.json().get("items", [])
        return []
    except:
        return []

def create_log(payload):
    try:
        resp = requests.post(f"{API_BASE}/receiving", json=payload)
        return resp.status_code == 200
    except:
        return False

# --- TABS ---
tab_log, tab_history = st.tabs(["📝 Log Invoice", "📜 History"])

# 1. LOG FORM
with tab_log:
    st.header("New Entry")
    with st.form("receiving_form"):
        c1, c2 = st.columns(2)
        log_date = c1.date_input("Date", value=date.today())
        supplier = c2.text_input("Supplier Name")
        
        c3, c4 = st.columns(2)
        inv_num = c3.text_input("Invoice Number (Optional)")
        total = c4.number_input("Total Amount ($)", min_value=0.0, step=0.01)
        
        issue = st.checkbox("Delivery Issue? (Missing items, damaged goods, etc.)")
        notes = st.text_area("Notes", placeholder="Details about the delivery or issues...")
        
        submitted = st.form_submit_button("Log Invoice", type="primary")
        
        if submitted:
            if not supplier:
                st.error("Supplier name required.")
            else:
                payload = {
                    "date": log_date.isoformat(),
                    "supplier": supplier,
                    "invoice_number": inv_num,
                    "total_amount": total,
                    "has_issue": issue,
                    "notes": notes
                }
                
                if create_log(payload):
                    st.success("Logged successfully!")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Failed to log invoice.")

# 2. HISTORY
with tab_history:
    st.header("Recent Invoices")
    logs = get_logs()
    
    if not logs:
        st.info("No logs found.")
    else:
        df = pd.DataFrame(logs)
        
        # Style
        # Highlight issues
        def highlight_issues(row):
            return ['background-color: #ffcccc' if row['has_issue'] else '' for _ in row]

        st.dataframe(
            df[["date", "supplier", "invoice_number", "total_amount", "has_issue", "notes"]].style.apply(highlight_issues, axis=1),
            use_container_width=True,
            column_config={
                "total_amount": st.column_config.NumberColumn("Total", format="$%.2f"),
                "has_issue": st.column_config.CheckboxColumn("Issue?"),
            }
        )
