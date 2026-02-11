import streamlit as st
import pandas as pd
from services import receiving

st.set_page_config(page_title="Receiving Log", page_icon="📦", layout="wide")

st.title("📦 Receiving & Invoice Log")

tab_log, tab_history = st.tabs(["📝 Log Delivery", "📜 History"])

# --- LOG TAB ---
with tab_log:
    st.subheader("Receive Items")
    st.caption("Use this when a delivery arrives to record items and do quality checks.")
    
    with st.form("receiving_form"):
        invoice_num = st.text_input("Invoice # (optional)")
        
        c1, c2 = st.columns(2)
        item_name = c1.text_input("Item Name")
        qty = c2.number_input("Quantity", min_value=0.0)
        
        c3, c4 = st.columns(2)
        unit = c3.text_input("Unit")
        unit_cost = c4.number_input("Unit Cost ($)", min_value=0.0, format="%.2f")
        
        c5, c6 = st.columns(2)
        temp = c5.number_input("Temp Check (°F)", value=0.0)
        quality = c6.selectbox("Quality", ["Pass", "Reject"])
        
        notes = st.text_input("Notes")
        received_by = st.text_input("Received By")
        
        total = qty * unit_cost
        st.metric("Line Total", f"${total:,.2f}")
        
        if st.form_submit_button("Log Item"):
            if not item_name:
                st.error("Item name required.")
            else:
                data = {
                    "invoice_number": invoice_num,
                    "item_name": item_name,
                    "quantity_received": qty,
                    "unit": unit,
                    "unit_cost": unit_cost,
                    "total_cost": total,
                    "temperature_check": temp if temp else None,
                    "quality_ok": 1 if quality == "Pass" else 0,
                    "notes": notes,
                    "received_by": received_by
                }
                msg = receiving.log_receiving(data)
                st.success(msg)

# --- HISTORY TAB ---
with tab_history:
    st.subheader("Receiving History")
    history = receiving.get_receiving_history()
    
    if not history:
        st.info("No receiving records yet.")
    else:
        df = pd.DataFrame(history)
        
        # Summary Metrics
        total_value = df["total_cost"].sum()
        st.metric("Total Received Value", f"${total_value:,.2f}")
        
        st.dataframe(df[["received_at", "item_name", "quantity_received", "unit", "unit_cost", "total_cost", "quality_ok"]], use_container_width=True)
