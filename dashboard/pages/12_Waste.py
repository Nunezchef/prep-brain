import streamlit as st
import pandas as pd
from services import waste

st.set_page_config(page_title="Waste Tracker", page_icon="🗑️", layout="wide")

st.title("🗑️ Waste & Spoilage Log")

tab_log, tab_report = st.tabs(["📝 Log Waste", "📊 Report"])

# --- LOG TAB ---
with tab_log:
    st.subheader("Log Waste Event")
    
    with st.form("waste_form"):
        item_name = st.text_input("Item Name")
        
        c1, c2, c3 = st.columns(3)
        qty = c1.number_input("Quantity", min_value=0.0)
        unit = c2.text_input("Unit")
        valor = c3.number_input("Dollar Value ($)", min_value=0.0, format="%.2f")
        
        reason = st.selectbox("Reason", ["Expired", "Spoiled", "Overproduction", "Dropped", "Burnt", "Contaminated", "Other"])
        category = st.selectbox("Category", ["Raw", "Prepped", "Finished"])
        logged_by = st.text_input("Logged By")
        
        if st.form_submit_button("Log Waste"):
            if not item_name:
                st.error("Item name required.")
            else:
                data = {
                    "item_name": item_name,
                    "quantity": qty,
                    "unit": unit,
                    "dollar_value": valor,
                    "reason": reason,
                    "category": category,
                    "logged_by": logged_by
                }
                msg = waste.log_waste(data)
                st.success(msg)

# --- REPORT TAB ---
with tab_report:
    st.subheader("Waste Summary")
    
    summary = waste.get_waste_summary()
    
    st.metric("Total Waste ($)", f"${summary['grand_total']:,.2f}")
    
    if summary["by_reason"]:
        st.markdown("### By Reason")
        df_reason = pd.DataFrame(summary["by_reason"])
        st.bar_chart(df_reason.set_index("reason")["total_value"])
        st.dataframe(df_reason, use_container_width=True)
    
    st.divider()
    st.markdown("### Recent Entries")
    history = waste.get_waste_history()
    if history:
        st.dataframe(pd.DataFrame(history), use_container_width=True)
    else:
        st.info("No waste logged yet.")
