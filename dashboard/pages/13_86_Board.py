import streamlit as st
import pandas as pd
from services import eighty_six

st.set_page_config(page_title="86 Board", page_icon="🚫", layout="wide")

st.title("🚫 86 Board")

tab_board, tab_add = st.tabs(["📋 Active 86s", "➕ 86 an Item"])

# --- BOARD TAB ---
with tab_board:
    active = eighty_six.get_active_86()
    
    if not active:
        st.success("✅ Nothing is 86'd — full menu available!")
    else:
        st.warning(f"⚠️ {len(active)} item(s) currently 86'd")
        
        for item in active:
            with st.container():
                c1, c2, c3 = st.columns([3, 2, 1])
                c1.markdown(f"### 🚫 {item['item_name']}")
                if item.get("reason"):
                    c1.caption(f"Reason: {item['reason']}")
                if item.get("substitution"):
                    c2.info(f"🔄 Sub: **{item['substitution']}**")
                else:
                    c2.caption("No substitution set")
                if c3.button("✅ Restore", key=f"restore_{item['id']}"):
                    eighty_six.resolve_86(item["id"])
                    st.rerun()
                st.divider()

# --- ADD TAB ---
with tab_add:
    st.subheader("86 an Item")
    
    with st.form("eighty_six_form"):
        name = st.text_input("Item Name")
        reason = st.selectbox("Reason", ["Out of stock", "Quality issue", "86'd by Chef", "Seasonal", "Other"])
        sub = st.text_input("Substitution (optional)")
        reported_by = st.text_input("Reported By")
        
        if st.form_submit_button("🚫 86 It"):
            if name:
                msg = eighty_six.eighty_six_item(name, reason, sub, reported_by)
                st.success(msg)
                st.rerun()
            else:
                st.error("Item name required.")
