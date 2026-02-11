import streamlit as st
import pandas as pd
from services import inventory

st.set_page_config(page_title="Inventory Management", page_icon="📦", layout="wide")

st.title("📦 Inventory Management")

tab_sheet, tab_setup = st.tabs(["📝 Count Sheet", "⚙️ Setup"])

# --- COUNT SHEET TAB ---
with tab_sheet:
    col_header, col_val = st.columns([3, 1])
    with col_header:
        st.markdown("### 📋 Inventory Counts")
        st.caption("Enter current quantities below.")
    with col_val:
        total_val = inventory.get_inventory_value()
        st.metric("Total Inventory Value", f"${total_val:,.2f}")
    
    if st.button("Refresh Data"):
        st.rerun()

    with st.form("inventory_count_form"):
        sheet_data = inventory.get_sheet_data()
        counts_to_submit = []
        
        # Iterate through areas in order
        for area_name, items in sheet_data.items():
            if not items and area_name == "Unassigned": 
                continue 
                
            with st.expander(f"📍 {area_name}", expanded=True):
                if not items:
                    st.caption("No items in this area.")
                else:
                    # Create a grid for input
                    for item in items:
                        c1, c2, c3, c4 = st.columns([3, 1, 1, 1])
                        c1.markdown(f"**{item['name']}** ({item['unit']})")
                        c2.caption(f"Last: {item['quantity']}")
                        
                        # Input for new count
                        new_qty = c3.number_input(
                            "Count", 
                            min_value=0.0, 
                            value=float(item['quantity']), 
                            key=f"qty_{item['id']}",
                            label_visibility="collapsed"
                        )
                        
                        # Setup submit data
                        counts_to_submit.append({
                            "item_id": item["id"],
                            "quantity": new_qty
                        })
                        
                        # Variance Indicator (Visual only for now)
                        # If we had Par, we could show color. 
                        # future: if new_qty < par: st.error("Low")
                        
        submitted = st.form_submit_button("Submit Counts", type="primary")
        
        if submitted:
            # We need to extract the values from session state because number_input keys
            # might not update 'counts_to_submit' list in real-time if we constructed it before interaction?
            # Actually in Streamlit, the script reruns. `new_qty` holds the current value of the widget.
            # So `counts_to_submit` is correct on submission run.
            
            msg = inventory.submit_count(counts_to_submit, user="Chef") # Todo: Get actual user
            st.success(msg)
            st.rerun()

# --- SETUP TAB ---
with tab_setup:
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("1. Storage Areas")
        with st.form("add_area"):
            new_area = st.text_input("New Area Name (e.g., Walk-in)")
            submitted_area = st.form_submit_button("Add Area")
            if submitted_area and new_area:
                msg = inventory.create_area(new_area)
                st.success(msg)
                st.rerun()
        
        st.divider()
        st.write("Current Areas:")
        areas = inventory.get_areas()
        if areas:
            for a in areas:
                st.text(f"- {a['name']}")
        else:
            st.info("No areas defined.")

    with col2:
        st.subheader("2. Add/Edit Inventory Items")
        
        # Get area options
        area_options = {a['id']: a['name'] for a in areas}
        
        with st.form("add_inventory_item"):
            item_name = st.text_input("Item Name")
            c1, c2 = st.columns(2)
            qty = c1.number_input("Current Qty", min_value=0.0)
            unit = c2.text_input("Unit", value="unit")
            
            c3, c4 = st.columns(2)
            cost = c3.number_input("Cost ($)", min_value=0.0)
            area_id = c4.selectbox("Storage Area", options=list(area_options.keys()), format_func=lambda x: area_options[x]) if area_options else None
            
            category = st.text_input("Category (e.g., Dairy, Produce)")
            
            submitted_item = st.form_submit_button("Save Item")
            
            if submitted_item:
                if not item_name:
                    st.error("Name required")
                else:
                    data = {
                        "name": item_name,
                        "quantity": qty,
                        "unit": unit,
                        "cost": cost,
                        "storage_area_id": area_id,
                        "category": category
                    }
                    msg = inventory.create_item(data)
                    st.success(msg)
