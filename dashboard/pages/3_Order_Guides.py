import streamlit as st
import pandas as pd
from services import providers, order_guide

st.set_page_config(page_title="Order Guides", page_icon="📋", layout="wide")

st.title("📋 Order Guide Builder")

# 1. Select Vendor
vendors = providers.get_all_vendors()
if not vendors:
    st.warning("No vendors found. Please add vendors in the Provider Directory first.")
    st.stop()

vendor_options = {v['id']: v['name'] for v in vendors}
selected_vendor_id = st.selectbox("Select Vendor", options=list(vendor_options.keys()), format_func=lambda x: vendor_options[x])

# 2. Tabs
tab_view, tab_add = st.tabs(["Edit Guide", "Add Items"])

# --- VIEW / EDIT TAB ---
with tab_view:
    items = order_guide.get_items_by_vendor(selected_vendor_id)
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.caption(f"Showing {len(items)} items for **{vendor_options[selected_vendor_id]}**")
    
    with col2:
        if items:
            csv_data = order_guide.export_guide_csv(selected_vendor_id)
            st.download_button(
                label="📥 Export CSV",
                data=csv_data,
                file_name=f"order_guide_{vendor_options[selected_vendor_id].replace(' ', '_')}.csv",
                mime="text/csv"
            )

    if items:
        # Prepare data for editor
        df = pd.DataFrame(items)
        
        # We'll use data_editor for quick updates
        # Hide ID and Vendor ID
        display_df = df.drop(columns=['vendor_id', 'created_at', 'updated_at'])
        
        edited_df = st.data_editor(
            display_df,
            column_config={
                "id": st.column_config.NumberColumn("ID", disabled=True),
                "item_name": "Item Name",
                "pack_size": "Pack Size",
                "price": st.column_config.NumberColumn("Price ($)", format="$%.2f"),
                "par_level": st.column_config.NumberColumn("Par Level"),
                "category": "Category",
                "is_active": st.column_config.CheckboxColumn("Active?"),
                "notes": "Notes"
            },
            hide_index=True,
            num_rows="dynamic",
            key="order_guide_editor"
        )
        
        # Handle edits (this is a bit complex in Streamlit, simplified approach:
        # Detect changes and apply them. For simplicity in this iteration, we might just
        # allow adding in the other tab and use this mainly for view or simple bulk updates if we implemented the callback).
        # 
        # Actually, st.data_editor returns the state. We can use `on_change` or check session state diffs.
        # For now, let's keep it simple: The editor visualizes. 
        # REAL IMPLEMENTATION: We would need a button to "Save Changes" if we want bulk edit, 
        # or use the add tab for reliable CRUD.
        
        # Let's add a "Save Changes" button for the editor
        if st.button("Save Changes"):
            # This is tricky because `edited_df` is the RESULT. We need to compare specific rows or just update all.
            # Updating all is safer for "simple" logic but inefficient. 
            # Better approach: Iterate and update.
            
            progress_bar = st.progress(0)
            for index, row in edited_df.iterrows():
                # We need the ID. Since we dropped it from view but it might be in the index if we didn't reset it?
                # Ah, we dropped columns but `display_df` still has data. 
                # Wait, `display_df` dropped columns. We need to keep ID but disable editing.
                
                # Re-do: Keep ID in the editor but disabled.
                item_id = row['id']
                
                # Check for changes (optimization: only update if changed? simplified: just update)
                update_data = {
                    "item_name": row["item_name"],
                    "pack_size": row["pack_size"],
                    "price": row["price"],
                    "par_level": row["par_level"],
                    "category": row["category"],
                    "notes": row["notes"],
                    "is_active": 1 if row["is_active"] else 0
                }
                order_guide.update_item(item_id, update_data)
                progress_bar.progress((index + 1) / len(edited_df))
            
            st.success("Order Guide Updated!")
            st.rerun()

    else:
        st.info("No items in this order guide yet.")

# --- ADD TAB ---
with tab_add:
    st.subheader(f"Add Item to {vendor_options[selected_vendor_id]}")
    
    with st.form("add_item_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        item_name = col1.text_input("Item Name (Required)")
        category = col2.selectbox("Category", ["Produce", "Dairy", "Meat", "Dry Goods", "Frozen", "Supplies", "Other"])
        
        col3, col4, col5 = st.columns(3)
        pack_size = col3.text_input("Pack Size", placeholder="e.g. 50lb Case")
        price = col4.number_input("Price ($)", min_value=0.0, step=0.01)
        par = col5.number_input("Par Level", min_value=0.0, step=0.1)
        
        notes = st.text_area("Notes")
        
        submitted = st.form_submit_button("Add Item")
        
        if submitted:
            if not item_name:
                st.error("Item Name is required.")
            else:
                data = {
                    "vendor_id": selected_vendor_id,
                    "item_name": item_name,
                    "pack_size": pack_size,
                    "price": price,
                    "par_level": par,
                    "category": category,
                    "notes": notes
                }
                result = order_guide.add_item(data)
                if "successfully" in result:
                    st.success(result)
                    st.rerun() # Refresh getting items
                else:
                    st.error(result)
