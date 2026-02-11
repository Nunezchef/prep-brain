import streamlit as st
import pandas as pd
from services import prep, recipes, inventory

st.set_page_config(page_title="Prep List", page_icon="🔪", layout="wide")

st.title("🔪 Prep Operations")

tab_list, tab_config = st.tabs(["📋 Daily Prep List", "⚙️ Configure Pars"])

# --- DAILY LIST TAB ---
with tab_list:
    col_act, col_stat = st.columns([2, 1])
    with col_act:
        if st.button("🚀 Generate Daily List", type="primary"):
            with st.spinner("Calculating needs based on inventory..."):
                msg = prep.generate_prep_list()
            st.success(msg)
            st.rerun()
            
    with col_stat:
        if st.button("Clear Completed"):
            prep.clear_completed()
            st.rerun()
            
    st.divider()
    
    # Todo List
    todos = prep.get_prep_list('todo')
    
    if not todos:
        st.info("Prep list is empty. Good job! or Click Generate.")
    else:
        st.markdown("### To Do")
        
        # Group by Station
        df = pd.DataFrame(todos)
        stations = df["station"].unique()
        
        for station in stations:
            st.markdown(f"#### 📍 {station or 'General'}")
            station_items = df[df["station"] == station]
            
            for index, row in station_items.iterrows():
                c1, c2, c3 = st.columns([4, 2, 1])
                c1.markdown(f"**{row['recipe_name']}**")
                c2.caption(f"Need: {row['need_quantity']} {row['unit']}")
                
                if c3.button("Done", key=f"done_{row['id']}"):
                    prep.complete_task(row['id'])
                    st.rerun()
                    
    # Done List
    dones = prep.get_prep_list('done')
    if dones:
        with st.expander("Completed Items"):
            st.dataframe(pd.DataFrame(dones)[['recipe_name', 'need_quantity', 'unit']], use_container_width=True)


# --- CONFIG TAB ---
with tab_config:
    st.subheader("Set Par Levels")
    st.caption("Define how much you need on hand at all times.")
    
    all_recipes = recipes.get_all_recipes()
    
    # Grid Edit would be nice. Let's do a loop or specialized form.
    # Simple form for now.
    
    selected_r_id = st.selectbox("Select Recipe", options=[r["id"] for r in all_recipes], format_func=lambda x: [r["name"] for r in all_recipes if r["id"]==x][0])
    
    if selected_r_id:
        # Get details to see current par
        details = recipes.get_recipe_details(selected_r_id)
        current_par = details.get("par_level", 0.0)
        current_out = details.get("output_inventory_item_id")
        
        with st.form("par_config"):
            new_par = st.number_input("Par Level", value=float(current_par), min_value=0.0)
            
            # Map Output Item
            # Get inventory items
            # This is expensive if list is huge, but fine for v1
            inv_items = inventory.get_sheet_data() # Returns grouped dict. Flatten it.
            flat_inv = []
            for area, items in inv_items.items():
                flat_inv.extend(items)
                
            inv_opts = {i["id"]: i["name"] for i in flat_inv}
            
            out_item = st.selectbox(
                "Link to Inventory Item (Output)", 
                options=[None] + list(inv_opts.keys()), 
                format_func=lambda x: inv_opts[x] if x else "No Link (Always Prep Full Par)",
                index=list(inv_opts.keys()).index(current_out) + 1 if current_out in inv_opts else 0
            )
            
            if st.form_submit_button("Save Settings"):
                msg = prep.set_recipe_par(selected_r_id, new_par, out_item)
                st.success(msg)
