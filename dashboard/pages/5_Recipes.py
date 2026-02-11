import streamlit as st
import pandas as pd
from services import recipes, inventory

st.set_page_config(page_title="Recipe Database", page_icon="👨‍🍳", layout="wide")

st.title("👨‍🍳 Recipe Database")

tab_view, tab_add = st.tabs(["📖 Cookbook", "✏️ New Recipe"])

# --- VIEW TAB ---
with tab_view:
    all_recipes = recipes.get_all_recipes()
    
    if not all_recipes:
        st.info("No recipes found. Create one in the 'New Recipe' tab.")
    else:
        # Sidebar or Grid selection could work. Let's use a Selectbox for simplicity or a grid.
        # Grid:
        
        col_list, col_detail = st.columns([1, 2])
        
        with col_list:
            st.subheader("Recipes")
            # Filter
            station_filter = st.selectbox("Filter by Station", ["All"] + list(set(r["station"] for r in all_recipes if r["station"])))
            
            filtered = all_recipes
            if station_filter != "All":
                filtered = [r for r in all_recipes if r["station"] == station_filter]
                
            selected_recipe_id = st.radio(
                "Select Recipe", 
                options=[r["id"] for r in filtered], 
                format_func=lambda x: [r["name"] for r in filtered if r["id"] == x][0]
            )
            
        with col_detail:
            if selected_recipe_id:
                # Update Costs Button
                from services import costing
                if st.button("🔄 Refresh Costs from Inventory"):
                    msg = costing.update_ingredient_costs(selected_recipe_id)
                    st.toast(msg)
                    st.rerun()

                details = recipes.get_recipe_details(selected_recipe_id)
                cost_data = costing.calculate_recipe_cost(selected_recipe_id)
                
                if details:
                    st.markdown(f"## {details['name']}")
                    st.caption(f"Yield: {details['yield_amount']} {details['yield_unit']} | Station: {details['station'] or 'N/A'}")
                    
                    # Metrics
                    m1, m2 = st.columns(2)
                    m1.metric("Total Recipe Cost", f"${cost_data['total_cost']:,.2f}")
                    m2.metric(f"Cost per {details['yield_unit']}", f"${cost_data['cost_per_yield']:,.2f}")
                    
                    st.divider()
                    
                    c1, c2 = st.columns(2)
                    with c1:
                        st.markdown("### Ingredients")
                        for ing in details["ingredients"]:
                            cost_display = f"(${ing['cost']:,.2f})" if ing.get('cost') else "(-)"
                            st.text(f"• {ing['quantity']} {ing['unit']} {ing['display_name']} {cost_display}")
                            if ing['notes']:
                                st.caption(f"  ({ing['notes']})")
                    
                    with c2:
                        st.markdown("### Method")
                        st.write(details['method'])
                        
                    # Print Button (Mock)
                    st.button("🖨️ Print Chef Card")

# --- ADD TAB ---
with tab_add:
    st.subheader("Create New Recipe")
    
    with st.form("new_recipe_form"):
        name = st.text_input("Recipe Name (e.g., Marinara Sauce)")
        
        c1, c2, c3 = st.columns(3)
        yield_amt = c1.number_input("Yield Amount", min_value=0.0)
        yield_unit = c2.text_input("Yield Unit (e.g., Liters, Portions)")
        station = c3.selectbox("Station", ["Prep", "Sauté", "Grill", "Pantry", "Pastry", "Bar"])
        
        method = st.text_area("Method (Steps)", height=200)
        
        st.markdown("### Ingredients (Add via text for now, bulk)")
        st.caption("Simplified entry for v1. Format: 'Item Name, Qty, Unit' per line? No, let's use a dynamic editor or just loop inputs.")
        
        # Simple approach: Text area for bulk parse or fixed slots.
        # Better: Data Editor? 
        # Let's use a dedicated list in session state? Too complex for single form.
        # Let's use a rudimentary approach: 5 slots.
        
        ingredients_to_add = []
        st.markdown("---")
        for i in range(5):
            cc1, cc2, cc3 = st.columns([3, 1, 1])
            i_name = cc1.text_input(f"Ingredient {i+1}", key=f"ing_name_{i}")
            i_qty = cc2.number_input("Qty", key=f"ing_qty_{i}", min_value=0.0)
            i_unit = cc3.text_input("Unit", key=f"ing_unit_{i}")
            
            if i_name:
                ingredients_to_add.append({
                    "item_name_text": i_name,
                    "quantity": i_qty,
                    "unit": i_unit,
                    "inventory_item_id": None # Link logic later
                })
        
        submitted = st.form_submit_button("Save Recipe")
        
        if submitted:
            if not name:
                st.error("Name required")
            else:
                r_data = {
                    "name": name,
                    "yield_amount": yield_amt,
                    "yield_unit": yield_unit,
                    "station": station,
                    "method": method,
                    "category": "Main" # Default
                }
                msg = recipes.create_recipe(r_data, ingredients_to_add)
                st.success(msg)
