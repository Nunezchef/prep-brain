import streamlit as st
import pandas as pd
from services import allergens, recipes

st.set_page_config(page_title="Allergen Matrix", page_icon="⚠️", layout="wide")

st.title("⚠️ Allergen & Dietary Matrix")

tab_matrix, tab_tag = st.tabs(["📊 Allergen Matrix", "🏷️ Tag Recipes"])

# --- MATRIX TAB ---
with tab_matrix:
    matrix_data = allergens.get_allergen_matrix()
    
    if not matrix_data["recipes"]:
        st.info("No recipes found. Add recipes first.")
    else:
        # Build DataFrame
        rows = []
        for r in matrix_data["recipes"]:
            row = {"Recipe": r["name"], "Station": r["station"] or ""}
            for a_name in matrix_data["allergens"]:
                row[a_name] = "⚠️" if r["allergens"].get(a_name) else ""
            rows.append(row)
        
        df = pd.DataFrame(rows)
        
        # Filters
        station_filter = st.selectbox("Filter by Station", ["All"] + list(df["Station"].unique()))
        if station_filter != "All":
            df = df[df["Station"] == station_filter]
        
        st.dataframe(df, use_container_width=True, height=500)
        
        st.caption("⚠️ = Contains this allergen")

# --- TAG TAB ---
with tab_tag:
    st.subheader("Tag Recipe Allergens")
    
    all_recipes = recipes.get_all_recipes()
    all_allergens = allergens.get_all_allergens()
    
    if not all_recipes:
        st.info("No recipes found.")
    else:
        selected_r = st.selectbox(
            "Select Recipe",
            options=[r["id"] for r in all_recipes],
            format_func=lambda x: [r["name"] for r in all_recipes if r["id"]==x][0]
        )
        
        if selected_r:
            # Get current allergens for this recipe
            current = allergens.get_recipe_allergens(selected_r)
            current_ids = [a["id"] for a in current]
            
            with st.form("allergen_tag_form"):
                st.markdown("Check all that apply:")
                
                selected_ids = []
                cols = st.columns(3)
                for i, a in enumerate(all_allergens):
                    col_idx = i % 3
                    if cols[col_idx].checkbox(a["name"], value=(a["id"] in current_ids), key=f"a_{a['id']}"):
                        selected_ids.append(a["id"])
                
                if st.form_submit_button("Save Allergens"):
                    msg = allergens.set_recipe_allergens(selected_r, selected_ids)
                    st.success(msg)
                    st.rerun()
