import streamlit as st
import pandas as pd
import altair as alt
from services import menu, recipes

st.set_page_config(page_title="Menu Engineering", page_icon="📈", layout="wide")

st.title("📈 Menu Engineering")

tab_setup, tab_sales, tab_analyze = st.tabs(["🛠️ Menu Setup", "💵 Log Sales", "📊 Matrix Analysis"])

# --- SETUP TAB ---
with tab_setup:
    st.subheader("Add Menu Item")
    with st.form("add_menu_item"):
        name = st.text_input("Menu Item Name (e.g., Classic Burger)")
        
        # Recipe Link
        all_recipes = recipes.get_all_recipes()
        recipe_options = {r["id"]: r["name"] for r in all_recipes}
        recipe_id = st.selectbox("Link to Recipe (for Costing)", options=[None] + list(recipe_options.keys()), format_func=lambda x: recipe_options[x] if x else "None")
        
        col1, col2 = st.columns(2)
        price = col1.number_input("Selling Price ($)", min_value=0.0, format="%.2f")
        category = col2.selectbox("Category", ["Appetizer", "Main", "Dessert", "Beverage", "Side"])
        
        submitted = st.form_submit_button("Create Item")
        if submitted:
            if not name:
                st.error("Name required")
            else:
                data = {
                    "name": name,
                    "recipe_id": recipe_id,
                    "selling_price": price,
                    "category": category
                }
                msg = menu.create_menu_item(data)
                st.success(msg)

    st.divider()
    st.markdown("### Active Menu")
    items = menu.get_menu_items()
    if items:
        df = pd.DataFrame(items)
        st.dataframe(df[["name", "category", "selling_price"]], use_container_width=True)

# --- SALES TAB ---
with tab_sales:
    st.subheader("Log Sales Mix")
    st.caption("Enter quantity sold for analysis period (e.g., last week).")
    
    items = menu.get_menu_items()
    if not items:
        st.info("No menu items found.")
    else:
        with st.form("log_sales_form"):
            sales_inputs = []
            
            # Group by Category for cleaner UI
            df = pd.DataFrame(items)
            categories = df["category"].unique()
            
            for cat in categories:
                st.markdown(f"**{cat}**")
                cat_items = df[df["category"] == cat]
                
                cols = st.columns(3)
                for idx, row in cat_items.iterrows():
                    col_idx = idx % 3
                    with cols[col_idx]:
                        qty = st.number_input(
                            f"{row['name']}", 
                            min_value=0, 
                            key=f"sale_{row['id']}"
                        )
                        if qty > 0:
                            sales_inputs.append({"menu_item_id": row["id"], "quantity_sold": qty})
            
            submitted_sales = st.form_submit_button("Log Sales", type="primary")
            
            if submitted_sales:
                if not sales_inputs:
                    st.warning("No sales entered.")
                else:
                    msg = menu.log_sales(sales_inputs)
                    st.success(msg)

# --- ANALYSIS TAB ---
with tab_analyze:
    st.subheader("BCG Matrix (Stars & Dogs)")
    
    if st.button("Refresh Analysis"):
        st.rerun()
        
    df = menu.get_matrix_data()
    
    if df.empty:
        st.info("No sufficient sales data for analysis.")
    else:
        # Scatter Plot using Altair
        # X = Popularity (Total Sold), Y = Profitability (CM per Item)
        
        avg_sold = df["total_sold"].mean()
        avg_cm = df["cm_per_item"].mean()
        
        base = alt.Chart(df).encode(
            x=alt.X('total_sold', title='Popularity (Qty Sold)'),
            y=alt.Y('cm_per_item', title='Profitability (CM $)'),
            color='class',
            tooltip=['name', 'class', 'total_sold', 'cm_per_item', 'selling_price', 'unit_cost']
        )
        
        points = base.mark_circle(size=100)
        text = base.mark_text(align='left', dx=10).encode(text='name')
        
        # Quadrant Lines
        rule_x = alt.Chart(pd.DataFrame({'x': [avg_sold]})).mark_rule(color='gray', strokeDash=[3,3]).encode(x='x')
        rule_y = alt.Chart(pd.DataFrame({'y': [avg_cm]})).mark_rule(color='gray', strokeDash=[3,3]).encode(y='y')
        
        chart = (points + text + rule_x + rule_y).properties(height=500).interactive()
        
        st.altair_chart(chart, use_container_width=True)
        
        st.markdown("### Data Table")
        st.dataframe(df, use_container_width=True)
        
        st.markdown("""
        ### Strategy Guide
        - **⭐ Stars**: High Sales, High Margin. **Keep, promote, maintain quality.**
        - **🐎 Plowhorses**: High Sales, Low Margin. **Increase price slightly, reduce portion cost.**
        - **🧩 Puzzles**: Low Sales, High Margin. **Promote heavily, rename, or improve description.**
        - **🐶 Dogs**: Low Sales, Low Margin. **Remove from menu or completely reinvent.**
        """)
