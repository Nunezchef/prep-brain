import sqlite3
import logging
import pandas as pd
from typing import List, Optional, Dict, Any
from services import memory, costing

logger = logging.getLogger(__name__)

def _get_conn() -> sqlite3.Connection:
    return memory.get_conn()

def create_menu_item(data: Dict[str, Any]) -> str:
    """Create a new menu item."""
    con = _get_conn()
    try:
        con.execute(
            """
            INSERT INTO menu_items (name, recipe_id, selling_price, category)
            VALUES (?, ?, ?, ?)
            """,
            (data.get("name"), data.get("recipe_id"), data.get("selling_price"), data.get("category"))
        )
        con.commit()
        return f"Menu Item '{data.get('name')}' created."
    except Exception as e:
        logger.error(f"Error creating menu item: {e}")
        return f"Error: {str(e)}"
    finally:
        con.close()

def get_menu_items() -> List[dict]:
    """Get all active menu items."""
    con = _get_conn()
    try:
        cur = con.execute("SELECT * FROM menu_items WHERE is_active = 1 ORDER BY category, name")
        return [dict(row) for row in cur.fetchall()]
    finally:
        con.close()

def log_sales(sales_data: List[Dict[str, Any]]) -> str:
    """
    Log sales for multiple items.
    sales_data: List of {menu_item_id, quantity_sold, date_logged(optional)}
    """
    con = _get_conn()
    try:
        updated = 0
        for item in sales_data:
            if item.get("quantity_sold", 0) > 0:
                con.execute(
                    """
                    INSERT INTO sales_log (menu_item_id, quantity_sold, date_logged)
                    VALUES (?, ?, COALESCE(?, CURRENT_DATE))
                    """,
                    (item["menu_item_id"], item["quantity_sold"], item.get("date_logged"))
                )
                updated += 1
        con.commit()
        return f"Logged sales for {updated} items."
    except Exception as e:
        logger.error(f"Error logging sales: {e}")
        return f"Error: {str(e)}"
    finally:
        con.close()

def get_matrix_data() -> pd.DataFrame:
    """
    Prepare data for BCG Matrix Analysis.
    Returns DataFrame with columns: 
    [Name, Category, Sold, Total Sales ($), Total Cost ($), Margin ($), CM per Item, Pop %, Category Avg Sales]
    """
    con = _get_conn()
    try:
        # 1. Get Sales Aggregated by Item
        sales_df = pd.read_sql_query(
            """
            SELECT 
                mi.id, mi.name, mi.category, mi.selling_price, mi.recipe_id,
                SUM(sl.quantity_sold) as total_sold
            FROM menu_items mi
            JOIN sales_log sl ON mi.id = sl.menu_item_id
            WHERE sl.quantity_sold > 0
            GROUP BY mi.id
            """, con
        )
        
        if sales_df.empty:
            return pd.DataFrame()
            
        # 2. Add Cost Data (slow loop but fine for small menu)
        # We need standard cost from recipe
        costs = []
        for _, row in sales_df.iterrows():
            r_id = row["recipe_id"]
            if r_id:
                # We calculate fresh or use stored? Let's calc fresh snapshot for analysis
                c_metrics = costing.calculate_recipe_cost(r_id)
                costs.append(c_metrics["total_cost"]) # Assuming recipe yield = 1 portion for simplicity or we need portion divisor
                # Todo: Handle Yield. If recipe yields 10L and portion is 0.5L, we have a disconnect.
                # For v1, we assume Recipe = 1 Portion or Recipe Yield matches Menu Item.
                # Let's use cost_per_yield as "Unit Cost" assuming Recipe Yield is the unit we sell (e.g. 1 Burger).
            else:
                costs.append(0.0)
        
        sales_df["unit_cost"] = costs
        
        # 3. Calculate Derived Metrics
        sales_df["revenue"] = sales_df["total_sold"] * sales_df["selling_price"]
        sales_df["cogs"] = sales_df["total_sold"] * sales_df["unit_cost"]
        sales_df["gross_profit"] = sales_df["revenue"] - sales_df["cogs"]
        sales_df["cm_per_item"] = sales_df["selling_price"] - sales_df["unit_cost"]
        
        # 4. Classify (Simple Global Avg for now, or per category)
        avg_sold = sales_df["total_sold"].mean()
        avg_cm = sales_df["cm_per_item"].mean() # Weighted avg might be better but simple avg is standard BCG start
        
        def classify(row):
            high_pop = row["total_sold"] >= avg_sold
            high_marg = row["cm_per_item"] >= avg_cm
            
            if high_pop and high_marg: return "Star"
            if high_pop and not high_marg: return "Plowhorse"
            if not high_pop and high_marg: return "Puzzle"
            return "Dog"
            
        sales_df["class"] = sales_df.apply(classify, axis=1)
        
        return sales_df
        
    finally:
        con.close()
