import sqlite3
import logging
from typing import List, Optional, Dict, Any
from pathlib import Path
import yaml
from services import memory

logger = logging.getLogger(__name__)

def _get_conn() -> sqlite3.Connection:
    return memory.get_conn()

def calculate_recipe_cost(recipe_id: int) -> Dict[str, float]:
    """
    Calculate cost metrics for a recipe based on its ingredients' currently stored 'cost' value.
    Returns: {
        "total_cost": float,
        "cost_per_yield": float
    }
    """
    con = _get_conn()
    try:
        # Get Recipe Yield
        cur = con.execute("SELECT yield_amount FROM recipes WHERE id = ?", (recipe_id,))
        row = cur.fetchone()
        if not row:
            return {"total_cost": 0.0, "cost_per_yield": 0.0}
        
        yield_amount = row["yield_amount"] or 1.0
        
        # Sum Ingredient Costs
        cur = con.execute("SELECT SUM(cost) as total FROM recipe_ingredients WHERE recipe_id = ?", (recipe_id,))
        total_cost = cur.fetchone()["total"] or 0.0
        
        return {
            "total_cost": total_cost,
            "cost_per_yield": total_cost / yield_amount if yield_amount > 0 else 0.0
        }
    finally:
        con.close()

def update_ingredient_costs(recipe_id: int) -> str:
    """
    Updates the 'cost' field for all ingredients in a recipe based on current Inventory prices.
    ASSUMPTION: Units match or we use cost per unit from inventory directly.
    Logic: 
    - If ingredient linked to inventory_item:
        - Get inventory_item.cost (Cost per Unit).
        - ingredient.cost = inventory_item.cost * ingredient.quantity
    - Else:
        - cost = 0 (or keep existing?) -> Set to 0 if unknown.
    """
    con = _get_conn()
    try:
        # Get ingredients
        cur = con.execute(
            """
            SELECT ri.id, ri.quantity, ii.cost as inv_cost
            FROM recipe_ingredients ri
            LEFT JOIN inventory_items ii ON ri.inventory_item_id = ii.id
            WHERE ri.recipe_id = ?
            """,
            (recipe_id,)
        )
        rows = cur.fetchall()
        
        updated = 0
        for row in rows:
            ri_id = row["id"]
            qty = row["quantity"]
            unit_cost = row["inv_cost"]
            
            if unit_cost is not None:
                new_cost = qty * unit_cost
                con.execute("UPDATE recipe_ingredients SET cost = ? WHERE id = ?", (new_cost, ri_id))
                updated += 1
            else:
                # If no link or no cost, maybe leave it? Or set 0? 
                # Let's leave it alone if manual entry was used, unless we want to enforce sync.
                # For now, if no link, we can't update it from inventory.
                pass
        
        con.commit()
        return f"Updated costs for {updated} ingredients."
    except Exception as e:
        logger.error(f"Error updating costs: {e}")
        return f"Error: {str(e)}"
    finally:
        con.close()
