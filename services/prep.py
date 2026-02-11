import sqlite3
import logging
from typing import List, Optional, Dict, Any
from services import memory, inventory

logger = logging.getLogger(__name__)

def _get_conn() -> sqlite3.Connection:
    return memory.get_conn()

def set_recipe_par(recipe_id: int, par_level: float, output_item_id: Optional[int] = None) -> str:
    """Set the par level and link output inventory item for a recipe."""
    con = _get_conn()
    try:
        if output_item_id is not None:
             con.execute(
                "UPDATE recipes SET par_level = ?, output_inventory_item_id = ? WHERE id = ?",
                (par_level, output_item_id, recipe_id)
            )
        else:
             con.execute(
                "UPDATE recipes SET par_level = ? WHERE id = ?",
                (par_level, recipe_id)
            )
        con.commit()
        return "Par level updated."
    except Exception as e:
        logger.error(f"Error setting par: {e}")
        return f"Error: {str(e)}"
    finally:
        con.close()

def generate_prep_list() -> str:
    """
    Generate the logical prep list.
    Logic:
    1. For each recipe with Par > 0:
    2. Check linked Inventory Item quantity (if linked).
    3. If not linked, assume 0 on hand (or manual entry required, here we assume full par needed).
    4. Need = Par - On Hand.
    5. If Need > 0, insert into prep_list_items (if not already 'todo').
    """
    con = _get_conn()
    try:
        # Get active recipes with pars
        cur = con.execute("SELECT id, name, par_level, yield_unit, output_inventory_item_id FROM recipes WHERE is_active=1 AND par_level > 0")
        recipes = cur.fetchall()
        
        generated_count = 0
        
        for r in recipes:
            r_id = r["id"]
            par = r["par_level"]
            out_id = r["output_inventory_item_id"]
            
            on_hand = 0.0
            if out_id:
                # Get current inventory
                cur_inv = con.execute("SELECT quantity FROM inventory_items WHERE id = ?", (out_id,))
                row = cur_inv.fetchone()
                if row:
                    on_hand = row["quantity"]
            
            need = par - on_hand
            
            if need > 0:
                # Check if already exists as 'todo' to avoid duplicates
                cur_check = con.execute(
                    "SELECT id FROM prep_list_items WHERE recipe_id = ? AND status = 'todo'", 
                    (r_id,)
                )
                if not cur_check.fetchone():
                    con.execute(
                        "INSERT INTO prep_list_items (recipe_id, need_quantity, unit, status) VALUES (?, ?, ?, 'todo')",
                        (r_id, need, r["yield_unit"])
                    )
                    generated_count += 1
        
        con.commit()
        return f"Generated {generated_count} prep tasks."
    except Exception as e:
        logger.error(f"Error generating prep list: {e}")
        return f"Error: {str(e)}"
    finally:
        con.close()

def get_prep_list(status: str = 'todo') -> List[dict]:
    """Get prep list items joined with recipe details."""
    con = _get_conn()
    try:
        cur = con.execute(
            """
            SELECT pli.*, r.name as recipe_name, r.station
            FROM prep_list_items pli
            JOIN recipes r ON pli.recipe_id = r.id
            WHERE pli.status = ?
            ORDER BY r.station, r.name
            """,
            (status,)
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        con.close()

def complete_task(item_id: int) -> str:
    """Mark a prep task as done."""
    con = _get_conn()
    try:
        con.execute("UPDATE prep_list_items SET status = 'done' WHERE id = ?", (item_id,))
        con.commit()
        return "Task completed."
    finally:
        con.close()

def clear_completed() -> str:
    """Clear done tasks."""
    con = _get_conn()
    try:
        con.execute("DELETE FROM prep_list_items WHERE status = 'done'")
        con.commit()
        return "Completed list cleared."
    finally:
        con.close()
