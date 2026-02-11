import sqlite3
import logging
from typing import List, Optional, Dict, Any
from services import memory

DB_PATH = memory.get_db_path()

logger = logging.getLogger(__name__)

def _get_conn() -> sqlite3.Connection:
    return memory.get_conn()

def create_recipe(data: Dict[str, Any], ingredients: List[Dict[str, Any]]) -> str:
    """
    Create a new recipe with ingredients.
    data: {name, yield_amount, yield_unit, station, category, method}
    ingredients: List of {inventory_item_id, item_name_text, quantity, unit, notes}
    """
    con = _get_conn()
    try:
        # Insert Recipe
        cur = con.execute(
            """
            INSERT INTO recipes (name, yield_amount, yield_unit, station, category, method)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                data.get("name"), 
                data.get("yield_amount"), 
                data.get("yield_unit"), 
                data.get("station"), 
                data.get("category"), 
                data.get("method")
            )
        )
        recipe_id = cur.lastrowid
        
        # Insert Ingredients
        for ing in ingredients:
            con.execute(
                """
                INSERT INTO recipe_ingredients (
                    recipe_id,
                    inventory_item_id,
                    item_name_text,
                    quantity,
                    unit,
                    canonical_value,
                    canonical_unit,
                    display_original,
                    display_pretty,
                    notes
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    recipe_id,
                    ing.get("inventory_item_id"),
                    ing.get("item_name_text"),
                    ing.get("quantity"),
                    ing.get("unit"),
                    ing.get("canonical_value", ing.get("quantity")),
                    ing.get("canonical_unit", ing.get("unit")),
                    ing.get("display_original"),
                    ing.get("display_pretty"),
                    ing.get("notes")
                )
            )
        
        con.commit()
        return f"Recipe '{data.get('name')}' created successfully."
    except Exception as e:
        logger.error(f"Error creating recipe: {e}")
        return f"Error: {str(e)}"
    finally:
        con.close()

def get_all_recipes() -> List[dict]:
    """Get all recipes (summary)."""
    con = _get_conn()
    try:
        cur = con.execute("SELECT * FROM recipes WHERE is_active = 1 ORDER BY name")
        return [dict(row) for row in cur.fetchall()]
    finally:
        con.close()

def get_recipe_details(recipe_id: int) -> Optional[Dict[str, Any]]:
    """Get full recipe details including ingredients."""
    con = _get_conn()
    try:
        # Recipe
        cur = con.execute("SELECT * FROM recipes WHERE id = ?", (recipe_id,))
        recipe_row = cur.fetchone()
        if not recipe_row:
            return None
        
        recipe = dict(recipe_row)
        
        # Ingredients (Join with inventory for name if linked)
        cur = con.execute(
            """
            SELECT ri.*, ii.name as inventory_name 
            FROM recipe_ingredients ri
            LEFT JOIN inventory_items ii ON ri.inventory_item_id = ii.id
            WHERE ri.recipe_id = ?
            """,
            (recipe_id,)
        )
        ingredients = [dict(row) for row in cur.fetchall()]
        
        # Resolve best name
        for ing in ingredients:
            ing["display_name"] = ing["inventory_name"] if ing["inventory_name"] else ing["item_name_text"]
            
        recipe["ingredients"] = ingredients
        return recipe
    finally:
        con.close()
