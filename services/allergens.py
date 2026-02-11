import sqlite3
import logging
from typing import List, Dict, Any
from services import memory

logger = logging.getLogger(__name__)

def _get_conn() -> sqlite3.Connection:
    return memory.get_conn()

def get_all_allergens() -> List[dict]:
    """Get all allergens."""
    con = _get_conn()
    try:
        cur = con.execute("SELECT * FROM allergens ORDER BY name")
        return [dict(row) for row in cur.fetchall()]
    finally:
        con.close()

def set_recipe_allergens(recipe_id: int, allergen_ids: List[int]) -> str:
    """Set allergens for a recipe (replace existing)."""
    con = _get_conn()
    try:
        con.execute("DELETE FROM recipe_allergens WHERE recipe_id = ?", (recipe_id,))
        for a_id in allergen_ids:
            con.execute(
                "INSERT OR IGNORE INTO recipe_allergens (recipe_id, allergen_id) VALUES (?, ?)",
                (recipe_id, a_id)
            )
        con.commit()
        return f"Allergens updated for recipe {recipe_id}."
    except Exception as e:
        logger.error(f"Error setting allergens: {e}")
        return f"Error: {str(e)}"
    finally:
        con.close()

def get_recipe_allergens(recipe_id: int) -> List[dict]:
    """Get allergens for a recipe."""
    con = _get_conn()
    try:
        cur = con.execute(
            """
            SELECT a.* FROM allergens a
            JOIN recipe_allergens ra ON a.id = ra.allergen_id
            WHERE ra.recipe_id = ?
            ORDER BY a.name
            """,
            (recipe_id,)
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        con.close()

def get_allergen_matrix() -> Dict[str, Any]:
    """
    Generate a full allergen matrix across all active recipes.
    Returns: {
        "allergens": [str],  (column headers)
        "recipes": [
            {"name": str, "allergens": {allergen_name: bool}}
        ]
    }
    """
    con = _get_conn()
    try:
        # Get all allergens and recipes
        allergens = [dict(r) for r in con.execute("SELECT * FROM allergens ORDER BY name").fetchall()]
        recipes_rows = [dict(r) for r in con.execute("SELECT * FROM recipes WHERE is_active=1 ORDER BY name").fetchall()]
        
        # Get all links
        links = [dict(r) for r in con.execute(
            """
            SELECT ra.recipe_id, a.name as allergen_name
            FROM recipe_allergens ra
            JOIN allergens a ON ra.allergen_id = a.id
            """
        ).fetchall()]
        
        # Build lookup
        link_map = {}
        for l in links:
            link_map.setdefault(l["recipe_id"], set()).add(l["allergen_name"])
        
        # Build Matrix Data
        allergen_names = [a["name"] for a in allergens]
        matrix = []
        for r in recipes_rows:
            row_allergens = link_map.get(r["id"], set())
            matrix.append({
                "name": r["name"],
                "station": r["station"],
                "allergens": {name: (name in row_allergens) for name in allergen_names}
            })
        
        return {
            "allergens": allergen_names,
            "recipes": matrix
        }
    finally:
        con.close()
