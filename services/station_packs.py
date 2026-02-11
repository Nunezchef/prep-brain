import sqlite3
import logging
from typing import List, Optional, Dict, Any
from services import memory, recipes, costing

logger = logging.getLogger(__name__)

def _get_conn() -> sqlite3.Connection:
    return memory.get_conn()

def get_stations() -> List[str]:
    """Get distinct stations from active recipes."""
    con = _get_conn()
    try:
        cur = con.execute("SELECT DISTINCT station FROM recipes WHERE is_active=1 AND station IS NOT NULL ORDER BY station")
        return [row["station"] for row in cur.fetchall()]
    finally:
        con.close()

def get_station_pack(station: str) -> Dict[str, Any]:
    """
    Generate a Station Pack: all recipes, costs, and notes for a given station.
    Returns: {
        "station": str,
        "recipes": [
            {
                "name": str,
                "yield_amount": float,
                "yield_unit": str,
                "method": str,
                "ingredients": [...],
                "total_cost": float,
                "cost_per_yield": float
            }
        ]
    }
    """
    con = _get_conn()
    try:
        cur = con.execute(
            "SELECT * FROM recipes WHERE station = ? AND is_active = 1 ORDER BY name",
            (station,)
        )
        station_recipes = [dict(row) for row in cur.fetchall()]
        
        results = []
        for r in station_recipes:
            details = recipes.get_recipe_details(r["id"])
            cost_data = costing.calculate_recipe_cost(r["id"])
            
            results.append({
                "name": details["name"],
                "yield_amount": details["yield_amount"],
                "yield_unit": details["yield_unit"],
                "method": details["method"],
                "ingredients": details["ingredients"],
                "total_cost": cost_data["total_cost"],
                "cost_per_yield": cost_data["cost_per_yield"]
            })
        
        return {
            "station": station,
            "recipes": results
        }
    finally:
        con.close()

def generate_training_card_md(recipe_id: int) -> str:
    """Generate a Markdown training card for a single recipe."""
    details = recipes.get_recipe_details(recipe_id)
    cost_data = costing.calculate_recipe_cost(recipe_id)
    
    if not details:
        return "Recipe not found."
    
    md = f"# {details['name']}\n"
    md += f"**Station:** {details['station'] or 'N/A'} | "
    md += f"**Yield:** {details['yield_amount']} {details['yield_unit']} | "
    md += f"**Cost:** ${cost_data['total_cost']:,.2f}\n\n"
    
    md += "## Ingredients\n"
    md += "| Item | Qty | Unit | Cost |\n"
    md += "|------|-----|------|------|\n"
    for ing in details["ingredients"]:
        cost_str = f"${ing['cost']:,.2f}" if ing.get('cost') else "-"
        md += f"| {ing['display_name']} | {ing['quantity']} | {ing['unit']} | {cost_str} |\n"
    
    md += f"\n## Method\n{details['method'] or 'No method specified.'}\n"
    
    return md
