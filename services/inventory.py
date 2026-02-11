import sqlite3
import logging
from typing import List, Optional, Dict, Any
from pathlib import Path
from services import memory

DB_PATH = memory.get_db_path()

logger = logging.getLogger(__name__)

def _get_conn() -> sqlite3.Connection:
    return memory.get_conn()

# --- Storage Areas ---

def create_area(name: str, sort_order: int = 0) -> str:
    """Create a new storage area."""
    con = _get_conn()
    try:
        con.execute(
            "INSERT INTO storage_areas (name, sort_order) VALUES (?, ?)",
            (name, sort_order)
        )
        con.commit()
        return f"Area '{name}' created."
    except sqlite3.IntegrityError:
        return f"Area '{name}' already exists."
    finally:
        con.close()

def get_areas() -> List[dict]:
    """Get all storage areas sorted."""
    con = _get_conn()
    try:
        cur = con.execute("SELECT * FROM storage_areas ORDER BY sort_order, name")
        return [dict(row) for row in cur.fetchall()]
    finally:
        con.close()

def update_area_order(updates: List[tuple]) -> str:
    """Update sort order for areas. list of (id, sort_order)."""
    con = _get_conn()
    try:
        con.executemany("UPDATE storage_areas SET sort_order = ? WHERE id = ?", [(o, i) for i, o in updates])
        con.commit()
        return "Area order updated."
    finally:
        con.close()

# --- Inventory Items ---

def create_item(data: Dict[str, Any]) -> str:
    """Create or update an inventory item."""
    con = _get_conn()
    try:
        # Check if exists to update
        cur = con.execute("SELECT id FROM inventory_items WHERE name = ?", (data["name"],))
        row = cur.fetchone()
        
        keys = ["name", "quantity", "unit", "cost", "storage_area_id", "category", "sort_order"]
        filtered_data = {k: data.get(k) for k in keys if k in data}
        
        if row:
            # Update
            set_clause = ", ".join([f"{k} = ?" for k in filtered_data])
            values = tuple(filtered_data.values()) + (data["name"],)
            con.execute(f"UPDATE inventory_items SET {set_clause}, updated_at=CURRENT_TIMESTAMP WHERE name = ?", values)
            msg = f"Item '{data['name']}' updated."
        else:
            # Insert
            columns = ", ".join(filtered_data.keys())
            placeholders = ", ".join(["?" for _ in filtered_data])
            values = tuple(filtered_data.values())
            con.execute(f"INSERT INTO inventory_items ({columns}) VALUES ({placeholders})", values)
            msg = f"Item '{data['name']}' created."

        con.commit()
        return msg
    except Exception as e:
        logger.error(f"Error saving item: {e}")
        return f"Error: {str(e)}"
    finally:
        con.close()

def get_sheet_data() -> Dict[str, List[dict]]:
    """Get inventory items grouped by storage area."""
    con = _get_conn()
    try:
        # Get Areas
        areas = get_areas()
        area_map = {a['id']: a['name'] for a in areas}
        area_map[None] = "Unassigned"
        
        # Get Items
        cur = con.execute("SELECT * FROM inventory_items ORDER BY sort_order, name")
        items = [dict(row) for row in cur.fetchall()]
        
        # Group
        grouped = {name: [] for name in area_map.values()}
        # Ensure correct order of keys based on area sort
        ordered_keys = [a['name'] for a in areas] + ["Unassigned"]
        result = {k: [] for k in ordered_keys}
        
        for item in items:
            area_id = item.get("storage_area_id")
            area_name = area_map.get(area_id, "Unassigned")
            result[area_name].append(item)
            
        # Remove empty areas if you want, or keep them to show structure
        return result
    finally:
        con.close()

def submit_count(counts: List[Dict[str, Any]], user: str = "Unknown") -> str:
    """
    Submit a batch of inventory counts.
    counts: List of dicts with {item_id, quantity}
    """
    con = _get_conn()
    try:
        updated_count = 0
        for count in counts:
            item_id = count.get("item_id")
            new_qty = count.get("quantity")
            
            if item_id is None or new_qty is None:
                continue
                
            # Get current state for history
            cur = con.execute("SELECT name, quantity FROM inventory_items WHERE id = ?", (item_id,))
            row = cur.fetchone()
            if not row:
                continue
                
            item_name = row["name"]
            prev_qty = row["quantity"]
            variance = new_qty - prev_qty # Simple diff for now. Real variance needs pars/sales.
            
            # Update Item
            con.execute(
                "UPDATE inventory_items SET quantity = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (new_qty, item_id)
            )
            
            # Insert History
            con.execute(
                """
                INSERT INTO inventory_counts (item_name, quantity, previous_quantity, variance, counted_by)
                VALUES (?, ?, ?, ?, ?)
                """,
                (item_name, new_qty, prev_qty, variance, user)
            )
            updated_count += 1
            
        con.commit()
        return f"Successfully updated {updated_count} items."
    except Exception as e:
        logger.error(f"Error submitting counts: {e}")
        return f"Error: {str(e)}"
    finally:
        con.close()

def get_inventory_value() -> float:
    """Calculate total value of inventory on hand."""
    con = _get_conn()
    try:
        cur = con.execute("SELECT SUM(quantity * cost) as total_value FROM inventory_items")
        row = cur.fetchone()
        return row["total_value"] or 0.0
    finally:
        con.close()
