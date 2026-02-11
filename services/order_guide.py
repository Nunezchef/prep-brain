import sqlite3
import logging
import csv
import io
from typing import List, Optional, Dict, Any
from services import memory

logger = logging.getLogger(__name__)

def _get_conn() -> sqlite3.Connection:
    return memory.get_conn()

def add_item(data: Dict[str, Any]) -> str:
    """Add a new item to a vendor's order guide."""
    con = _get_conn()
    try:
        keys = ["vendor_id", "item_name", "pack_size", "price", "par_level", "category", "notes"]
        filtered_data = {k: data.get(k) for k in keys if k in data}

        if "vendor_id" not in filtered_data or "item_name" not in filtered_data:
            return "Error: Vendor ID and Item Name are required."

        columns = ", ".join(filtered_data.keys())
        placeholders = ", ".join(["?" for _ in filtered_data])
        values = tuple(filtered_data.values())

        con.execute(
            f"INSERT INTO order_guide_items ({columns}) VALUES ({placeholders})",
            values
        )
        con.commit()
        return f"Item '{filtered_data['item_name']}' added successfully."
    except sqlite3.IntegrityError:
        return f"Error: Item '{data.get('item_name')}' already exists for this vendor."
    except Exception as e:
        logger.error(f"Error adding item: {e}")
        return f"Error adding item: {str(e)}"
    finally:
        con.close()

def update_item(item_id: int, data: Dict[str, Any]) -> str:
    """Update an order guide item."""
    con = _get_conn()
    try:
        keys = ["item_name", "pack_size", "price", "par_level", "category", "notes", "is_active"]
        filtered_data = {k: data.get(k) for k in keys if k in data}

        if not filtered_data:
            return "No valid fields to update."

        set_clause = ", ".join([f"{k} = ?" for k in filtered_data])
        values = tuple(filtered_data.values()) + (item_id,)

        con.execute(
            f"UPDATE order_guide_items SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            values
        )
        con.commit()
        return "Item updated successfully."
    except Exception as e:
        logger.error(f"Error updating item: {e}")
        return f"Error updating item: {str(e)}"
    finally:
        con.close()

def delete_item(item_id: int) -> str:
    """Delete an order guide item."""
    con = _get_conn()
    try:
        con.execute("DELETE FROM order_guide_items WHERE id = ?", (item_id,))
        con.commit()
        return "Item deleted successfully."
    except Exception as e:
        logger.error(f"Error deleting item: {e}")
        return f"Error deleting item: {str(e)}"
    finally:
        con.close()

def get_items_by_vendor(vendor_id: int) -> List[dict]:
    """Get all items for a specific vendor."""
    con = _get_conn()
    try:
        cur = con.execute("SELECT * FROM order_guide_items WHERE vendor_id = ? ORDER BY category, item_name", (vendor_id,))
        return [dict(row) for row in cur.fetchall()]
    finally:
        con.close()

def export_guide_csv(vendor_id: int) -> str:
    """Export vendor guide to CSV string."""
    items = get_items_by_vendor(vendor_id)
    if not items:
        return ""

    output = io.StringIO()
    writer = csv.writer(output)
    
    # Header
    writer.writerow(["Item Name", "Pack Size", "Price", "Par Level", "Category", "Notes", "Active"])
    
    for item in items:
        writer.writerow([
            item["item_name"],
            item["pack_size"],
            item["price"],
            item["par_level"],
            item["category"],
            item["notes"],
            "Yes" if item["is_active"] else "No"
        ])
        
    return output.getvalue()
