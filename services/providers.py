import sqlite3
import logging
from typing import List, Optional, Dict, Any
from services import memory

logger = logging.getLogger(__name__)

def _get_conn() -> sqlite3.Connection:
    return memory.get_conn()

def create_vendor(data: Dict[str, Any]) -> str:
    """Create a new vendor."""
    con = _get_conn()
    try:
        keys = ["name", "contact_name", "email", "phone", "website", "ordering_method", "cutoff_time", "lead_time_days", "notes"]
        # Filter data to only include valid keys
        filtered_data = {k: data.get(k) for k in keys if k in data}
        
        # Ensure name is present
        if "name" not in filtered_data or not filtered_data["name"]:
            return "Error: Vendor name is required."

        columns = ", ".join(filtered_data.keys())
        placeholders = ", ".join(["?" for _ in filtered_data])
        values = tuple(filtered_data.values())

        con.execute(
            f"INSERT INTO vendors ({columns}) VALUES ({placeholders})",
            values
        )
        con.commit()
        return f"Vendor '{filtered_data['name']}' created successfully."
    except sqlite3.IntegrityError:
        return f"Error: Vendor '{data.get('name')}' already exists."
    except Exception as e:
        logger.error(f"Error creating vendor: {e}")
        return f"Error creating vendor: {str(e)}"
    finally:
        con.close()

def update_vendor(vendor_id: int, data: Dict[str, Any]) -> str:
    """Update an existing vendor."""
    con = _get_conn()
    try:
        keys = ["name", "contact_name", "email", "phone", "website", "ordering_method", "cutoff_time", "lead_time_days", "notes"]
        filtered_data = {k: data.get(k) for k in keys if k in data}

        if not filtered_data:
            return "No valid fields to update."

        set_clause = ", ".join([f"{k} = ?" for k in filtered_data])
        values = tuple(filtered_data.values()) + (vendor_id,)

        con.execute(
            f"UPDATE vendors SET {set_clause} WHERE id = ?",
            values
        )
        con.commit()
        return "Vendor updated successfully."
    except Exception as e:
        logger.error(f"Error updating vendor: {e}")
        return f"Error updating vendor: {str(e)}"
    finally:
        con.close()

def get_all_vendors() -> List[dict]:
    """Get all vendors."""
    con = _get_conn()
    try:
        cur = con.execute("SELECT * FROM vendors ORDER BY name")
        return [dict(row) for row in cur.fetchall()]
    finally:
        con.close()

def get_vendor(vendor_id: int) -> Optional[dict]:
    """Get a single vendor by ID."""
    con = _get_conn()
    try:
        cur = con.execute("SELECT * FROM vendors WHERE id = ?", (vendor_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        con.close()

def delete_vendor(vendor_id: int) -> str:
    """Delete a vendor."""
    con = _get_conn()
    try:
        con.execute("DELETE FROM vendors WHERE id = ?", (vendor_id,))
        con.commit()
        return "Vendor deleted successfully."
    except Exception as e:
        logger.error(f"Error deleting vendor: {e}")
        return f"Error deleting vendor: {str(e)}"
    finally:
        con.close()
