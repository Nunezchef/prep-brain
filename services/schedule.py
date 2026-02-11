import sqlite3
import logging
from typing import List, Optional, Dict, Any
from services import memory, prep

logger = logging.getLogger(__name__)

def _get_conn() -> sqlite3.Connection:
    return memory.get_conn()

def create_staff(name: str, role: str) -> str:
    """Create a new staff member."""
    con = _get_conn()
    try:
        con.execute(
            "INSERT INTO staff (name, role) VALUES (?, ?)",
            (name, role)
        )
        con.commit()
        return f"Staff '{name}' created."
    except Exception as e:
        logger.error(f"Error creating staff: {e}")
        return f"Error: {str(e)}"
    finally:
        con.close()

def get_active_staff() -> List[dict]:
    """Get all active staff."""
    con = _get_conn()
    try:
        cur = con.execute("SELECT * FROM staff WHERE is_active=1 ORDER BY name")
        return [dict(row) for row in cur.fetchall()]
    finally:
        con.close()

def assign_prep_item(item_id: int, staff_id: int, date: str, shift: str) -> str:
    """Assign a prep item to a staff member."""
    con = _get_conn()
    try:
        # Check if already assigned? 
        # For simplicity, we allow re-assignment by just inserting. 
        # But maybe we should update if it exists for this item?
        # Let's clean up old assignment for this item if any.
        con.execute("DELETE FROM production_assignments WHERE prep_list_item_id = ?", (item_id,))
        
        con.execute(
            """
            INSERT INTO production_assignments (prep_list_item_id, staff_id, assigned_date, shift)
            VALUES (?, ?, ?, ?)
            """,
            (item_id, staff_id, date, shift)
        )
        con.commit()
        return "Task assigned."
    except Exception as e:
        logger.error(f"Error assigning task: {e}")
        return f"Error: {str(e)}"
    finally:
        con.close()

def get_schedule(date: str) -> List[dict]:
    """Get the schedule for a specific date."""
    con = _get_conn()
    try:
        cur = con.execute(
            """
            SELECT pa.*, s.name as staff_name, pli.recipe_id, pli.need_quantity, pli.unit, pli.status, r.name as recipe_name
            FROM production_assignments pa
            JOIN staff s ON pa.staff_id = s.id
            JOIN prep_list_items pli ON pa.prep_list_item_id = pli.id
            JOIN recipes r ON pli.recipe_id = r.id
            WHERE pa.assigned_date = ?
            ORDER BY s.name, pa.shift
            """,
            (date,)
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        con.close()

def unassign_item(assignment_id: int) -> str:
    """Remove an assignment."""
    con = _get_conn()
    try:
        con.execute("DELETE FROM production_assignments WHERE id = ?", (assignment_id,))
        con.commit()
        return "Assignment removed."
    finally:
        con.close()
