import sqlite3
import logging
from typing import List, Optional, Tuple, Dict
from services import memory

logger = logging.getLogger(__name__)

def _get_conn() -> sqlite3.Connection:
    return memory.get_conn()

# --- INVENTORY OPERATIONS ---

def get_inventory_status(item_name: Optional[str] = None) -> List[dict]:
    """Get inventory status for a specific item or all items."""
    con = _get_conn()
    try:
        if item_name:
            cur = con.execute(
                "SELECT * FROM inventory_items WHERE name LIKE ?", 
                (f"%{item_name}%",)
            )
        else:
            cur = con.execute("SELECT * FROM inventory_items ORDER BY name")
        
        rows = cur.fetchall()
        return [dict(row) for row in rows]
    finally:
        con.close()

def update_inventory(item_name: str, quantity: float, unit: str = 'unit', cost: float = 0.0) -> str:
    """Update or create an inventory item."""
    con = _get_conn()
    try:
        # Check if item exists to preserve other fields if needed, or just upsert
        cur = con.execute("SELECT id FROM inventory_items WHERE name = ?", (item_name,))
        row = cur.fetchone()
        
        if row:
            con.execute(
                """
                UPDATE inventory_items 
                SET quantity = ?, unit = ?, updated_at = CURRENT_TIMESTAMP 
                WHERE name = ?
                """,
                (quantity, unit, item_name)
            )
            action = "updated"
        else:
            con.execute(
                """
                INSERT INTO inventory_items (name, quantity, unit, cost) 
                VALUES (?, ?, ?, ?)
                """,
                (item_name, quantity, unit, cost)
            )
            action = "created"
        
        con.commit()
        return f"Inventory {action}: {item_name} set to {quantity} {unit}."
    finally:
        con.close()

def set_item_unavailable(item_name: str) -> str:
    """Mark an item as out of stock (86'd)."""
    con = _get_conn()
    try:
        con.execute(
            "UPDATE inventory_items SET quantity = 0, updated_at = CURRENT_TIMESTAMP WHERE name = ?",
            (item_name,)
        )
        con.commit()
        # In a real app, this might trigger notifications
        return f"86'd: {item_name} is now marked as OUT OF STOCK."
    finally:
        con.close()

# --- WASTE OPERATIONS ---

def log_waste(item_name: str, quantity: float, reason: str, user: str) -> str:
    """Log waste for an item."""
    con = _get_conn()
    try:
        con.execute(
            """
            INSERT INTO waste_logs (item_name, quantity_lost, reason, logged_by)
            VALUES (?, ?, ?, ?)
            """,
            (item_name, quantity, reason, user)
        )
        # Optionally deduct from inventory if it exists
        con.execute(
            "UPDATE inventory_items SET quantity = MAX(0, quantity - ?) WHERE name = ?",
            (quantity, item_name)
        )
        con.commit()
        return f"Waste recorded: {quantity} of {item_name} ({reason}). Inv adjusted."
    finally:
        con.close()

# --- PREP OPERATIONS ---

def get_prep_list(status: str = 'todo') -> List[dict]:
    """Get prep tasks filtered by status."""
    con = _get_conn()
    try:
        cur = con.execute(
            "SELECT * FROM prep_tasks WHERE status = ? ORDER BY created_at",
            (status,)
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        con.close()

def add_prep_task(task: str, assigned_to: Optional[str] = None) -> str:
    """Add a new task to the prep list."""
    con = _get_conn()
    try:
        con.execute(
            "INSERT INTO prep_tasks (task, assigned_to, status) VALUES (?, ?, 'todo')",
            (task, assigned_to)
        )
        con.commit()
        return f"Prep added: {task}"
    finally:
        con.close()

def complete_prep_task(task_id: int) -> str:
    """Mark a prep task as done."""
    con = _get_conn()
    try:
        con.execute("UPDATE prep_tasks SET status = 'done' WHERE id = ?", (task_id,))
        con.commit()
        return "Task marked as complete."
    finally:
        con.close()
        
def clear_completed_prep() -> str:
    """Clear done tasks (e.g., for start of day)."""
    con = _get_conn()
    try:
        con.execute("DELETE FROM prep_tasks WHERE status = 'done'")
        con.commit()
        return "Completed prep tasks cleared."
    finally:
        con.close()

# --- SHOPPING / ORDERING ---

def add_to_order_guide(item_name: str, quantity: float, unit: str, user: str) -> str:
    """Add an item to the shopping list."""
    con = _get_conn()
    try:
        con.execute(
            """
            INSERT INTO shopping_list (item_name, quantity, unit, added_by)
            VALUES (?, ?, ?, ?)
            """,
            (item_name, quantity, unit, user)
        )
        con.commit()
        return f"Added to order guide: {quantity} {unit} {item_name}."
    finally:
        con.close()

def get_order_guide() -> List[dict]:
    """Get current shopping list."""
    con = _get_conn()
    try:
        cur = con.execute("SELECT * FROM shopping_list ORDER BY item_name")
        return [dict(row) for row in cur.fetchall()]
    finally:
        con.close()

def clear_order_guide() -> str:
    """Clear the shopping list (after ordering)."""
    con = _get_conn()
    try:
        con.execute("DELETE FROM shopping_list")
        con.commit()
        return "Order guide cleared."
    finally:
        con.close()

# --- COSTING (MOCK) ---

def get_item_cost(item_name: str) -> str:
    """Get cost info (mock logic for now)."""
    con = _get_conn()
    try:
        cur = con.execute("SELECT cost, unit FROM inventory_items WHERE name = ?", (item_name,))
        row = cur.fetchone()
        if row and row['cost'] > 0:
            return f"{item_name}: ${row['cost']:.2f} per {row['unit']}"
        else:
            return f"No cost data found for {item_name}."
    finally:
        con.close()
