import sqlite3
import logging
from typing import List, Dict, Any
from services import memory

logger = logging.getLogger(__name__)

def _get_conn() -> sqlite3.Connection:
    return memory.get_conn()

def log_waste(data: Dict[str, Any]) -> str:
    """Log a waste entry."""
    con = _get_conn()
    try:
        con.execute(
            """
            INSERT INTO waste_tracking 
            (item_name, quantity, unit, reason, category, dollar_value, logged_by)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data.get("item_name"),
                data.get("quantity"),
                data.get("unit"),
                data.get("reason"),
                data.get("category"),
                data.get("dollar_value", 0.0),
                data.get("logged_by")
            )
        )
        con.commit()
        return f"Waste logged: {data.get('item_name')}"
    except Exception as e:
        logger.error(f"Error logging waste: {e}")
        return f"Error: {str(e)}"
    finally:
        con.close()

def get_waste_history(limit: int = 50) -> List[dict]:
    """Get recent waste entries."""
    con = _get_conn()
    try:
        cur = con.execute(
            "SELECT * FROM waste_tracking ORDER BY logged_at DESC LIMIT ?",
            (limit,)
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        con.close()

def get_waste_summary() -> Dict[str, Any]:
    """Get total waste value by reason."""
    con = _get_conn()
    try:
        cur = con.execute(
            """
            SELECT reason, SUM(dollar_value) as total_value, COUNT(*) as count
            FROM waste_tracking
            GROUP BY reason
            ORDER BY total_value DESC
            """
        )
        by_reason = [dict(row) for row in cur.fetchall()]
        
        cur2 = con.execute("SELECT SUM(dollar_value) as grand_total FROM waste_tracking")
        grand_total = cur2.fetchone()["grand_total"] or 0.0
        
        return {
            "by_reason": by_reason,
            "grand_total": grand_total
        }
    finally:
        con.close()
