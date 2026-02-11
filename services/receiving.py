import sqlite3
import logging
from typing import List, Dict, Any
from services import memory

logger = logging.getLogger(__name__)

def _get_conn() -> sqlite3.Connection:
    return memory.get_conn()

def log_receiving(data: Dict[str, Any]) -> str:
    """Log a received item."""
    con = _get_conn()
    try:
        con.execute(
            """
            INSERT INTO receiving_log 
            (vendor_id, invoice_number, item_name, quantity_received, unit, unit_cost, total_cost, temperature_check, quality_ok, notes, received_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data.get("vendor_id"),
                data.get("invoice_number"),
                data.get("item_name"),
                data.get("quantity_received"),
                data.get("unit"),
                data.get("unit_cost"),
                data.get("total_cost"),
                data.get("temperature_check"),
                data.get("quality_ok", 1),
                data.get("notes"),
                data.get("received_by")
            )
        )
        con.commit()
        return f"Received: {data.get('item_name')}"
    except Exception as e:
        logger.error(f"Error logging receiving: {e}")
        return f"Error: {str(e)}"
    finally:
        con.close()

def get_receiving_history(limit: int = 50) -> List[dict]:
    """Get recent receiving entries."""
    con = _get_conn()
    try:
        cur = con.execute(
            """
            SELECT rl.*, v.name as vendor_name
            FROM receiving_log rl
            LEFT JOIN vendors v ON rl.vendor_id = v.id
            ORDER BY rl.received_at DESC
            LIMIT ?
            """,
            (limit,)
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        con.close()

def get_invoice_summary(invoice_number: str) -> Dict[str, Any]:
    """Get summary for a specific invoice."""
    con = _get_conn()
    try:
        cur = con.execute(
            "SELECT * FROM receiving_log WHERE invoice_number = ? ORDER BY item_name",
            (invoice_number,)
        )
        items = [dict(row) for row in cur.fetchall()]
        total = sum(i.get("total_cost", 0) or 0 for i in items)
        return {
            "invoice_number": invoice_number,
            "items": items,
            "total": total,
            "item_count": len(items)
        }
    finally:
        con.close()
