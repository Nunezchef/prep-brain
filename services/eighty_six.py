import sqlite3
import logging
from typing import List, Dict, Any, Optional
from services import memory

logger = logging.getLogger(__name__)

def _get_conn() -> sqlite3.Connection:
    return memory.get_conn()

# ── 86 Board ──
def eighty_six_item(item_name: str, reason: str = "", substitution: str = "", reported_by: str = "") -> str:
    """Add an item to the 86 board."""
    con = _get_conn()
    try:
        con.execute(
            """
            INSERT INTO eighty_six_board (item_name, reason, substitution, reported_by)
            VALUES (?, ?, ?, ?)
            """,
            (item_name, reason, substitution, reported_by)
        )
        con.commit()
        return f"86'd: {item_name}"
    except Exception as e:
        logger.error(f"Error 86-ing item: {e}")
        return f"Error: {str(e)}"
    finally:
        con.close()

def get_active_86() -> List[dict]:
    """Get all currently 86'd items."""
    con = _get_conn()
    try:
        cur = con.execute(
            "SELECT * FROM eighty_six_board WHERE is_active = 1 ORDER BY created_at DESC"
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        con.close()

def resolve_86(item_id: int) -> str:
    """Remove an item from the 86 board."""
    con = _get_conn()
    try:
        con.execute(
            "UPDATE eighty_six_board SET is_active = 0, resolved_at = CURRENT_TIMESTAMP WHERE id = ?",
            (item_id,)
        )
        con.commit()
        return "Item restored."
    finally:
        con.close()

def get_substitution(item_name: str) -> Optional[str]:
    """Get substitution suggestion for an 86'd item."""
    con = _get_conn()
    try:
        cur = con.execute(
            "SELECT substitution FROM eighty_six_board WHERE item_name = ? AND is_active = 1",
            (item_name,)
        )
        row = cur.fetchone()
        return row["substitution"] if row and row["substitution"] else None
    finally:
        con.close()
