import sqlite3
import logging
from typing import List, Dict, Any, Optional
from services import memory

logger = logging.getLogger(__name__)

def _get_conn() -> sqlite3.Connection:
    return memory.get_conn()

def create_service_note(data: Dict[str, Any]) -> str:
    """Create a service note / post-mortem."""
    con = _get_conn()
    try:
        con.execute(
            """
            INSERT INTO service_notes 
            (service_date, shift, covers, weather, notes, highlights, issues, action_items, logged_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data.get("service_date"),
                data.get("shift"),
                data.get("covers", 0),
                data.get("weather"),
                data.get("notes"),
                data.get("highlights"),
                data.get("issues"),
                data.get("action_items"),
                data.get("logged_by")
            )
        )
        con.commit()
        return "Service note saved."
    except Exception as e:
        logger.error(f"Error saving service note: {e}")
        return f"Error: {str(e)}"
    finally:
        con.close()

def get_service_notes(limit: int = 30) -> List[dict]:
    """Get recent service notes."""
    con = _get_conn()
    try:
        cur = con.execute(
            "SELECT * FROM service_notes ORDER BY service_date DESC, shift DESC LIMIT ?",
            (limit,)
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        con.close()

def get_note_by_id(note_id: int) -> Optional[dict]:
    """Get a single service note."""
    con = _get_conn()
    try:
        cur = con.execute("SELECT * FROM service_notes WHERE id = ?", (note_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        con.close()
