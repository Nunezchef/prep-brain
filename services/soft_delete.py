from datetime import datetime
from typing import Optional, Dict, Any, List
import sqlite3
import logging
from services import memory

logger = logging.getLogger(__name__)

def _get_conn() -> sqlite3.Connection:
    return memory.get_conn()

def soft_delete(table: str, id: int, actor: str) -> bool:
    """
    Mark a record as deleted by setting deleted_at to current timestamp.
    Returns True if successful (row existed), False otherwise.
    """
    con = _get_conn()
    try:
        # Verify table allowed to prevent injection (though table name should be controlled code side)
        allowed_tables = {
            "recipes", "inventory_items", "menu_items", "vendors", "vendor_items", "prep_list_items"
        }
        if table not in allowed_tables:
            raise ValueError(f"Table {table} not configured for soft delete.")

        cur = con.execute(
            f"UPDATE {table} SET deleted_at = CURRENT_TIMESTAMP WHERE id = ? AND deleted_at IS NULL",
            (id,)
        )
        con.commit()
        
        if cur.rowcount > 0:
            logger.info(f"Soft deleted {table} id={id} by {actor}")
            return True
        return False
    except Exception as e:
        logger.error(f"Error soft deleting from {table} id={id}: {e}")
        return False
    finally:
        con.close()

def restore(table: str, id: int, actor: str) -> bool:
    """
    Restore a soft-deleted record by setting deleted_at to NULL.
    """
    con = _get_conn()
    try:
        allowed_tables = {
            "recipes", "inventory_items", "menu_items", "vendors", "vendor_items", "prep_list_items"
        }
        if table not in allowed_tables:
            raise ValueError(f"Table {table} not configured for restoration.")
            
        cur = con.execute(
            f"UPDATE {table} SET deleted_at = NULL WHERE id = ?",
            (id,)
        )
        con.commit()
        
        if cur.rowcount > 0:
            logger.info(f"Restored {table} id={id} by {actor}")
            return True
        return False
    except Exception as e:
        logger.error(f"Error restoring {table} id={id}: {e}")
        return False
    finally:
        con.close()

def get_active_where_clause(alias: Optional[str] = None) -> str:
    """
    Return SQL fragment for filtering active records.
    """
    if alias:
        return f"{alias}.deleted_at IS NULL"
    return "deleted_at IS NULL"
