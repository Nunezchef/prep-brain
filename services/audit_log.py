import json
from typing import Any, Optional

from services import memory


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=True, sort_keys=True)
    return str(value)


def record_event(
    *,
    actor_telegram_user_id: int,
    actor_display_name: str,
    action_type: str,
    entity_type: str,
    entity_id: int,
    old_value: Any,
    new_value: Any,
    note: Optional[str] = None,
    con=None,
) -> int:
    owns_con = con is None
    db = con or memory.get_conn()
    try:
        cur = db.execute(
            """
            INSERT INTO audit_events (
                actor_telegram_user_id,
                actor_display_name,
                action_type,
                entity_type,
                entity_id,
                old_value,
                new_value,
                note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(actor_telegram_user_id or 0),
                str(actor_display_name or "").strip() or "unknown",
                str(action_type or "").strip(),
                str(entity_type or "").strip(),
                int(entity_id),
                _stringify(old_value),
                _stringify(new_value),
                str(note or "")[:1000],
            ),
        )
        if owns_con:
            db.commit()
        return int(cur.lastrowid)
    finally:
        if owns_con:
            db.close()
