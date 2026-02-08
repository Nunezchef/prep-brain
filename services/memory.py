import sqlite3
import yaml
from pathlib import Path
from typing import List, Tuple

def load_config():
    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)

CONFIG = load_config()
DB_PATH = Path(CONFIG["memory"]["db_path"])

def _conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    return con

def init_db() -> None:
    con = _conn()
    cur = con.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        telegram_user_id INTEGER UNIQUE,
        display_name TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS sessions (
        id INTEGER PRIMARY KEY,
        telegram_chat_id INTEGER,
        telegram_user_id INTEGER,
        title TEXT,
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY,
        session_id INTEGER,
        role TEXT CHECK(role IN ('user','assistant','system')) NOT NULL,
        content TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
    );
    """)

    con.commit()
    con.close()

def get_or_create_user(telegram_user_id: int, display_name: str) -> None:
    con = _conn()
    con.execute(
        "INSERT OR IGNORE INTO users (telegram_user_id, display_name) VALUES (?, ?)",
        (telegram_user_id, display_name),
    )
    con.commit()
    con.close()

def get_or_create_active_session(chat_id: int, user_id: int) -> int:
    con = _conn()
    cur = con.cursor()

    cur.execute(
        "SELECT id FROM sessions WHERE telegram_chat_id=? AND telegram_user_id=? AND is_active=1 ORDER BY id DESC LIMIT 1",
        (chat_id, user_id),
    )
    row = cur.fetchone()
    if row:
        con.close()
        return int(row[0])

    cur.execute(
        "INSERT INTO sessions (telegram_chat_id, telegram_user_id, title, is_active) VALUES (?, ?, ?, 1)",
        (chat_id, user_id, "default"),
    )
    con.commit()
    session_id = int(cur.lastrowid)
    con.close()
    return session_id

def add_message(session_id: int, role: str, content: str) -> None:
    con = _conn()
    con.execute(
        "INSERT INTO messages (session_id, role, content) VALUES (?, ?, ?)",
        (session_id, role, content),
    )
    con.commit()
    con.close()

def get_recent_messages(session_id: int, limit: int = 16) -> List[Tuple[str, str]]:
    con = _conn()
    cur = con.cursor()
    cur.execute(
        "SELECT role, content FROM messages WHERE session_id=? ORDER BY id DESC LIMIT ?",
        (session_id, limit),
    )
    rows = cur.fetchall()
    con.close()
    rows.reverse()
    return [(r[0], r[1]) for r in rows]