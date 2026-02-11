from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional

from prep_brain.config import get_db_path as _resolved_db_path

DB_PATH: Path = _resolved_db_path()


def get_db_path() -> Path:
    return DB_PATH


def set_db_path(path: Path) -> None:
    global DB_PATH
    DB_PATH = Path(path)


def get_conn(path: Optional[Path] = None) -> sqlite3.Connection:
    db_path = Path(path) if path else DB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    return con
