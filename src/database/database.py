"""SQLite connection and schema bootstrap."""
import sqlite3
from pathlib import Path

from config import config


def connect(db_path=None) -> sqlite3.Connection:
    db_path = Path(db_path or config.DB_PATH)
    if str(db_path) != ":memory:":
        db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    """Idempotent: every statement is CREATE ... IF NOT EXISTS."""
    conn.executescript((config.SQL_DIR / "create_tables.sql").read_text(encoding="utf-8"))
    conn.execute("PRAGMA foreign_keys = ON")  # executescript commits; re-assert for safety
    conn.commit()
