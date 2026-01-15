import sqlite3
import os
from typing import Iterable, Tuple, Any, Optional

def ensure_db(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.close()

def connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def exec_many(conn: sqlite3.Connection, sql: str, rows: Iterable[Tuple[Any, ...]]) -> None:
    conn.executemany(sql, rows)
    conn.commit()

def exec_one(conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> None:
    conn.execute(sql, params)
    conn.commit()

def fetch_one(conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> Optional[sqlite3.Row]:
    cur = conn.execute(sql, params)
    row = cur.fetchone()
    return row
