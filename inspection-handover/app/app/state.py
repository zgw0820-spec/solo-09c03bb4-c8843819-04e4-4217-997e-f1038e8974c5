"""交接状态持久化。

接单库状态保存在 SQLite（挂载到持久卷），应用重启后沿用已生效的接单库。
状态取值：
  OLD        旧库接单（初始）
  SWITCHING  交接进行中：旧库已暂停接单，等待追平
  NEW        新库接单（已生效）
"""
import json
import sqlite3
import threading
import time
from pathlib import Path

VALID_STATES = ("OLD", "SWITCHING", "NEW")


class StateStore:
    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS kv (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS events (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts         REAL NOT NULL,
                    phase      TEXT NOT NULL,
                    message    TEXT NOT NULL,
                    detail     TEXT
                );
                """
            )
            cur = self._conn.execute("SELECT value FROM kv WHERE key = 'state'")
            row = cur.fetchone()
            if row is None:
                self._conn.execute(
                    "INSERT INTO kv(key, value) VALUES ('state', 'OLD')"
                )
            self._conn.commit()

    def get_state(self) -> str:
        with self._lock:
            cur = self._conn.execute("SELECT value FROM kv WHERE key = 'state'")
            return cur.fetchone()["value"]

    def set_state(self, state: str) -> None:
        if state not in VALID_STATES:
            raise ValueError(f"invalid state {state}")
        with self._lock:
            self._conn.execute(
                "UPDATE kv SET value = ? WHERE key = 'state'", (state,)
            )
            self._conn.commit()

    def log_event(self, phase: str, message: str, detail: dict | None = None) -> dict:
        with self._lock:
            ts = time.time()
            self._conn.execute(
                "INSERT INTO events(ts, phase, message, detail) VALUES (?, ?, ?, ?)",
                (ts, phase, message, json.dumps(detail, ensure_ascii=False) if detail else None),
            )
            self._conn.commit()
            return {"ts": ts, "phase": phase, "message": message, "detail": detail or {}}

    def list_events(self, limit: int = 100) -> list[dict]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT ts, phase, message, detail FROM events "
                "ORDER BY id DESC LIMIT ?",
                (limit,),
            )
            return [
                {
                    "ts": r["ts"],
                    "phase": r["phase"],
                    "message": r["message"],
                    "detail": json.loads(r["detail"]) if r["detail"] else {},
                }
                for r in cur.fetchall()
            ]
