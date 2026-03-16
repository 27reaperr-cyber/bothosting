"""
db.py — Database layer with SQLite + in-memory caching.
Minimizes disk I/O under high load.
"""

import sqlite3
import asyncio
import logging
import time
from pathlib import Path
from typing import Optional
from contextlib import contextmanager

logger = logging.getLogger("bothost.db")

DB_PATH = Path("data/bothost.db")

# ──────────────────────────────────────────────
# Simple in-memory cache
# ──────────────────────────────────────────────
_cache: dict[str, tuple[float, object]] = {}
CACHE_TTL = 30  # seconds


def _cache_get(key: str):
    entry = _cache.get(key)
    if entry and time.time() - entry[0] < CACHE_TTL:
        return entry[1]
    return None


def _cache_set(key: str, value):
    _cache[key] = (time.time(), value)


def _cache_del(key: str):
    _cache.pop(key, None)


def _cache_del_prefix(prefix: str):
    for k in list(_cache.keys()):
        if k.startswith(prefix):
            del _cache[k]


# ──────────────────────────────────────────────
# Connection
# ──────────────────────────────────────────────
@contextmanager
def get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ──────────────────────────────────────────────
# Schema
# ──────────────────────────────────────────────
def init_db():
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id     INTEGER PRIMARY KEY,
            username    TEXT,
            first_name  TEXT,
            is_banned   INTEGER DEFAULT 0,
            max_bots    INTEGER DEFAULT 5,
            created_at  REAL DEFAULT (unixepoch())
        );

        CREATE TABLE IF NOT EXISTS bots (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL REFERENCES users(user_id),
            name        TEXT NOT NULL,
            main_file   TEXT NOT NULL DEFAULT 'main.py',
            status      TEXT NOT NULL DEFAULT 'stopped',
            pid         INTEGER,
            restarts    INTEGER DEFAULT 0,
            max_restarts INTEGER DEFAULT 5,
            created_at  REAL DEFAULT (unixepoch()),
            started_at  REAL,
            UNIQUE(user_id, name)
        );

        CREATE TABLE IF NOT EXISTS bot_env (
            bot_id  INTEGER NOT NULL REFERENCES bots(id) ON DELETE CASCADE,
            key     TEXT NOT NULL,
            value   TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (bot_id, key)
        );

        CREATE TABLE IF NOT EXISTS rate_limits (
            user_id    INTEGER NOT NULL,
            action     TEXT NOT NULL,
            count      INTEGER DEFAULT 0,
            window_start REAL DEFAULT (unixepoch()),
            PRIMARY KEY (user_id, action)
        );

        CREATE INDEX IF NOT EXISTS idx_bots_user ON bots(user_id);
        CREATE INDEX IF NOT EXISTS idx_bots_status ON bots(status);
        """)
    logger.info("Database initialized at %s", DB_PATH)


# ──────────────────────────────────────────────
# Users
# ──────────────────────────────────────────────
def upsert_user(user_id: int, username: str, first_name: str):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO users(user_id, username, first_name)
            VALUES(?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                first_name=excluded.first_name
        """, (user_id, username, first_name))
    _cache_del(f"user:{user_id}")


def get_user(user_id: int) -> Optional[dict]:
    key = f"user:{user_id}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
        result = dict(row) if row else None
    _cache_set(key, result)
    return result


def is_banned(user_id: int) -> bool:
    u = get_user(user_id)
    return bool(u and u.get("is_banned"))


# ──────────────────────────────────────────────
# Bots
# ──────────────────────────────────────────────
def create_bot(user_id: int, name: str, main_file: str) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO bots(user_id, name, main_file) VALUES(?,?,?)",
            (user_id, name, main_file)
        )
        bot_id = cur.lastrowid
    _cache_del_prefix(f"bots:{user_id}")
    return bot_id


def get_bot(bot_id: int) -> Optional[dict]:
    key = f"bot:{bot_id}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM bots WHERE id=?", (bot_id,)).fetchone()
        result = dict(row) if row else None
    _cache_set(key, result)
    return result


def get_user_bots(user_id: int) -> list[dict]:
    key = f"bots:{user_id}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM bots WHERE user_id=? ORDER BY created_at DESC", (user_id,)
        ).fetchall()
        result = [dict(r) for r in rows]
    _cache_set(key, result)
    return result


def count_user_bots(user_id: int) -> int:
    return len(get_user_bots(user_id))


def get_all_active_bots() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM bots WHERE status='running'"
        ).fetchall()
        return [dict(r) for r in rows]


def update_bot_status(bot_id: int, status: str, pid: int = None):
    with get_conn() as conn:
        if pid is not None:
            conn.execute(
                "UPDATE bots SET status=?, pid=?, started_at=unixepoch() WHERE id=?",
                (status, pid, bot_id)
            )
        else:
            conn.execute("UPDATE bots SET status=? WHERE id=?", (status, bot_id))
    _cache_del(f"bot:{bot_id}")
    # also invalidate user bots list
    bot = get_bot(bot_id)
    if bot:
        _cache_del_prefix(f"bots:{bot['user_id']}")


def increment_restarts(bot_id: int):
    with get_conn() as conn:
        conn.execute("UPDATE bots SET restarts=restarts+1 WHERE id=?", (bot_id,))
    _cache_del(f"bot:{bot_id}")


def update_bot_main_file(bot_id: int, main_file: str):
    with get_conn() as conn:
        conn.execute("UPDATE bots SET main_file=? WHERE id=?", (main_file, bot_id))
    _cache_del(f"bot:{bot_id}")


def delete_bot(bot_id: int):
    bot = get_bot(bot_id)
    with get_conn() as conn:
        conn.execute("DELETE FROM bots WHERE id=?", (bot_id,))
    _cache_del(f"bot:{bot_id}")
    if bot:
        _cache_del_prefix(f"bots:{bot['user_id']}")


# ──────────────────────────────────────────────
# ENV variables
# ──────────────────────────────────────────────
def get_env(bot_id: int) -> dict[str, str]:
    key = f"env:{bot_id}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT key, value FROM bot_env WHERE bot_id=?", (bot_id,)
        ).fetchall()
        result = {r["key"]: r["value"] for r in rows}
    _cache_set(key, result)
    return result


def set_env_var(bot_id: int, key: str, value: str):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO bot_env(bot_id, key, value) VALUES(?,?,?)
            ON CONFLICT(bot_id, key) DO UPDATE SET value=excluded.value
        """, (bot_id, key, value))
    _cache_del(f"env:{bot_id}")


def delete_env_var(bot_id: int, key: str):
    with get_conn() as conn:
        conn.execute("DELETE FROM bot_env WHERE bot_id=? AND key=?", (bot_id, key))
    _cache_del(f"env:{bot_id}")


def set_env_bulk(bot_id: int, env_dict: dict[str, str]):
    with get_conn() as conn:
        conn.execute("DELETE FROM bot_env WHERE bot_id=?", (bot_id,))
        conn.executemany(
            "INSERT INTO bot_env(bot_id, key, value) VALUES(?,?,?)",
            [(bot_id, k, v) for k, v in env_dict.items()]
        )
    _cache_del(f"env:{bot_id}")


# ──────────────────────────────────────────────
# Rate limiting
# ──────────────────────────────────────────────
RATE_LIMITS = {
    "action":    (10, 60),   # 10 actions per 60s
    "bot_start": (3, 60),    # 3 starts per 60s
    "create_bot": (3, 300),  # 3 creates per 5min
}


def check_rate_limit(user_id: int, action: str) -> bool:
    """Returns True if allowed, False if rate-limited."""
    limit, window = RATE_LIMITS.get(action, (10, 60))
    now = time.time()
    with get_conn() as conn:
        row = conn.execute(
            "SELECT count, window_start FROM rate_limits WHERE user_id=? AND action=?",
            (user_id, action)
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO rate_limits(user_id, action, count, window_start) VALUES(?,?,1,?)",
                (user_id, action, now)
            )
            return True
        count, ws = row["count"], row["window_start"]
        if now - ws > window:
            conn.execute(
                "UPDATE rate_limits SET count=1, window_start=? WHERE user_id=? AND action=?",
                (now, user_id, action)
            )
            return True
        if count >= limit:
            return False
        conn.execute(
            "UPDATE rate_limits SET count=count+1 WHERE user_id=? AND action=?",
            (user_id, action)
        )
        return True


def get_rate_limit_remaining(user_id: int, action: str) -> int:
    limit, window = RATE_LIMITS.get(action, (10, 60))
    now = time.time()
    with get_conn() as conn:
        row = conn.execute(
            "SELECT count, window_start FROM rate_limits WHERE user_id=? AND action=?",
            (user_id, action)
        ).fetchone()
        if row is None:
            return limit
        if now - row["window_start"] > window:
            return limit
        return max(0, limit - row["count"])
