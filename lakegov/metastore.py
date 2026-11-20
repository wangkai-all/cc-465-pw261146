"""元数据存储：SQLite 实现的湖表 metastore。

快照隔离采用 MVCC（类 Iceberg）：
- 每个文件记录 added_snapshot / deleted_snapshot；
- 快照 S 可见的文件：added_snapshot <= S AND (deleted_snapshot IS NULL OR deleted_snapshot > S)；
- 合并提交是单个 SQLite 事务：旧文件置 deleted、新文件置 added，同时写新快照行；
- 合并失败 => 事务不提交 => 原文件引用天然保持不变（回滚零成本）；
- 读查询开始时 pin 快照（pinned_snapshots 表），GC 不得清理仍被 pin 引用的文件。
"""
import os
import sqlite3
import threading
import time
import uuid

from . import config

# 提交串行化：SQLite 单写者，合并提交统一走这把锁
COMMIT_LOCK = threading.RLock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  table_name TEXT NOT NULL,
  kind TEXT NOT NULL,              -- INIT / MERGE_COMMIT / MERGE_ROLLBACK / TIER_CHANGE
  detail TEXT DEFAULT '',
  created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS partitions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  table_name TEXT NOT NULL,
  part_key TEXT NOT NULL,
  tier TEXT NOT NULL DEFAULT 'HOT',   -- HOT / COLD
  created_at REAL NOT NULL,
  last_accessed REAL,
  UNIQUE(table_name, part_key)
);
CREATE TABLE IF NOT EXISTS files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  table_name TEXT NOT NULL,
  part_key TEXT NOT NULL,
  path TEXT NOT NULL,
  size_bytes INTEGER NOT NULL,
  row_count INTEGER NOT NULL,
  min_key INTEGER NOT NULL,
  max_key INTEGER NOT NULL,
  checksum TEXT NOT NULL,
  corrupt INTEGER NOT NULL DEFAULT 0,
  added_snapshot INTEGER NOT NULL,
  deleted_snapshot INTEGER,
  created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_files_part ON files(table_name, part_key);
CREATE TABLE IF NOT EXISTS merge_jobs (
  id TEXT PRIMARY KEY,
  table_name TEXT NOT NULL,
  part_key TEXT NOT NULL,
  strategy TEXT NOT NULL,
  repair INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL,              -- PLANNED/RUNNING/RETRYING/COMMITTED/FAILED/NO_OP
  plan_json TEXT NOT NULL,
  cost_json TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  fail_at TEXT,                      -- 故障注入点：read/write/commit
  fail_attempts TEXT DEFAULT '[1]',  -- 在哪些尝试上注入失败
  snapshot_before INTEGER,
  snapshot_after INTEGER,
  error TEXT,
  history TEXT DEFAULT '[]',         -- 每次尝试的记录
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS query_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  table_name TEXT NOT NULL,
  part_start TEXT, part_end TEXT,
  key_min INTEGER, key_max INTEGER,
  snapshot INTEGER,
  partitions_hit INTEGER, partitions_pruned INTEGER,
  files_hit INTEGER, files_pruned INTEGER,
  corrupt_files INTEGER DEFAULT 0,
  bytes_scanned INTEGER, rows_returned INTEGER,
  duration_ms REAL,
  hit_parts TEXT DEFAULT '[]',
  created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS pinned_snapshots (
  token TEXT PRIMARY KEY,
  table_name TEXT NOT NULL,
  snapshot INTEGER NOT NULL,
  created_at REAL NOT NULL
);
"""


def connect():
    """每次调用新建连接（线程安全：每线程独立连接）。"""
    os.makedirs(config.DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(config.DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db():
    with connect() as conn:
        conn.executescript(SCHEMA)


def reset_db():
    """清空全部元数据（测试 / 重新模拟用）。"""
    with connect() as conn:
        for t in ("snapshots", "partitions", "files", "merge_jobs",
                  "query_log", "pinned_snapshots"):
            conn.execute(f"DROP TABLE IF EXISTS {t}")
        conn.executescript(SCHEMA)


# ---------------- 快照 ----------------

def current_snapshot(conn, table):
    row = conn.execute(
        "SELECT MAX(id) AS v FROM snapshots WHERE table_name=?", (table,)
    ).fetchone()
    return row["v"] or 0


def new_snapshot(conn, table, kind, detail=""):
    cur = conn.execute(
        "INSERT INTO snapshots(table_name, kind, detail, created_at) VALUES(?,?,?,?)",
        (table, kind, detail, time.time()),
    )
    return cur.lastrowid


def list_snapshots(conn, table, limit=50):
    return [dict(r) for r in conn.execute(
        "SELECT * FROM snapshots WHERE table_name=? ORDER BY id DESC LIMIT ?",
        (table, limit)).fetchall()]


def pin_snapshot(conn, table, snapshot):
    token = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO pinned_snapshots(token, table_name, snapshot, created_at)"
        " VALUES(?,?,?,?)",
        (token, table, snapshot, time.time()),
    )
    conn.commit()
    return token


def unpin_snapshot(conn, token):
    conn.execute("DELETE FROM pinned_snapshots WHERE token=?", (token,))
    conn.commit()


def min_pinned_snapshot(conn, table):
    row = conn.execute(
        "SELECT MIN(snapshot) AS v FROM pinned_snapshots WHERE table_name=?",
        (table,)).fetchone()
    return row["v"]


# ---------------- 分区 / 文件 ----------------

def add_partition(conn, table, part_key, tier="HOT"):
    conn.execute(
        "INSERT OR IGNORE INTO partitions(table_name, part_key, tier, created_at)"
        " VALUES(?,?,?,?)",
        (table, part_key, tier, time.time()),
    )


def list_partitions(conn, table):
    return [dict(r) for r in conn.execute(
        "SELECT * FROM partitions WHERE table_name=? ORDER BY part_key",
        (table,)).fetchall()]


def get_partition(conn, table, part_key):
    r = conn.execute(
        "SELECT * FROM partitions WHERE table_name=? AND part_key=?",
        (table, part_key)).fetchone()
    return dict(r) if r else None


def touch_partition(conn, table, part_key):
    conn.execute(
        "UPDATE partitions SET last_accessed=? WHERE table_name=? AND part_key=?",
        (time.time(), table, part_key))
    conn.commit()


def insert_file(conn, table, part_key, meta, snapshot):
    cur = conn.execute(
        "INSERT INTO files(table_name, part_key, path, size_bytes, row_count,"
        " min_key, max_key, checksum, corrupt, added_snapshot, created_at)"
        " VALUES(?,?,?,?,?,?,?,?,0,?,?)",
        (table, part_key, meta["path"], meta["size_bytes"], meta["row_count"],
         meta["min_key"], meta["max_key"], meta["checksum"], snapshot,
         time.time()),
    )
    return cur.lastrowid


def visible_files(conn, table, part_key, snapshot):
    """快照 S 下可见的文件集合（MVCC 核心）。"""
    return [dict(r) for r in conn.execute(
        "SELECT * FROM files WHERE table_name=? AND part_key=?"
        " AND added_snapshot<=? AND (deleted_snapshot IS NULL OR deleted_snapshot>?)"
        " ORDER BY id",
        (table, part_key, snapshot, snapshot)).fetchall()]


def all_live_files(conn, table):
    return [dict(r) for r in conn.execute(
        "SELECT * FROM files WHERE table_name=? AND deleted_snapshot IS NULL"
        " ORDER BY part_key, id", (table,)).fetchall()]


def mark_corrupt(conn, file_id, corrupt=1):
    conn.execute("UPDATE files SET corrupt=? WHERE id=?", (corrupt, file_id))
    conn.commit()


# ---------------- 查询日志 ----------------

def log_query(conn, q):
    conn.execute(
        "INSERT INTO query_log(table_name, part_start, part_end, key_min, key_max,"
        " snapshot, partitions_hit, partitions_pruned, files_hit, files_pruned,"
        " corrupt_files, bytes_scanned, rows_returned, duration_ms, hit_parts,"
        " created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (q["table"], q.get("part_start"), q.get("part_end"), q.get("key_min"),
         q.get("key_max"), q["snapshot"], q["partitions_hit"],
         q["partitions_pruned"], q["files_hit"], q["files_pruned"],
         q["corrupt_files"], q["bytes_scanned"], q["rows_returned"],
         q["duration_ms"], q.get("hit_parts", "[]"), time.time()),
    )
    conn.commit()


def recent_queries(conn, table, limit=50):
    return [dict(r) for r in conn.execute(
        "SELECT * FROM query_log WHERE table_name=? ORDER BY id DESC LIMIT ?",
        (table, limit)).fetchall()]
