"""SQLite 持久层（WAL）。

状态机与表结构是整套系统"崩溃安全 / 恰好一次 / 原位回放"的基础：
- packets 记录每个包在租户序列中的位置、状态、隔离原因、租约；
- messages 用 (tenant_id, seq) UNIQUE 约束保证一条消息最多落库一次；
- consumer_results 用 message_id 外键 UNIQUE 保证消费者恰好处理一次；
- audit_events 追加式审计，隔离、回放、跳号处置全部留痕。
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from contextlib import contextmanager
from typing import Iterator, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS tenants (
    tenant_id     TEXT PRIMARY KEY,
    created_at    REAL NOT NULL,
    last_seq      INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS packets (
    id                TEXT PRIMARY KEY,
    tenant_id         TEXT NOT NULL REFERENCES tenants(tenant_id),
    seq               INTEGER NOT NULL,
    client_uid        TEXT NOT NULL,
    blob_path         TEXT NOT NULL,
    blob_size         INTEGER NOT NULL,
    uncompressed_size INTEGER,
    msg_count         INTEGER,
    status            TEXT NOT NULL,
    error_code        TEXT,
    error_detail      TEXT,
    attempts          INTEGER NOT NULL DEFAULT 0,
    leased_by         TEXT,
    leased_at         REAL,
    created_at        REAL NOT NULL,
    updated_at        REAL NOT NULL,
    UNIQUE(tenant_id, seq),
    UNIQUE(tenant_id, client_uid)
);

CREATE TABLE IF NOT EXISTS messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id   TEXT NOT NULL,
    seq         INTEGER NOT NULL,
    packet_id   TEXT NOT NULL REFERENCES packets(id),
    msg_index   INTEGER NOT NULL,
    payload     TEXT NOT NULL,
    payload_sha TEXT NOT NULL,
    created_at  REAL NOT NULL,
    UNIQUE(tenant_id, seq)
);

CREATE TABLE IF NOT EXISTS consumer_results (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id    INTEGER NOT NULL UNIQUE REFERENCES messages(id),
    tenant_id     TEXT NOT NULL,
    seq           INTEGER NOT NULL,
    output_sha    TEXT NOT NULL,
    processed_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          REAL NOT NULL,
    component   TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    tenant_id   TEXT,
    packet_id   TEXT,
    detail      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS resource_metrics (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          REAL NOT NULL,
    rss_bytes   INTEGER NOT NULL,
    tmp_bytes   INTEGER NOT NULL,
    spool_bytes INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS components (
    name       TEXT PRIMARY KEY,
    pid        INTEGER,
    heartbeat  REAL,
    status     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS control_requests (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           REAL NOT NULL,
    request_type TEXT NOT NULL,
    packet_id    TEXT NOT NULL,
    new_blob     TEXT,
    status       TEXT NOT NULL DEFAULT 'pending',
    handled_at   REAL,
    result       TEXT
);

CREATE INDEX IF NOT EXISTS idx_packets_tenant_status ON packets(tenant_id, status, seq);
CREATE INDEX IF NOT EXISTS idx_messages_tenant_seq ON messages(tenant_id, seq);
CREATE INDEX IF NOT EXISTS idx_results_tenant ON consumer_results(tenant_id, seq);
CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_events(ts);
"""

# 包状态机
ST_QUEUED = "queued"          # 已入场，等待解包
ST_PROCESSING = "processing"  # 处理器持有租约，正在解包
ST_QUARANTINED = "quarantined"  # 隔离：坏包挡住本租户队头
ST_DEAD = "dead"              # 人工确认跳号：该位置永久留缺口
ST_READY = "ready"            # 解包成功，消息已落库，等待消费
ST_DONE = "done"              # 租户序列内该包所有消息已消费完

ALL_STATUSES = {ST_QUEUED, ST_PROCESSING, ST_QUARANTINED, ST_DEAD, ST_READY, ST_DONE}


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: str) -> None:
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()


@contextmanager
def tx(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """显式 BEGIN IMMEDIATE 事务，保证多进程并发下的写入串行化。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except Exception:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def audit(conn: sqlite3.Connection, component: str, event_type: str,
          detail: str, tenant_id: Optional[str] = None,
          packet_id: Optional[str] = None) -> None:
    conn.execute(
        "INSERT INTO audit_events(ts, component, event_type, tenant_id, packet_id, detail)"
        " VALUES (?,?,?,?,?,?)",
        (time.time(), component, event_type, tenant_id, packet_id,
         json.dumps(detail, ensure_ascii=False)),
    )


def heartbeat(conn: sqlite3.Connection, name: str, status: str = "running") -> None:
    conn.execute(
        "INSERT INTO components(name, pid, heartbeat, status) VALUES(?,?,?,?)"
        " ON CONFLICT(name) DO UPDATE SET pid=excluded.pid,"
        " heartbeat=excluded.heartbeat, status=excluded.status",
        (name, os.getpid(), time.time(), status),
    )
