"""入口组件（独立进程）。

职责：
- 扫描 spool/ 中的新压缩包，文件名解析租户与客户端 UID；
- 为每个租户维护严格递增序号（租户内顺序的根基），去重 (tenant, client_uid)；
- 识别 replay__ 前缀的修正包，把隔离包在【原 seq 位置】复位为 queued，
  已落库消息先清除（隔离包通常无消息），保证重放不会重复影响已处理数据；
- 不合法 / 超限 / 重复文件移动到 rejected/ 并审计，绝不静默丢弃。

入口只负责"登记与编排位置"，不做任何解压。
"""
from __future__ import annotations

import os
import signal
import sqlite3
import time
import uuid

from .common import (atomic_replace, now_iso, parse_spool_name, valid_tenant)
from .config import Config
from .db import (ST_DEAD, ST_QUARANTINED, ST_QUEUED, audit, connect,
                 heartbeat, init_db, tx)

COMPONENT = "ingress"


class Ingress:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.conn = connect(cfg.db_path)
        self._stop = False
        # filename -> (size, first_seen_ts)：等文件大小稳定再处理，防止读到半文件
        self._pending: dict[str, tuple[int, float]] = {}
        self._blobs = os.path.join(cfg.home, "blobs")
        os.makedirs(self._blobs, exist_ok=True)

    def request_stop(self, *_args) -> None:
        self._stop = True

    # ------------------------------------------------------------------ #
    # 控制请求（Web 控制台写入 control_requests，入口消费）
    # ------------------------------------------------------------------ #
    def handle_control_requests(self) -> None:
        rows = self.conn.execute(
            "SELECT * FROM control_requests WHERE status='pending' ORDER BY id"
        ).fetchall()
        for row in rows:
            try:
                result = self._handle_one_control(row)
            except Exception as exc:  # 控制请求失败要留痕，不能杀死循环
                result = f"error: {exc!r}"
                self.conn.execute(
                    "UPDATE control_requests SET status='failed', handled_at=?,"
                    " result=? WHERE id=?",
                    (time.time(), result, row["id"]),
                )
                audit(self.conn, COMPONENT, "control_failed",
                      {"request_id": row["id"], "error": result})
            else:
                self.conn.execute(
                    "UPDATE control_requests SET status='handled', handled_at=?,"
                    " result=? WHERE id=?",
                    (time.time(), result, row["id"]),
                )

    def _handle_one_control(self, row) -> str:
        packet = self.conn.execute(
            "SELECT * FROM packets WHERE id=?", (row["packet_id"],)
        ).fetchone()
        if packet is None:
            return "packet_not_found"
        if row["request_type"] == "discard":
            if packet["status"] != ST_QUARANTINED:
                return f"not_quarantined(status={packet['status']})"
            with tx(self.conn):
                self.conn.execute(
                    "UPDATE packets SET status=?, error_detail=?,"
                    " leased_by=NULL, leased_at=NULL, updated_at=? WHERE id=?",
                    (ST_DEAD,
                     (packet["error_detail"] or "") + " | 人工确认跳号，该序号永久留缺口",
                      time.time(), packet["id"]),
                )
                audit(self.conn, COMPONENT, "gap_acknowledged",
                      {"packet_id": packet["id"], "seq": packet["seq"],
                       "reason": packet["error_code"]},
                      tenant_id=packet["tenant_id"], packet_id=packet["id"])
            return "marked_dead"

        if row["request_type"] == "retry":
            if packet["status"] != ST_QUARANTINED:
                return f"not_quarantined(status={packet['status']})"
            new_blob = row["new_blob"]
            if new_blob and os.path.exists(new_blob):
                blob_path, blob_size = self._store_blob(packet["id"], new_blob,
                                                        packet["attempts"])
            else:
                blob_path, blob_size = packet["blob_path"], packet["blob_size"]
            self._reset_quarantined(packet, blob_path, blob_size,
                                    source="control_retry")
            return "requeued"
        return f"unknown_type:{row['request_type']}"

    # ------------------------------------------------------------------ #
    # spool 扫描
    # ------------------------------------------------------------------ #
    def scan_spool(self) -> None:
        try:
            names = sorted(os.listdir(self.cfg.spool_dir))
        except FileNotFoundError:
            return
        seen = set()
        for name in names:
            if name.endswith(".tmp") or not name.endswith(".gz"):
                continue
            seen.add(name)
            path = os.path.join(self.cfg.spool_dir, name)
            if not os.path.isfile(path):
                continue
            try:
                size = os.path.getsize(path)
            except OSError:
                continue
            prev = self._pending.get(name)
            if prev is None or prev[0] != size:
                self._pending[name] = (size, time.time())
                continue  # 等下一轮确认大小稳定
            if time.time() - prev[1] < self.cfg.poll_interval:
                continue
            self._ingest_file(name, path, size)
        for gone in [n for n in self._pending if n not in seen]:
            self._pending.pop(gone, None)

    def _ingest_file(self, name: str, path: str, size: int) -> None:
        meta = parse_spool_name(name)
        self._pending.pop(name, None)
        if meta is None or not valid_tenant(meta["tenant_id"]):
            self._reject(path, name, "bad_filename", {"name": name})
            return
        if size > self.cfg.max_blob_bytes:
            self._reject(path, name, "blob_too_large",
                         {"size": size, "limit": self.cfg.max_blob_bytes},
                         tenant_id=meta["tenant_id"])
            return
        if meta["replay_packet_id"]:
            self._apply_replay_file(path, name, meta, size)
        else:
            self._ingest_new(path, name, meta, size)

    def _ingest_new(self, path: str, name: str, meta: dict, size: int) -> None:
        packet_id = uuid.uuid4().hex
        blob_path = os.path.join(self._blobs, f"{packet_id}.gz")
        # 事务前预检：保证重复事件能在独立事务里落审计（不会随主事务回滚丢失）
        existing = self.conn.execute(
            "SELECT id, status FROM packets WHERE tenant_id=? AND client_uid=?",
            (meta["tenant_id"], meta["client_uid"]),
        ).fetchone()
        if existing is not None:
            with tx(self.conn):
                audit(self.conn, COMPONENT, "ingest_duplicate",
                      {"filename": name, "existing_packet": existing["id"],
                       "existing_status": existing["status"]},
                      tenant_id=meta["tenant_id"])
            self._reject(path, name, "duplicate_uid",
                         {"client_uid": meta["client_uid"],
                          "existing_packet": existing["id"],
                          "existing_status": existing["status"]},
                         tenant_id=meta["tenant_id"])
            return
        try:
            with tx(self.conn):
                self.conn.execute(
                    "INSERT INTO tenants(tenant_id, created_at, last_seq)"
                    " VALUES(?, ?, 0)"
                    " ON CONFLICT(tenant_id) DO UPDATE SET tenant_id=tenant_id",
                    (meta["tenant_id"], time.time()),
                )
                row = self.conn.execute(
                    "SELECT last_seq FROM tenants WHERE tenant_id=?",
                    (meta["tenant_id"],),
                ).fetchone()
                seq = row["last_seq"] + 1
                # 事务内完成原子移动：DB 提交失败则文件还在 spool，下轮可重试
                atomic_replace(path, blob_path)
                self.conn.execute(
                    "UPDATE tenants SET last_seq=? WHERE tenant_id=?",
                    (seq, meta["tenant_id"]),
                )
                self.conn.execute(
                    "INSERT INTO packets(id, tenant_id, seq, client_uid,"
                    " blob_path, blob_size, status, created_at, updated_at)"
                    " VALUES(?,?,?,?,?,?,?,?,?)",
                    (packet_id, meta["tenant_id"], seq, meta["client_uid"],
                     blob_path, size, ST_QUEUED, time.time(), time.time()),
                )
                audit(self.conn, COMPONENT, "ingested",
                      {"filename": name, "seq": seq, "blob_size": size},
                      tenant_id=meta["tenant_id"], packet_id=packet_id)
        except sqlite3.IntegrityError:
            # 并发下 UNIQUE(tenant_id, client_uid) 兜底
            if os.path.exists(blob_path):
                try:
                    os.replace(blob_path, path)  # 把文件退回 spool 走拒收
                except OSError:
                    pass
            self._reject(path, name, "duplicate_uid",
                         {"client_uid": meta["client_uid"],
                          "reason": "integrity_constraint"},
                         tenant_id=meta["tenant_id"])

    def _apply_replay_file(self, path: str, name: str, meta: dict, size: int) -> None:
        packet = self.conn.execute(
            "SELECT * FROM packets WHERE id=?", (meta["replay_packet_id"],)
        ).fetchone()
        if packet is None:
            self._reject(path, name, "replay_target_missing",
                         {"packet_id": meta["replay_packet_id"]},
                         tenant_id=meta["tenant_id"])
            return
        if packet["tenant_id"] != meta["tenant_id"]:
            self._reject(path, name, "replay_tenant_mismatch",
                         {"packet_id": packet["id"],
                          "file_tenant": meta["tenant_id"],
                          "packet_tenant": packet["tenant_id"]},
                         tenant_id=meta["tenant_id"])
            return
        if packet["status"] != ST_QUARANTINED:
            # 幂等保护：目标不在隔离态说明已处理过，绝不重复入队
            self._reject(path, name, "replay_not_quarantined",
                         {"packet_id": packet["id"], "status": packet["status"]},
                         tenant_id=meta["tenant_id"])
            return
        blob_path, blob_size = self._store_blob(packet["id"], path,
                                                packet["attempts"])
        self._reset_quarantined(packet, blob_path, blob_size,
                                source="spool_replay_file",
                                extra={"filename": name, "size": size})

    def _store_blob(self, packet_id: str, src_path: str, attempts: int) -> tuple[str, int]:
        version = attempts + 1
        blob_path = os.path.join(self._blobs, f"{packet_id}.v{version}.gz")
        atomic_replace(src_path, blob_path)
        return blob_path, os.path.getsize(blob_path)

    def _reset_quarantined(self, packet, blob_path: str, blob_size: int,
                           source: str, extra: dict | None = None) -> None:
        """把隔离包在原租户、原 seq 位置复位为 queued（原位接回的核心）。"""
        with tx(self.conn):
            # 防御性清理：隔离包理论上没有 staged 消息；若有残留先清，杜绝重复
            self.conn.execute("DELETE FROM messages WHERE packet_id=?",
                              (packet["id"],))
            self.conn.execute(
                "UPDATE packets SET status=?, blob_path=?, blob_size=?,"
                " error_code=NULL, error_detail=NULL, leased_by=NULL,"
                " leased_at=NULL, uncompressed_size=NULL, msg_count=NULL,"
                " updated_at=? WHERE id=?",
                (ST_QUEUED, blob_path, blob_size, time.time(), packet["id"]),
            )
            detail = {"source": source, "seq": packet["seq"],
                      "blob_path": blob_path, "blob_size": blob_size}
            if extra:
                detail.update(extra)
            audit(self.conn, COMPONENT, "replay_requeued", detail,
                  tenant_id=packet["tenant_id"], packet_id=packet["id"])

    def _reject(self, path: str, name: str, reason: str, detail: dict,
                tenant_id: str | None = None) -> None:
        dest = os.path.join(self.cfg.reject_dir, f"{reason}__{name}")
        try:
            if os.path.exists(path):
                atomic_replace(path, dest)
        except OSError:
            dest = "(move_failed)"
        with tx(self.conn):
            audit(self.conn, COMPONENT, "rejected",
                  {**detail, "reason": reason, "moved_to": dest,
                   "at": now_iso()}, tenant_id=tenant_id)

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self.request_stop)
        signal.signal(signal.SIGINT, self.request_stop)
        self.conn.execute(
            "INSERT INTO components(name, pid, heartbeat, status) VALUES(?,?,?,?)"
            " ON CONFLICT(name) DO UPDATE SET pid=excluded.pid,"
            " heartbeat=excluded.heartbeat, status=excluded.status",
            (COMPONENT, os.getpid(), time.time(), "running"),
        )
        while not self._stop:
            heartbeat(self.conn, COMPONENT)
            self.handle_control_requests()
            self.scan_spool()
            time.sleep(self.cfg.poll_interval)
        self.conn.execute(
            "UPDATE components SET status='stopped', heartbeat=? WHERE name=?",
            (time.time(), COMPONENT),
        )
        self.conn.close()


def main(home: str) -> None:
    from .config import ensure_dirs, load_config
    cfg = load_config(home)
    ensure_dirs(cfg)
    init_db(cfg.db_path)
    Ingress(cfg).run()
