"""消费端组件（独立进程）。

- 租户内严格按消息 seq 顺序消费（每次只取租户内最老的未消费消息）；
- 跨租户轮转，单个租户消息再多也不会饿死其他租户；
- 恰好一次：consumer_results.message_id 有 UNIQUE 约束，
  "写处理结果 + 标记消息已消费" 在同一事务提交；进程在提交前崩溃，
  重启后重放同一条消息，不会产生第二条结果；
- 提交后追加外部结果文件 consumer_results.jsonl，启动时按 DB 对账重建，
  网页的"核对消费者结果"同时校验库内与外部文件两份产物。
"""
from __future__ import annotations

import json
import os
import signal
import time

from .common import sha256_text
from .config import Config
from .db import (ST_DONE, ST_READY, audit, connect, heartbeat, init_db, tx)

COMPONENT = "consumer"


class Consumer:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.conn = connect(cfg.db_path)
        self._stop = False
        self._cursor = ""
        self._fh = None

    def request_stop(self, *_args) -> None:
        self._stop = True

    # ------------------------------------------------------------------ #
    # 外部结果文件：启动时按数据库重建，保证两份产物始终一致
    # ------------------------------------------------------------------ #
    def reconcile_output(self) -> None:
        rows = self.conn.execute(
            "SELECT cr.tenant_id, cr.seq, m.payload, cr.output_sha, cr.processed_at"
            " FROM consumer_results cr JOIN messages m ON m.id=cr.message_id"
            " ORDER BY cr.tenant_id, cr.seq"
        ).fetchall()
        if os.path.exists(self.cfg.result_path):
            os.replace(self.cfg.result_path,
                       self.cfg.result_path + f".stale.{int(time.time())}")
        with open(self.cfg.result_path, "w", encoding="utf-8") as fh:
            for r in rows:
                fh.write(self._render_line(r["tenant_id"], r["seq"],
                                           r["payload"], r["output_sha"],
                                           r["processed_at"]))
                fh.flush()
            os.fsync(fh.fileno())
        self._fh = open(self.cfg.result_path, "a", encoding="utf-8")
        with tx(self.conn):
            audit(self.conn, COMPONENT, "output_reconciled",
                  {"results": len(rows), "path": self.cfg.result_path})

    @staticmethod
    def _render_line(tenant_id: str, seq: int, payload: str,
                     output_sha: str, processed_at: float) -> str:
        return json.dumps({
            "tenant_id": tenant_id,
            "seq": seq,
            "payload": json.loads(payload),
            "output_sha": output_sha,
            "processed_at": processed_at,
        }, ensure_ascii=False) + "\n"

    def _business_logic(self, tenant_id: str, seq: int,
                        payload: str) -> tuple[str, str]:
        """示例业务处理：确定性派生，便于网页核对，无外部副作用。"""
        output = f"{tenant_id}:{seq}:{payload}"
        return output, sha256_text(output)

    # ------------------------------------------------------------------ #
    # 调度
    # ------------------------------------------------------------------ #
    def pick_next_message(self):
        tenants = [r["tenant_id"] for r in self.conn.execute(
            "SELECT DISTINCT m.tenant_id FROM messages m"
            " LEFT JOIN consumer_results cr ON cr.message_id=m.id"
            " WHERE cr.id IS NULL ORDER BY m.tenant_id").fetchall()]
        if not tenants:
            return None
        ordered = sorted(tenants)
        if self._cursor in ordered:
            idx = (ordered.index(self._cursor) + 1) % len(ordered)
            ordered = ordered[idx:] + ordered[:idx]
        for tenant_id in ordered:
            row = self.conn.execute(
                "SELECT m.* FROM messages m"
                " LEFT JOIN consumer_results cr ON cr.message_id=m.id"
                " WHERE cr.id IS NULL AND m.tenant_id=?"
                " ORDER BY m.seq LIMIT 1", (tenant_id,)).fetchone()
            if row is not None:
                self._cursor = tenant_id
                return row
        return None

    def _consume_one(self, msg) -> None:
        output, output_sha = self._business_logic(msg["tenant_id"], msg["seq"],
                                                  msg["payload"])
        processed_at = time.time()
        # 1) DB 事务：结果落库（UNIQUE 防重）+ 包完成状态推进
        with tx(self.conn):
            self.conn.execute(
                "INSERT INTO consumer_results(message_id, tenant_id, seq,"
                " output_sha, processed_at) VALUES(?,?,?,?,?)",
                (msg["id"], msg["tenant_id"], msg["seq"],
                 output_sha, processed_at),
            )
            remaining = self.conn.execute(
                "SELECT COUNT(*) AS n FROM messages m"
                " LEFT JOIN consumer_results cr ON cr.message_id=m.id"
                " WHERE m.packet_id=? AND cr.id IS NULL",
                (msg["packet_id"],),
            ).fetchone()["n"]
            if remaining == 0:
                self.conn.execute(
                    "UPDATE packets SET status=?, updated_at=? WHERE id=?"
                    " AND status=?",
                    (ST_DONE, time.time(), msg["packet_id"], ST_READY),
                )
                audit(self.conn, COMPONENT, "packet_completed",
                      {"packet_id": msg["packet_id"]},
                      tenant_id=msg["tenant_id"], packet_id=msg["packet_id"])
        # 2) 事务提交后再追加外部文件；崩溃在这之后 -> 重启 reconcile 去重重建
        self._fh.write(self._render_line(msg["tenant_id"], msg["seq"],
                                         msg["payload"], output_sha,
                                         processed_at))
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self.request_stop)
        signal.signal(signal.SIGINT, self.request_stop)
        self.reconcile_output()
        self.conn.execute(
            "INSERT INTO components(name, pid, heartbeat, status) VALUES(?,?,?,?)"
            " ON CONFLICT(name) DO UPDATE SET pid=excluded.pid,"
            " heartbeat=excluded.heartbeat, status=excluded.status",
            (COMPONENT, os.getpid(), time.time(), "running"),
        )
        while not self._stop:
            heartbeat(self.conn, COMPONENT)
            try:
                msg = self.pick_next_message()
            except Exception as exc:
                with tx(self.conn):
                    audit(self.conn, COMPONENT, "consumer_loop_error",
                          {"error": repr(exc)})
                time.sleep(self.cfg.backoff_seconds)
                continue
            if msg is None:
                time.sleep(self.cfg.poll_interval)
                continue
            try:
                self._consume_one(msg)
            except Exception as exc:
                # 业务异常：消息未确认，下轮重试；记录审计而不是静默卡死
                with tx(self.conn):
                    audit(self.conn, COMPONENT, "consume_failed_retry",
                          {"message_id": msg["id"], "error": repr(exc)},
                          tenant_id=msg["tenant_id"])
                time.sleep(self.cfg.backoff_seconds)
        if self._fh:
            self._fh.close()
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
    Consumer(cfg).run()
