"""解包处理器组件（独立进程，受总内存与临时盘额度约束）。

调度原则：
- 按租户轮转（round-robin 游标），每次取一个租户队头可处理的包；
  被隔离 / 处理中的租户本轮跳过——坏包只堵自己，其他租户继续流动；
- 租户内严格按 seq 处理，dead 包（人工确认跳号）让位给后续包；
- processing 租约超时即视为上次进程崩溃，自动重新接管（幂等解压）。

额度控制：
- 流式 gzip 解压，边解压边累计未压缩字节，超 max_uncompressed_bytes 立即中止；
- 解包产物落在 tmp_extract/<packet_id>/，总量超 temp_disk_bytes 全队退避；
- RSS 超 memory_bytes 同样退避，不接新包。
"""
from __future__ import annotations

import gzip
import json
import os
import shutil
import signal
import threading
import time

from .common import dir_size, rss_bytes, sha256_text
from .config import Config
from .db import (ST_DONE, ST_PROCESSING, ST_QUARANTINED, ST_QUEUED, ST_READY,
                 audit, connect, heartbeat, init_db, tx)

COMPONENT = "processor"

# 隔离原因码（页面直接展示，同时驱动演示场景断言）
ERR_UNCOMPRESSED_LIMIT = "UNCOMPRESSED_LIMIT"
ERR_DISK_QUOTA = "TEMP_DISK_QUOTA"
ERR_FORMAT_CORRUPT = "FORMAT_CORRUPT"
ERR_BAD_MESSAGE = "BAD_MESSAGE"
ERR_TOO_MANY_MESSAGES = "TOO_MANY_MESSAGES"
ERR_BLOB_MISSING = "BLOB_MISSING"


class ExtractError(Exception):
    def __init__(self, code: str, detail: str):
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


class Processor:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.conn = connect(cfg.db_path)
        self._stop = False
        self._rr_cursor = ""  # 轮转游标：记录上次服务的租户，保证公平
        self._lock = threading.Lock()
        self._metrics = {"rss": 0, "tmp": 0, "spool": 0}

    def request_stop(self, *_args) -> None:
        self._stop = True

    # ------------------------------------------------------------------ #
    # 资源采样线程：页面能看到额度占用曲线的最新点
    # ------------------------------------------------------------------ #
    def _sampler(self) -> None:
        spool_dir = os.path.join(self.cfg.home, "spool")
        while not self._stop:
            rss = rss_bytes()
            tmp = dir_size(self.cfg.tmp_dir)
            spool = dir_size(spool_dir)
            with self._lock:
                self._metrics = {"rss": rss, "tmp": tmp, "spool": spool}
            try:
                self.conn.execute(
                    "INSERT INTO resource_metrics(ts, rss_bytes, tmp_bytes,"
                    " spool_bytes) VALUES(?,?,?,?)",
                    (time.time(), rss, tmp, spool),
                )
            except Exception:
                pass
            time.sleep(self.cfg.sample_interval)

    # ------------------------------------------------------------------ #
    # 崩溃恢复：接管过期租约
    # ------------------------------------------------------------------ #
    def recover_stale_leases(self) -> int:
        cutoff = time.time() - self.cfg.lease_seconds
        rows = self.conn.execute(
            "SELECT id, tenant_id FROM packets WHERE status=? AND leased_at < ?",
            (ST_PROCESSING, cutoff),
        ).fetchall()
        for row in rows:
            with tx(self.conn):
                self.conn.execute(
                    "UPDATE packets SET status=?, leased_by=NULL,"
                    " leased_at=NULL, updated_at=? WHERE id=?",
                    (ST_QUEUED, time.time(), row["id"]),
                )
                # 上次崩溃可能残留部分消息与临时目录，全部清理后重解（幂等）
                self.conn.execute("DELETE FROM messages WHERE packet_id=?",
                                  (row["id"],))
                audit(self.conn, COMPONENT, "lease_recovered",
                      {"packet_id": row["id"], "lease_seconds": self.cfg.lease_seconds},
                      tenant_id=row["tenant_id"], packet_id=row["id"])
            stale_dir = os.path.join(self.cfg.tmp_dir, row["id"])
            shutil.rmtree(stale_dir, ignore_errors=True)
        return len(rows)

    # ------------------------------------------------------------------ #
    # 公平调度：轮转选租户，再取该租户队头
    # ------------------------------------------------------------------ #
    def pick_next_packet(self):
        """公平轮转选租户，再取该租户【队头】可处理的包。

        租户内保序要求：一个包只有在所有更早的包都已到终态
        （ready/done 或 dead 让位）时才能解包。否则隔离中的队头会
        阻塞本租户——这正是"坏包只堵自己、不堵别人"的语义。
        """
        tenants = [r["tenant_id"] for r in self.conn.execute(
            "SELECT DISTINCT tenant_id FROM packets WHERE status IN (?, ?)"
            " ORDER BY tenant_id", (ST_QUEUED, ST_PROCESSING)).fetchall()]
        if not tenants:
            return None
        ordered = sorted(tenants)
        if self._rr_cursor in ordered:
            idx = (ordered.index(self._rr_cursor) + 1) % len(ordered)
            ordered = ordered[idx:] + ordered[:idx]
        for tenant_id in ordered:
            row = self.conn.execute(
                "SELECT * FROM packets WHERE tenant_id=? AND status IN (?, ?)"
                " ORDER BY seq LIMIT 1",
                (tenant_id, ST_QUEUED, ST_PROCESSING),
            ).fetchone()
            if row is None:
                continue
            if row["status"] == ST_PROCESSING:
                if row["leased_at"] and \
                        time.time() - row["leased_at"] < self.cfg.lease_seconds:
                    continue  # 别的处理器正在干，跳过找下一个租户
            # 队头保序闸门：前面不允许存在未终态（queued/processing/quarantined）包
            ahead = self.conn.execute(
                "SELECT COUNT(*) AS n FROM packets WHERE tenant_id=? AND seq < ?"
                " AND status IN (?, ?, ?)",
                (tenant_id, row["seq"], ST_QUEUED, ST_PROCESSING,
                 ST_QUARANTINED),
            ).fetchone()["n"]
            if ahead > 0:
                continue  # 本租户被更早的隔离/未决包阻塞，换下一个租户
            self._rr_cursor = tenant_id
            return row
        return None

    def _capacity_ok(self, blob_size: int) -> tuple[bool, str]:
        with self._lock:
            rss, tmp = self._metrics["rss"], self._metrics["tmp"]
        if rss >= self.cfg.memory_bytes:
            return False, "memory"
        # 临时盘至少要放得下一个解压工作目录（按压缩大小的保守估计 + 余量）
        if tmp + max(blob_size, 1024) >= self.cfg.temp_disk_bytes:
            return False, "disk"
        return True, ""

    # ------------------------------------------------------------------ #
    # 主循环
    # ------------------------------------------------------------------ #
    def run(self) -> None:
        signal.signal(signal.SIGTERM, self.request_stop)
        signal.signal(signal.SIGINT, self.request_stop)
        sampler = threading.Thread(target=self._sampler, daemon=True)
        sampler.start()
        self.conn.execute(
            "INSERT INTO components(name, pid, heartbeat, status) VALUES(?,?,?,?)"
            " ON CONFLICT(name) DO UPDATE SET pid=excluded.pid,"
            " heartbeat=excluded.heartbeat, status=excluded.status",
            (COMPONENT, os.getpid(), time.time(), "running"),
        )
        # 启动即接管上次进程（可能是被 SIGKILL）遗留下的处理中租约
        recovered = self.recover_stale_leases()
        if recovered:
            audit(self.conn, COMPONENT, "startup_lease_sweep",
                 {"recovered": recovered})
        while not self._stop:
            heartbeat(self.conn, COMPONENT)
            try:
                recovered = self.recover_stale_leases()
                packet = self.pick_next_packet()
                if packet is None:
                    time.sleep(self.cfg.poll_interval)
                    continue
                ok, reason = self._capacity_ok(packet["blob_size"])
                if not ok:
                    with tx(self.conn):
                        audit(self.conn, COMPONENT, "capacity_backoff",
                              {"reason": reason, "packet_id": packet["id"]},
                              tenant_id=packet["tenant_id"],
                              packet_id=packet["id"])
                    time.sleep(self.cfg.backoff_seconds)
                    continue
                self._process_packet(packet, recovered)
            except Exception as exc:  # 单包异常不能杀死处理器
                with tx(self.conn):
                    audit(self.conn, COMPONENT, "processor_loop_error",
                          {"error": repr(exc)})
                time.sleep(self.cfg.backoff_seconds)
        self.conn.execute(
            "UPDATE components SET status='stopped', heartbeat=? WHERE name=?",
            (time.time(), COMPONENT),
        )
        self.conn.close()

    # ------------------------------------------------------------------ #
    # 单包处理
    # ------------------------------------------------------------------ #
    def _acquire_lease(self, packet) -> bool:
        with tx(self.conn):
            # 条件更新防止与租约恢复 / 其他处理器竞争
            cur = self.conn.execute(
                "SELECT status, leased_at FROM packets WHERE id=?",
                (packet["id"],),
            ).fetchone()
            if cur is None or cur["status"] not in (ST_QUEUED, ST_PROCESSING):
                return False
            self.conn.execute(
                "UPDATE packets SET status=?, leased_by=?, leased_at=?,"
                " attempts=attempts+1, updated_at=? WHERE id=?",
                (ST_PROCESSING, f"{COMPONENT}:{os.getpid()}", time.time(),
                 time.time(), packet["id"]),
            )
        return True

    def _process_packet(self, packet, recovered: int) -> None:
        if not self._acquire_lease(packet):
            return
        work_dir = os.path.join(self.cfg.tmp_dir, packet["id"])
        os.makedirs(work_dir, exist_ok=True)
        try:
            staged, uncompressed = self._extract_and_stage(packet, work_dir)
        except ExtractError as err:
            shutil.rmtree(work_dir, ignore_errors=True)
            self._quarantine(packet, err.code, err.detail)
            return
        except FileNotFoundError as err:
            shutil.rmtree(work_dir, ignore_errors=True)
            self._quarantine(packet, ERR_BLOB_MISSING, str(err))
            return
        # 成功：清理临时目录、转 ready，消息已在同一事务内落库
        shutil.rmtree(work_dir, ignore_errors=True)
        with tx(self.conn):
            self.conn.execute(
                "UPDATE packets SET status=?, uncompressed_size=?,"
                " msg_count=?, leased_by=NULL, leased_at=NULL,"
                " updated_at=? WHERE id=?",
                (ST_READY, uncompressed, staged, time.time(), packet["id"]),
            )
            audit(self.conn, COMPONENT, "extracted",
                  {"packet_id": packet["id"], "messages": staged,
                   "uncompressed_bytes": uncompressed,
                   "lease_recovered_this_tick": recovered},
                  tenant_id=packet["tenant_id"], packet_id=packet["id"])

    def _extract_and_stage(self, packet, work_dir: str) -> tuple[int, int]:
        """流式解压到临时盘，逐行校验并在一个事务里 stage 全部消息。"""
        blob = packet["blob_path"]
        if not os.path.exists(blob):
            raise FileNotFoundError(blob)
        raw_path = os.path.join(work_dir, "data.jsonl")
        total = 0
        msg_count = 0
        try:
            with gzip.open(blob, "rb") as gz, open(raw_path, "wb") as out:
                while True:
                    chunk = gz.read(64 * 1024)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > self.cfg.max_uncompressed_bytes:
                        raise ExtractError(
                            ERR_UNCOMPRESSED_LIMIT,
                            f"解压后 {total}B 超过上限"
                            f" {self.cfg.max_uncompressed_bytes}B，已中止解压")
                    # 临时盘额度在写入侧兜底
                    if dir_size(self.cfg.tmp_dir) >= self.cfg.temp_disk_bytes:
                        raise ExtractError(
                            ERR_DISK_QUOTA,
                            f"临时盘达到上限 {self.cfg.temp_disk_bytes}B")
                    out.write(chunk)
        except ExtractError:
            raise
        except (OSError, EOFError, gzip.BadGzipFile) as err:
            raise ExtractError(ERR_FORMAT_CORRUPT,
                               f"gzip 流损坏/截断: {err!r}")
        except Exception as err:  # zlib.error 等解压异常统一归为格式损坏
            raise ExtractError(ERR_FORMAT_CORRUPT, f"解压异常: {err!r}")

        # 逐行解析 JSONL
        staged_rows: list[tuple] = []
        base_seq = self._first_message_seq(packet["tenant_id"], packet["id"])
        try:
            with open(raw_path, "r", encoding="utf-8") as fh:
                for line_no, line in enumerate(fh, start=1):
                    if not line.strip():
                        raise ExtractError(
                            ERR_BAD_MESSAGE,
                            f"第 {line_no} 行为空，JSONL 不允许空行")
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError as err:
                        raise ExtractError(
                            ERR_FORMAT_CORRUPT,
                            f"第 {line_no} 行不是合法 JSON: {err}")
                    if not isinstance(obj, dict) or "id" not in obj \
                            or "payload" not in obj:
                        raise ExtractError(
                            ERR_BAD_MESSAGE,
                            f"第 {line_no} 行缺少 id/payload 字段: {line[:80]}")
                    msg_count += 1
                    if msg_count > self.cfg.max_messages_per_packet:
                        raise ExtractError(
                            ERR_TOO_MANY_MESSAGES,
                            f"消息数超过 {self.cfg.max_messages_per_packet}")
                    payload_text = json.dumps(obj["payload"],
                                              ensure_ascii=False,
                                              sort_keys=True)
                    staged_rows.append((
                        packet["tenant_id"],
                        base_seq + line_no - 1,
                        packet["id"],
                        line_no,
                        payload_text,
                        sha256_text(payload_text),
                        time.time(),
                    ))
        except UnicodeDecodeError as err:
            raise ExtractError(ERR_FORMAT_CORRUPT, f"非 UTF-8 内容: {err}")

        if msg_count == 0:
            raise ExtractError(ERR_BAD_MESSAGE, "包内没有任何消息行")

        with tx(self.conn):
            # 崩溃重解场景先清残留；UNIQUE(tenant_id, seq) 是最后一道防线
            self.conn.execute("DELETE FROM messages WHERE packet_id=?",
                              (packet["id"],))
            self.conn.executemany(
                "INSERT INTO messages(tenant_id, seq, packet_id, msg_index,"
                " payload, payload_sha, created_at) VALUES(?,?,?,?,?,?,?)",
                staged_rows,
            )
        return msg_count, total

    def _first_message_seq(self, tenant_id: str, packet_id: str) -> int:
        """消息全局序号 = 前面所有包的消息数累加，保证租户内跨包连续。"""
        row = self.conn.execute(
            "SELECT COALESCE(SUM(msg_count), 0) AS n FROM packets"
            " WHERE tenant_id=? AND seq < (SELECT seq FROM packets WHERE id=?)"
            " AND status IN (?, ?)",
            (tenant_id, packet_id, ST_READY, ST_DONE),
        ).fetchone()
        return row["n"] + 1

    def _quarantine(self, packet, code: str, detail: str) -> None:
        with tx(self.conn):
            self.conn.execute(
                "UPDATE packets SET status=?, error_code=?, error_detail=?,"
                " leased_by=NULL, leased_at=NULL, updated_at=? WHERE id=?",
                (ST_QUARANTINED, code, detail, time.time(), packet["id"]),
            )
            audit(self.conn, COMPONENT, "quarantined",
                  {"packet_id": packet["id"], "error_code": code,
                   "error_detail": detail, "seq": packet["seq"]},
                  tenant_id=packet["tenant_id"], packet_id=packet["id"])


def main(home: str) -> None:
    from .config import ensure_dirs, load_config
    cfg = load_config(home)
    ensure_dirs(cfg)
    init_db(cfg.db_path)
    Processor(cfg).run()
