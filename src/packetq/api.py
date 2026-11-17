"""只读查询与核对逻辑（不依赖 HTTP 层，方便单元测试直接调用）。"""
from __future__ import annotations

import json
import os
import sqlite3
import time

from .common import dir_size, now_iso
from .config import Config
from .db import (ST_DEAD, ST_DONE, ST_PROCESSING, ST_QUARANTINED, ST_QUEUED,
                 ST_READY)

ACTIVE_PACKET_STATUSES = (ST_QUEUED, ST_PROCESSING, ST_QUARANTINED, ST_READY)
STATUS_LABELS = {
    ST_QUEUED: "排队中",
    ST_PROCESSING: "解包中",
    ST_QUARANTINED: "已隔离",
    ST_DEAD: "已跳号(缺口)",
    ST_READY: "待消费",
    ST_DONE: "已完成",
}
ERROR_LABELS = {
    "UNCOMPRESSED_LIMIT": "解压超限",
    "TEMP_DISK_QUOTA": "临时盘额度不足",
    "FORMAT_CORRUPT": "格式损坏",
    "BAD_MESSAGE": "消息内容非法",
    "TOO_MANY_MESSAGES": "消息条数超限",
    "BLOB_MISSING": "压缩包文件丢失",
}


def component_health(conn: sqlite3.Connection, heartbeat_timeout: int) -> list[dict]:
    rows = conn.execute("SELECT * FROM components ORDER BY name").fetchall()
    out = []
    for r in rows:
        fresh = r["heartbeat"] and time.time() - r["heartbeat"] <= heartbeat_timeout
        out.append({
            "name": r["name"],
            "pid": r["pid"],
            "status": r["status"],
            "last_heartbeat": now_iso(r["heartbeat"]) if r["heartbeat"] else None,
            "alive": bool(fresh) and r["status"] == "running",
        })
    return out


def quota_view(conn: sqlite3.Connection, cfg: Config) -> dict:
    latest = conn.execute(
        "SELECT * FROM resource_metrics ORDER BY id DESC LIMIT 1"
    ).fetchone()
    rss = latest["rss_bytes"] if latest else 0
    tmp = dir_size(cfg.tmp_dir)
    spool = dir_size(cfg.spool_dir)
    blobs = dir_size(os.path.join(cfg.home, "blobs"))
    rejected = dir_size(cfg.reject_dir)

    def pct(used: int, limit: int) -> float:
        return round(100.0 * used / limit, 1) if limit else 0.0

    return {
        "memory": {"used": rss, "limit": cfg.memory_bytes,
                   "percent": pct(rss, cfg.memory_bytes)},
        "temp_disk": {"used": tmp, "limit": cfg.temp_disk_bytes,
                      "percent": pct(tmp, cfg.temp_disk_bytes)},
        "uncompressed_per_packet": {"limit": cfg.max_uncompressed_bytes},
        "storage": {"spool": spool, "blobs": blobs, "rejected": rejected},
        "sampled_at": now_iso(latest["ts"]) if latest else None,
    }


def tenant_view(conn: sqlite3.Connection) -> list[dict]:
    tenants = conn.execute(
        "SELECT * FROM tenants ORDER BY tenant_id").fetchall()
    out = []
    for t in tenants:
        packets = conn.execute(
            "SELECT * FROM packets WHERE tenant_id=? ORDER BY seq",
            (t["tenant_id"],)).fetchall()
        gaps = []
        blocked = None
        counts = {s: 0 for s in STATUS_LABELS}
        for p in packets:
            counts[p["status"]] = counts.get(p["status"], 0) + 1
            if p["status"] == ST_QUARANTINED:
                gaps.append({
                    "seq": p["seq"], "packet_id": p["id"], "kind": "quarantine",
                    "error_code": p["error_code"],
                    "error_label": ERROR_LABELS.get(p["error_code"], p["error_code"]),
                    "error_detail": p["error_detail"],
                })
                if blocked is None:
                    blocked = p["seq"]
            elif p["status"] == ST_DEAD:
                gaps.append({
                    "seq": p["seq"], "packet_id": p["id"], "kind": "dead",
                    "error_code": p["error_code"],
                    "error_label": ERROR_LABELS.get(p["error_code"], p["error_code"]),
                    "error_detail": p["error_detail"],
                })
        msg_total = conn.execute(
            "SELECT COUNT(*) n, COALESCE(MAX(seq),0) max_seq FROM messages"
            " WHERE tenant_id=?", (t["tenant_id"],)).fetchone()
        consumed = conn.execute(
            "SELECT COUNT(*) n FROM consumer_results WHERE tenant_id=?",
            (t["tenant_id"],)).fetchone()["n"]
        # 完整性判定：没有任何未决包才算完整；隔离/跳号都明确暴露为缺口
        pending = sum(counts.get(s, 0) for s in ACTIVE_PACKET_STATUSES)
        complete = pending == 0 and len(packets) > 0
        out.append({
            "tenant_id": t["tenant_id"],
            "packet_last_seq": t["last_seq"],
            "packet_counts": counts,
            "messages_total": msg_total["n"],
            "messages_max_seq": msg_total["max_seq"],
            "messages_consumed": consumed,
            "blocked_at_seq": blocked,
            "gaps": gaps,
            "is_complete": complete,
            "has_gap": bool(gaps),
        })
    return out


def packet_view(conn: sqlite3.Connection, status: str | None = None) -> list[dict]:
    if status:
        rows = conn.execute(
            "SELECT * FROM packets WHERE status=? ORDER BY tenant_id, seq",
            (status,)).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM packets ORDER BY tenant_id, seq").fetchall()
    return [{
        "id": r["id"], "tenant_id": r["tenant_id"], "seq": r["seq"],
        "client_uid": r["client_uid"], "status": r["status"],
        "status_label": STATUS_LABELS.get(r["status"], r["status"]),
        "blob_size": r["blob_size"],
        "uncompressed_size": r["uncompressed_size"],
        "msg_count": r["msg_count"], "attempts": r["attempts"],
        "error_code": r["error_code"],
        "error_label": ERROR_LABELS.get(r["error_code"], r["error_code"]),
        "error_detail": r["error_detail"],
        "leased_by": r["leased_by"],
        "created_at": now_iso(r["created_at"]),
        "updated_at": now_iso(r["updated_at"]),
    } for r in rows]


def audit_tail(conn: sqlite3.Connection, limit: int = 50) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM audit_events ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return [{
        "id": r["id"], "ts": now_iso(r["ts"]), "component": r["component"],
        "event_type": r["event_type"], "tenant_id": r["tenant_id"],
        "packet_id": r["packet_id"],
        "detail": json.loads(r["detail"]),
    } for r in rows]


def verify_consumer(conn: sqlite3.Connection, cfg: Config) -> dict:
    """核对消费者结果：库内连续性 / 去重 / 与外部文件逐项比对。"""
    checks: list[dict] = []

    def add(name: str, ok: bool, detail: dict) -> None:
        checks.append({"check": name, "ok": ok, "detail": detail})

    tenants = [r["tenant_id"] for r in
               conn.execute("SELECT DISTINCT tenant_id FROM messages"
                            " ORDER BY tenant_id")]
    db_total = 0
    expected_lines: list[tuple] = []
    all_contiguous = True
    for tenant in tenants:
        seqs = [r["seq"] for r in conn.execute(
            "SELECT seq FROM messages WHERE tenant_id=? ORDER BY seq", (tenant,))]
        consumed = [r["seq"] for r in conn.execute(
            "SELECT seq FROM consumer_results WHERE tenant_id=? ORDER BY seq",
            (tenant,))]
        db_total += len(consumed)
        # 消息侧：seq 必须从 1 连续无重复
        contiguous = seqs == list(range(1, len(seqs) + 1))
        all_contiguous = all_contiguous and contiguous
        # 消费侧：必须是消息 seq 的前缀（租户内顺序消费的体现）
        prefix_ok = consumed == seqs[:len(consumed)]
        add(f"tenant:{tenant}:消息序号连续无重复", contiguous,
            {"message_count": len(seqs), "max_seq": max(seqs, default=0)})
        add(f"tenant:{tenant}:消费严格按序且无遗漏", prefix_ok,
            {"consumed": len(consumed), "total": len(seqs)})

    # 全局按 (租户, 序号) 排序取三元组，与外部结果文件的行序严格一致
    expected_lines = [tuple(r) for r in conn.execute(
        "SELECT cr.tenant_id, cr.seq, cr.output_sha FROM consumer_results cr"
        " ORDER BY cr.tenant_id, cr.seq")]

    # 去重约束由 UNIQUE 保证，这里再显式验证一次
    dup_row = conn.execute(
        "SELECT message_id, COUNT(*) n FROM consumer_results"
        " GROUP BY message_id HAVING n > 1 LIMIT 1").fetchone()
    add("全局:每条消息恰好一条消费结果(无重复影响)", dup_row is None,
        {"duplicate": dict(dup_row) if dup_row else None})

    # 外部结果文件逐项核对
    file_lines: list[dict] = []
    file_error = None
    if os.path.exists(cfg.result_path):
        try:
            with open(cfg.result_path, "r", encoding="utf-8") as fh:
                for line_no, line in enumerate(fh, 1):
                    obj = json.loads(line)
                    file_lines.append((obj["tenant_id"], obj["seq"],
                                       obj["output_sha"]))
        except Exception as exc:
            file_error = repr(exc)
    add("外部结果文件可解析", file_error is None,
        {"path": cfg.result_path, "error": file_error})
    add("外部文件与数据库结果完全一致(条数/哈希多重集)",
        sorted(file_lines) == sorted(expected_lines),
        {"db_rows": len(expected_lines), "file_rows": len(file_lines)})

    return {
        "ok": all(c["ok"] for c in checks),
        "db_result_rows": db_total,
        "file_result_rows": len(file_lines),
        "all_tenants_contiguous": all_contiguous,
        "checks": checks,
        "verified_at": now_iso(),
    }
