"""Web 控制台（独立进程，标准库 http.server，无第三方依赖）。

只读视图：组件存活、额度占用、租户缺口与隔离原因、包明细、审计、消费者核对。
处置动作不直接改库，而是：
- 上传/回放：把文件原子写入 spool/，由入口组件按正常流程登记；
- 重试/跳号：写 control_requests，由入口组件事务化执行。
这样三个处理组件的职责边界在 Web 操作下依然成立。
"""
from __future__ import annotations

import json
import os
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from . import api
from .common import write_attempt
from .config import Config
from .db import connect, init_db

COMPONENT = "web"


class WebState:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.db_lock = threading.Lock()


def make_handler(state: WebState):
    cfg = state.cfg

    class Handler(BaseHTTPRequestHandler):
        server_version = "PacketQ/1.0"

        def log_message(self, fmt, *args):  # 精简日志
            print(f"[web] {self.address_string()} {fmt % args}")

        # ------------------------- 基础工具 ------------------------- #
        def _send_json(self, obj, status: int = 200) -> None:
            body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _read_body(self) -> bytes:
            length = int(self.headers.get("Content-Length", 0))
            if length > cfg.max_blob_bytes:
                raise ValueError(f"请求体 {length}B 超过上传上限")
            return self.rfile.read(length) if length else b""

        def _db(self):
            # SQLite WAL 支持并发读；写动作仅短事务，加进程锁串行化
            return connect(cfg.db_path)

        # ------------------------- GET 路由 ------------------------- #
        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            path, qs = parsed.path, parse_qs(parsed.query)
            try:
                if path == "/" or path == "/index.html":
                    self._serve_index()
                elif path == "/api/state":
                    with state.db_lock, self._db() as conn:
                        self._send_json({
                            "components": api.component_health(
                                conn, cfg.heartbeat_timeout),
                            "quota": api.quota_view(conn, cfg),
                            "tenants": api.tenant_view(conn),
                        })
                elif path == "/api/packets":
                    status = qs.get("status", [None])[0]
                    with state.db_lock, self._db() as conn:
                        self._send_json(
                            {"packets": api.packet_view(conn, status)})
                elif path == "/api/audit":
                    limit = int(qs.get("limit", ["50"])[0])
                    with state.db_lock, self._db() as conn:
                        self._send_json(
                            {"events": api.audit_tail(conn, min(limit, 500))})
                elif path == "/api/verify":
                    with state.db_lock, self._db() as conn:
                        self._send_json(api.verify_consumer(conn, cfg))
                elif path == "/api/limits":
                    self._send_json({
                        "memory_bytes": cfg.memory_bytes,
                        "temp_disk_bytes": cfg.temp_disk_bytes,
                        "max_uncompressed_bytes": cfg.max_uncompressed_bytes,
                        "max_messages_per_packet": cfg.max_messages_per_packet,
                        "max_blob_bytes": cfg.max_blob_bytes,
                        "lease_seconds": cfg.lease_seconds,
                    })
                else:
                    self._send_json({"error": "not found"}, 404)
            except Exception as exc:
                self._send_json({"error": repr(exc)}, 500)

        def _serve_index(self) -> None:
            page = os.path.join(os.path.dirname(__file__), "static",
                                "index.html")
            with open(page, "rb") as fh:
                body = fh.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        # ------------------------- POST 路由 ------------------------- #
        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path
            try:
                if path == "/api/upload":
                    self._handle_upload()
                elif path.startswith("/api/packets/") and \
                        path.endswith("/retry"):
                    self._handle_retry(path)
                elif path.startswith("/api/packets/") and \
                        path.endswith("/discard"):
                    self._handle_discard(path)
                else:
                    self._send_json({"error": "not found"}, 404)
            except ValueError as exc:
                self._send_json({"error": str(exc)}, 400)
            except Exception as exc:
                self._send_json({"error": repr(exc)}, 500)

        def _handle_upload(self) -> None:
            tenant = self.headers.get("X-Tenant-Id", "").strip()
            uid = self.headers.get("X-Client-Uid", "").strip() or uuid.uuid4().hex
            if not tenant or not all(
                    c.isalnum() or c in "_.-" for c in tenant):
                raise ValueError("X-Tenant-Id 缺失或含非法字符")
            data = self._read_body()
            if not data:
                raise ValueError("上传体为空")
            name = f"{tenant}__{uid}.gz"
            dest = os.path.join(cfg.spool_dir, name)
            if os.path.exists(dest):
                raise ValueError(f"spool 中已存在同名文件 {name}，请更换 UID")
            write_attempt(dest, data)
            self._send_json({"ok": True, "spooled_as": name,
                             "note": "已写入 spool，等待入口登记"}, 202)

        def _handle_retry(self, path: str) -> None:
            packet_id = path.split("/")[3]
            data = self._read_body()
            new_blob = None
            if data:
                name = f"replay__{packet_id}__web-{uuid.uuid4().hex}.gz"
                new_blob = os.path.join(cfg.spool_dir, name)
                write_attempt(new_blob, data)
            with state.db_lock, self._db() as conn:
                packet = conn.execute(
                    "SELECT tenant_id, status FROM packets WHERE id=?",
                    (packet_id,)).fetchone()
                if packet is None:
                    raise ValueError("packet 不存在")
                if packet["status"] != "quarantined":
                    raise ValueError(
                        f"仅隔离态可回放，当前状态 {packet['status']}")
                conn.execute(
                    "INSERT INTO control_requests(ts, request_type, packet_id,"
                    " new_blob, status) VALUES(?,?,?,?,'pending')",
                    (time.time(), "retry", packet_id, new_blob))
            self._send_json({"ok": True, "action": "retry",
                             "replacement_uploaded": bool(data),
                             "note": "回放请求已排队，入口将在原 seq 位置复位"},
                            202)

        def _handle_discard(self, path: str) -> None:
            packet_id = path.split("/")[3]
            with state.db_lock, self._db() as conn:
                packet = conn.execute(
                    "SELECT tenant_id, status FROM packets WHERE id=?",
                    (packet_id,)).fetchone()
                if packet is None:
                    raise ValueError("packet 不存在")
                if packet["status"] != "quarantined":
                    raise ValueError("仅隔离态包可确认跳号")
                conn.execute(
                    "INSERT INTO control_requests(ts, request_type, packet_id,"
                    " status) VALUES(?,?,?,'pending')",
                    (time.time(), "discard", packet_id))
            self._send_json({"ok": True, "action": "discard",
                             "note": "入口将把该位置标记为永久缺口"}, 202)

    return Handler


def serve(home: str, port: int = 8080) -> None:
    from .config import ensure_dirs, load_config
    cfg = load_config(home)
    ensure_dirs(cfg)
    init_db(cfg.db_path)
    state = WebState(cfg)
    httpd = ThreadingHTTPServer(("0.0.0.0", port), make_handler(state))
    print(f"[web] 控制台: http://127.0.0.1:{port}/  数据目录: {home}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
