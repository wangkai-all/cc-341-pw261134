"""跨组件共用的无状态工具函数。"""
from __future__ import annotations

import hashlib
import os
import re
import resource
import time

TENANT_RE = re.compile(r"^[A-Za-z0-9_.-]{1,32}$")
# 普通投递：<tenant>__<client_uid>.gz；回放：replay__<packet_id>__<client_uid>.gz
NAME_RE = re.compile(
    r"^(?:replay__(?P<replay>[A-Za-z0-9_-]+)__)?(?P<tenant>[A-Za-z0-9_.-]{1,32})__(?P<uid>[A-Za-z0-9_.-]{1,64})\.gz$"
)


def now_iso(ts: float | None = None) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts if ts is not None else time.time()))


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def dir_size(path: str) -> int:
    total = 0
    if not os.path.isdir(path):
        return 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            fp = os.path.join(root, name)
            try:
                total += os.path.getsize(fp)
            except OSError:
                pass
    return total


def rss_bytes() -> int:
    """当前进程常驻内存（字节）。Linux ru_maxrss 单位 KB。"""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024


def valid_tenant(tenant_id: str) -> bool:
    return bool(TENANT_RE.match(tenant_id))


def parse_spool_name(filename: str) -> dict | None:
    m = NAME_RE.match(filename)
    if not m:
        return None
    return {
        "tenant_id": m.group("tenant"),
        "client_uid": m.group("uid"),
        "replay_packet_id": m.group("replay"),
    }


def atomic_replace(src: str, dst: str) -> None:
    """同文件系统内原子改名，spool 投递 / 拒收移动都走它，杜绝半文件。"""
    os.replace(src, dst)


def write_attempt(path: str, data: bytes) -> None:
    """先写 .tmp 再原子改名，扫描端只会看到完整文件。"""
    tmp = path + ".tmp"
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
