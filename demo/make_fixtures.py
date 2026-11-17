"""演示 / 测试夹具生成器：把各种压缩包写到指定目录。

每个函数只负责"造数据"，不触碰运行库，因此可以安全地在空数据目录上重建场景。
用法:
  python3 -I demo/make_fixtures.py <out_dir>
"""
from __future__ import annotations

import gzip
import json
import os
import sys
import zlib

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def valid_packet(path: str, tenant: str, uid: str, n: int = 3,
                 payload_prefix: str = "msg", payload_size: int = 0) -> None:
    lines = [json.dumps({"id": f"{uid}-{i}",
                         "payload": (f"{payload_prefix}-{i}"
                                     if not payload_size
                                     else "X" * payload_size)},
                        ensure_ascii=False)
             for i in range(1, n + 1)]
    with gzip.open(path, "wb") as gz:
        gz.write(("\n".join(lines) + "\n").encode("utf-8"))


def bomb_packet(path: str, tenant: str, uid: str,
                uncompressed_target: int = 3 * 1024 * 1024) -> None:
    """高压缩比解压炸弹：gzip 后很小，解压后远超单包上限。"""
    chunk = json.dumps({"id": "bomb", "payload": "A" * 4096}) + "\n"
    repeats = uncompressed_target // len(chunk) + 1
    with gzip.open(path, "wb", compresslevel=9) as gz:
        for _ in range(repeats):
            gz.write(chunk.encode("utf-8"))


def truncated_gzip(path: str, tenant: str, uid: str) -> None:
    """gzip 流被截断：解压缩到一半 EOF，报 FORMAT_CORRUPT。"""
    buf = gzip.compress(
        ("\n".join(json.dumps({"id": f"t-{i}", "payload": i})
                   for i in range(200)) + "\n").encode("utf-8"))
    with open(path, "wb") as fh:
        fh.write(buf[: len(buf) // 2])  # 故意砍掉后半段


def bad_jsonl_packet(path: str, tenant: str, uid: str) -> None:
    """gzip 完好但内含非法 JSON 行：报 FORMAT_CORRUPT 并指出行号。"""
    text = "\n".join([
        json.dumps({"id": "ok-1", "payload": "good"}),
        "{this is : not json,,,",
        json.dumps({"id": "ok-2", "payload": "good2"}),
    ]) + "\n"
    with gzip.open(path, "wb") as gz:
        gz.write(text.encode("utf-8"))


def fixed_packet(path: str, tenant: str, uid: str, n: int = 2) -> None:
    """隔离包的修正版（内容与原包不同，用于证明重放的是新数据）。"""
    valid_packet(path, tenant, uid, n=n, payload_prefix="fixed")


def build_all(out_dir: str) -> dict:
    os.makedirs(out_dir, exist_ok=True)
    made: dict[str, str] = {}

    def put(name: str, fn, *args) -> str:
        p = os.path.join(out_dir, name)
        fn(p, *args)
        made[name] = p
        return p

    # 三个租户的正常流量
    put("tenant-a__a1.gz", valid_packet, "tenant-a", "a1", 3)
    put("tenant-a__a2.gz", valid_packet, "tenant-a", "a2", 2)
    put("tenant-b__b1.gz", valid_packet, "tenant-b", "b1", 4)
    put("tenant-b__b2.gz", valid_packet, "tenant-b", "b2", 2)
    put("tenant-c__c1.gz", valid_packet, "tenant-c", "c1", 5)
    # 场景 1：解压超限（tenant-a 的第三个包）
    put("tenant-a__a3-bomb.gz", bomb_packet, "tenant-a", "a3-bomb")
    # 场景 2a：gzip 截断（tenant-b 的第三个包）
    put("tenant-b__b3-trunc.gz", truncated_gzip, "tenant-b", "b3-trunc")
    # 场景 2b：JSON 行损坏（tenant-c 的第二个包）
    put("tenant-c__c2-badjson.gz", bad_jsonl_packet, "tenant-c", "c2-badjson")
    # 修正包（回放时再改名成 replay__<packet_id>__...）
    put("FIX-tenant-a__a3fix.gz", fixed_packet, "tenant-a", "a3fix", 2)
    put("FIX-tenant-b__b3fix.gz", fixed_packet, "tenant-b", "b3fix", 2)
    put("FIX-tenant-c__c2fix.gz", fixed_packet, "tenant-c", "c2fix", 3)
    # 场景 3 用：回放进行中到达的新普通包
    put("tenant-a__a4-late.gz", valid_packet, "tenant-a", "a4-late", 2)
    return made


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "fixtures"
    info = build_all(target)
    for name, path in sorted(info.items()):
        print(f"{name:34s} {os.path.getsize(path):8d}B  {path}")
    print(f"\n{len(info)} fixtures ready in {os.path.abspath(target)}")
