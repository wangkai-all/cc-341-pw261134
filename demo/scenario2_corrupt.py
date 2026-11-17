"""场景 2：格式损坏的两种形态 + 人工确认跳号。

- tenant-b 的 gzip 流被截断 -> FORMAT_CORRUPT，修正包回放原位恢复；
- tenant-c 包内出现非法 JSON 行 -> FORMAT_CORRUPT（带行号），
  人工"确认跳号"后该 seq 成为永久缺口（页面持续展示），后续包继续流动；
- 任何路径都不会把含缺口的租户宣称为完整。
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from demo_common import DemoEnv  # noqa: E402
from make_fixtures import (bad_jsonl_packet, fixed_packet,  # noqa: E402
                           truncated_gzip, valid_packet)


def main(home: str) -> None:
    fx = os.path.join(home, "_fixtures")
    os.makedirs(fx, exist_ok=True)
    env = DemoEnv(os.path.join(home, "data"), port=18082)
    env.start_all()
    try:
        # tenant-b: 正常包 + 截断 gzip
        b1 = os.path.join(fx, "b1.gz")
        valid_packet(b1, "tenant-b", "b1", 3)
        env.upload(b1, "tenant-b", "b1")
        env.wait_packet("tenant-b", 1, "done")

        trunc = os.path.join(fx, "b3.gz")
        truncated_gzip(trunc, "tenant-b", "b3-trunc")
        env.upload(trunc, "tenant-b", "b3-trunc")
        bad_b = env.wait_packet("tenant-b", 2, "quarantined")
        assert bad_b["error_code"] == "FORMAT_CORRUPT"
        print(f"  [ok] tenant-b#2 截断 gzip 隔离: {bad_b['error_detail']}")

        # tenant-c: 正常包 + 坏 JSON 行
        c1 = os.path.join(fx, "c1.gz")
        valid_packet(c1, "tenant-c", "c1", 2)
        env.upload(c1, "tenant-c", "c1")
        env.wait_packet("tenant-c", 1, "done")
        badjson = os.path.join(fx, "c2.gz")
        bad_jsonl_packet(badjson, "tenant-c", "c2-badjson")
        env.upload(badjson, "tenant-c", "c2-badjson")
        bad_c = env.wait_packet("tenant-c", 2, "quarantined")
        assert bad_c["error_code"] == "FORMAT_CORRUPT" and "行" in bad_c["error_detail"]
        print(f"  [ok] tenant-c#2 坏 JSON 行隔离: {bad_c['error_detail']}")

        # tenant-c 确认跳号：缺口永久保留，后续 c3 继续
        env.http("POST", f"/api/packets/{bad_c['id']}/discard", None, {})
        env.wait_packet("tenant-c", 2, "dead")
        c3 = os.path.join(fx, "c3.gz")
        valid_packet(c3, "tenant-c", "c3", 2)
        env.upload(c3, "tenant-c", "c3")
        env.wait_packet("tenant-c", 3, "done")
        tenants = {t["tenant_id"]: t for t in env.get("/api/state")["tenants"]}
        tc = tenants["tenant-c"]
        assert tc["is_complete"], "dead 是已处置的终态，租户可以流动到完整"
        assert tc["has_gap"] and tc["gaps"][0]["kind"] == "dead"
        print(f"  [ok] tenant-c#2 标记跳号（永久缺口仍在页面展示），#3 正常完成")

        # tenant-b 修正回放，原 seq=2 接回
        fix = os.path.join(fx, "b3fix.gz")
        fixed_packet(fix, "tenant-b", "b3fix", n=2)
        with open(fix, "rb") as fh:
            env.http("POST", f"/api/packets/{bad_b['id']}/retry", fh.read(),
                     {"Content-Type": "application/gzip"})
        env.wait_packet("tenant-b", 2, "done")
        env.wait_tenant_complete("tenant-b")

        verify = env.get("/api/verify")
        assert verify["ok"] and verify["all_tenants_contiguous"], verify
        print("  [ok] tenant-b 回放恢复；消费者核对通过（跳号包无消息，不制造消息空洞）")
    finally:
        env.stop_all()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "run_data/scenario2")
