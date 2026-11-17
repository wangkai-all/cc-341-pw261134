"""场景 3：隔离包修正回放的同时，新包持续到达。

检验点：
- 修正包必须接回【原 seq 位置】，新到达的包排在它后面，不能插队；
- 回放复位与新包登记都由入口串行处理，序号不重不乱；
- 消费者最终看到的消息顺序严格按租户内序号递增，无重复、无遗漏。
"""
from __future__ import annotations

import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from demo_common import DemoEnv  # noqa: E402
from make_fixtures import bad_jsonl_packet, fixed_packet, valid_packet  # noqa: E402


def main(home: str) -> None:
    fx = os.path.join(home, "_fixtures")
    os.makedirs(fx, exist_ok=True)
    env = DemoEnv(os.path.join(home, "data"), port=18083)
    env.start_all()
    try:
        p1 = os.path.join(fx, "a1.gz")
        valid_packet(p1, "tenant-a", "a1", 3)
        env.upload(p1, "tenant-a", "a1")
        env.wait_packet("tenant-a", 1, "done")

        bad = os.path.join(fx, "a2bad.gz")
        bad_jsonl_packet(bad, "tenant-a", "a2-bad")
        env.upload(bad, "tenant-a", "a2-bad")
        quarantined = env.wait_packet("tenant-a", 2, "quarantined")
        print("  [ok] tenant-a#2 已隔离，准备在回放期间并发投递新包")

        # 先让 3 个新包进入队列（隔离态下它们只能排队等待），再发回放请求：
        # 稳定复现"回放进行中已有新包到达"，且新包不能越过原 seq=2
        for i in range(3, 6):
            p = os.path.join(fx, f"a{i}.gz")
            valid_packet(p, "tenant-a", f"a{i}-late", 2)
            env.upload(p, "tenant-a", f"a{i}-late")

        fix = os.path.join(fx, "a2fix.gz")
        fixed_packet(fix, "tenant-a", "a2fix", n=2)
        with open(fix, "rb") as fh:
            env.http("POST", f"/api/packets/{quarantined['id']}/retry",
                     fh.read(), {"Content-Type": "application/gzip"})
        # 回放提交后再并发追加一个新到达包，覆盖"回放处理中仍有新包"的时序
        def late_arrival() -> None:
            time.sleep(0.2)
            p = os.path.join(fx, "a6.gz")
            valid_packet(p, "tenant-a", "a6-late", 2)
            env.upload(p, "tenant-a", "a6-late")

        threading.Thread(target=late_arrival).start()

        env.wait_tenant_complete("tenant-a", timeout=30)
        packets = [p for p in env.get("/api/packets")["packets"]
                   if p["tenant_id"] == "tenant-a"]
        seqs = [(p["seq"], p["status"]) for p in sorted(packets, key=lambda x: x["seq"])]
        assert [s for s, _ in seqs] == [1, 2, 3, 4, 5, 6], seqs
        assert all(st == "done" for _, st in seqs), seqs
        print(f"  [ok] 回放接回原 seq=2，新包依次为 #3..#6，顺序: {seqs}")

        verify = env.get("/api/verify")
        assert verify["ok"], verify
        # 显式核对消费侧消息序号连续
        tenant = next(t for t in env.get("/api/state")["tenants"]
                      if t["tenant_id"] == "tenant-a")
        assert tenant["messages_consumed"] == tenant["messages_total"]
        print(f"  [ok] 共消费 {tenant['messages_consumed']} 条消息，"
              f"序号 1..{tenant['messages_max_seq']} 连续，无重复影响")
    finally:
        env.stop_all()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "run_data/scenario3")
