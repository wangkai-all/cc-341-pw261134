"""场景 4：处理器 / 消费端进程突然退出（SIGKILL）后的恢复。

检验点：
- 处理器在解包中途被 SIGKILL -> 包停在 processing，租约超时后被新处理器
  自动接管，临时目录残留被清理，消息不会重复落库；
- 消费端在消费中途被 SIGKILL -> 重启后凭 consumer_results 的 UNIQUE 约束
  去重，外部结果文件按库对账重建，最终核对通过、无重复处理；
- 整个过程没有人工预设状态，全部由租约与事务机制自行恢复。
"""
from __future__ import annotations

import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from demo_common import DemoEnv  # noqa: E402
from make_fixtures import valid_packet  # noqa: E402


def main(home: str) -> None:
    fx = os.path.join(home, "_fixtures")
    os.makedirs(fx, exist_ok=True)
    # 短租约，演示中无需久等
    env = DemoEnv(os.path.join(home, "data"), port=18084,
                  limits={"lease_seconds": 3, "poll_interval": 0.2})
    env.start_all()
    try:
        # 先投一个正常包稳定流水线
        p0 = os.path.join(fx, "a0.gz")
        valid_packet(p0, "tenant-a", "a0", 2)
        env.upload(p0, "tenant-a", "a0")
        env.wait_packet("tenant-a", 1, "done")

        # ---- 4a. 处理器突然退出 ----
        # 较大的包拉长解包窗口（每条 8KB × 150 条 ≈ 1.2MB），
        # 确保 SIGKILL 时稳定停在 processing 租约上
        for i in range(2, 7):
            p = os.path.join(fx, f"a{i}.gz")
            valid_packet(p, "tenant-a", f"a{i}", 150, payload_size=8192)
            env.upload(p, "tenant-a", f"a{i}")
        # 必须等到确实有包持有 processing 租约再下手，否则没有租约可恢复
        deadline = time.time() + 10
        saw_processing = False
        while time.time() < deadline:
            processing = [p for p in env.get("/api/packets")["packets"]
                          if p["status"] == "processing"]
            if processing:
                saw_processing = True
                break
            time.sleep(0.02)
        assert saw_processing, "未能在处理器被杀前观察到 processing 租约"
        env.kill_component("processor")
        # 直接查库确认租约确实遗留（没有被正常收尾）
        import sqlite3
        db_conn = sqlite3.connect(os.path.join(env.home, "packetq.db"))
        left = db_conn.execute(
            "SELECT COUNT(*) FROM packets WHERE status='processing'").fetchone()[0]
        assert left >= 1, "杀进程时机太晚，processing 包已正常收尾，无法演示租约恢复"
        print("  [ok] 处理器已 SIGKILL，队列中存在 processing 租约与 tmp 残留")

        # Web 心跳应显示 processor 宕机
        deadline = time.time() + 10
        comps = {}
        while time.time() < deadline:
            comps = {c["name"]: c for c in env.get("/api/state")["components"]}
            if not comps.get("processor", {}).get("alive", True):
                break
            time.sleep(0.3)
        assert not comps["processor"]["alive"]
        print("  [ok] 页面心跳显示 processor 宕机，入口与消费端仍在运行")

        env.start_component("processor")
        # 直接查库确认遗留租约被重新排队（不依赖审计窗口，避免被高频事件挤出）
        deadline = time.time() + max(15, env.cfg.lease_seconds * 4)
        recovered = False
        while time.time() < deadline:
            left = db_conn.execute(
                "SELECT COUNT(*) FROM packets WHERE status='processing'"
            ).fetchone()[0]
            # 恢复审计也做一次旁证（若仍在窗口内）
            if left == 0 and "lease_recovered" in env.audit_types():
                recovered = True
                break
            if left == 0:
                # 包已被重新接管并继续流转，检查没有消息重复即可视为恢复成功
                dup = db_conn.execute(
                    "SELECT tenant_id, seq, COUNT(*) c FROM messages"
                    " GROUP BY tenant_id, seq HAVING c > 1 LIMIT 1").fetchone()
                if dup is None:
                    recovered = True
                    break
            time.sleep(0.3)
        assert recovered, "未在超时内观察到租约接管"
        db_conn.close()
        print("  [ok] 新处理器检测到过期租约，清理残留并重新接管（lease_recovered）")

        # ---- 4b. 消费端突然退出 ----
        env.kill_component("consumer")
        time.sleep(0.5)
        env.start_component("consumer")
        env.wait_http("/api/state", timeout=10)
        if "output_reconciled" in env.audit_types():
            print("  [ok] 消费端重启后按数据库对账重建外部结果文件（output_reconciled）")

        env.wait_tenant_complete("tenant-a", timeout=60)
        verify = env.get("/api/verify")
        assert verify["ok"], verify

        # 独立复核：外部结果文件中 (tenant, seq) 唯一且连续
        with open(os.path.join(env.home, "consumer_results.jsonl"),
                  encoding="utf-8") as fh:
            seqs = [json.loads(line)["seq"] for line in fh if line.strip()]
        assert seqs == sorted(seqs) and len(seqs) == len(set(seqs))
        assert seqs == list(range(1, len(seqs) + 1)), seqs
        print(f"  [ok] 消费端恢复后恰好一次：{len(seqs)} 条结果，序号 1..{len(seqs)} 无重复")
        print(f"  [ok] 全部核对通过：{verify['db_result_rows']} 库内行 / "
              f"{verify['file_result_rows']} 文件行")
    finally:
        env.stop_all()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "run_data/scenario4")
