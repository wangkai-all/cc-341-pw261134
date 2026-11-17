"""场景 1：解压超限（解压炸弹）。

预期：
- tenant-a 第 3 个包解压后超 max_uncompressed_bytes -> 隔离 UNCOMPRESSED_LIMIT；
- 隔离只阻塞 tenant-a 自己的队头，tenant-b / tenant-c 不受影响、正常完成；
- 页面 / API 能看到额度占用、隔离原因与租户缺口；
- 修正包回放后在原 seq=3 位置接回，后续 a4 顺序不变，消费者核对通过。
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from demo_common import DemoEnv  # noqa: E402
from make_fixtures import bomb_packet, fixed_packet, valid_packet  # noqa: E402

MB = 1024 * 1024
LIMITS = {
    "memory_bytes": 256 * MB,
    "temp_disk_bytes": 8 * MB,
    "max_uncompressed_bytes": 2 * MB,  # 炸弹解压后约 3MB+，必触发
}


def main(home: str) -> None:
    fx = os.path.join(home, "_fixtures")
    os.makedirs(fx, exist_ok=True)
    env = DemoEnv(os.path.join(home, "data"), port=18081, limits=LIMITS)
    env.start_all()
    try:
        # ---- 正常流量：a1/a2, b1, c1 ----
        for name, tenant, uid, n in [
            ("a1", "tenant-a", "a1", 3), ("a2", "tenant-a", "a2", 2),
            ("b1", "tenant-b", "b1", 4), ("c1", "tenant-c", "c1", 5)]:
            p = os.path.join(fx, f"{tenant}__{name}.gz")
            valid_packet(p, tenant, uid, n)
            env.upload(p, tenant, uid)
        env.wait_tenant_complete("tenant-b")
        env.wait_tenant_complete("tenant-c")

        # ---- 投递解压炸弹：tenant-a #3 ----
        bomb = os.path.join(fx, "bomb.gz")
        bomb_packet(bomb, "tenant-a", "a3-bomb")
        env.upload(bomb, "tenant-a", "a3-bomb")
        bad = env.wait_packet("tenant-a", 3, "quarantined")
        assert bad["error_code"] == "UNCOMPRESSED_LIMIT", bad
        print(f"  [ok] tenant-a#3 已隔离: {bad['error_label']} - {bad['error_detail']}")

        # 隔离后到达的 a4 必须排队等待，不能越过 #3
        late = os.path.join(fx, "a4.gz")
        valid_packet(late, "tenant-a", "a4-late", 2)
        env.upload(late, "tenant-a", "a4-late")
        state = {t["tenant_id"]: t for t in env.get("/api/state")["tenants"]}
        assert state["tenant-a"]["blocked_at_seq"] == 3
        assert state["tenant-b"]["is_complete"] and state["tenant-c"]["is_complete"]
        print("  [ok] 隔离仅阻塞 tenant-a 队头；tenant-b/c 已完整，a4 在 #3 后等待")

        # 额度视图
        quota = env.get("/api/state")["quota"]
        print(f"  [ok] 额度视图: 内存 {quota['memory']['percent']}% ,"
              f" 临时盘 {quota['temp_disk']['percent']}% ,"
              f" 单包解压上限 {quota['uncompressed_per_packet']['limit']}B")

        # ---- 上传修正包，在原 seq=3 回放 ----
        fix = os.path.join(fx, "a3fix.gz")
        fixed_packet(fix, "tenant-a", "a3fix", n=2)
        with open(fix, "rb") as fh:
            env.http("POST", f"/api/packets/{bad['id']}/retry", fh.read(),
                     {"Content-Type": "application/gzip"})
        env.wait_packet("tenant-a", 3, "done")
        env.wait_tenant_complete("tenant-a")
        verify = env.get("/api/verify")
        assert verify["ok"], verify
        print(f"  [ok] 回放原位接回 seq=3，a4 随后完成；核对全部通过"
              f"（库内 {verify['db_result_rows']} 条 / 文件 {verify['file_result_rows']} 条）")
    finally:
        env.stop_all()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "run_data/scenario1")
