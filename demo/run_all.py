"""一键演示：在全新空目录上依次跑四个场景，每个场景独立数据目录。

用法: python3 -I demo/run_all.py [run_root]
"""
from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import scenario1_overlimit  # noqa: E402
import scenario2_corrupt  # noqa: E402
import scenario3_replay_arrival  # noqa: E402
import scenario4_crash  # noqa: E402
from make_fixtures import build_all  # noqa: E402

SCENARIOS = [
    ("场景1 解压超限 / 隔离不堵全队 / 修正回放原位接回",
     scenario1_overlimit.main),
    ("场景2 格式损坏（截断 gzip + 坏 JSON 行）/ 确认跳号留缺口",
     scenario2_corrupt.main),
    ("场景3 回放进行中并发新包到达 / 租户内顺序不乱",
     scenario3_replay_arrival.main),
    ("场景4 处理器与消费端突然退出 / 租约接管与恰好一次",
     scenario4_crash.main),
]


def main(root: str) -> None:
    root = os.path.abspath(root)
    os.makedirs(root, exist_ok=True)
    build_all(os.path.join(root, "shared_fixtures"))
    print(f"演示根目录（全新空目录起步）: {root}\n")
    failures = []
    for idx, (title, fn) in enumerate(SCENARIOS, start=1):
        home = os.path.join(root, f"scenario{idx}")
        print(f"=== {title} ===")
        t0 = time.time()
        try:
            fn(home)
        except Exception as exc:
            failures.append((title, exc))
            print(f"  [FAIL] {exc!r}")
        print(f"  --- 用时 {time.time() - t0:.1f}s\n")
    if failures:
        print("失败场景:")
        for title, exc in failures:
            print(f"  - {title}: {exc!r}")
        raise SystemExit(1)
    print("全部四个场景在空目录上端到端通过。")
    print(f"可任选一个数据目录启动控制台查看: "
          f"python3 -I bin/run.py web {os.path.join(root, 'scenario1', 'data')} 18080")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "run_data/demo_all")
