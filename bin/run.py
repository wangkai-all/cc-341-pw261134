#!/usr/bin/env python3
"""组件启动入口（配合 python3 -I 运行：隔离环境变量与 CWD 的注入风险）。

用法:
  python3 -I bin/run.py init     <data_home>
  python3 -I bin/run.py ingress  <data_home>
  python3 -I bin/run.py processor <data_home>
  python3 -I bin/run.py consumer <data_home>
  python3 -I bin/run.py web      <data_home> [port]
"""
import os
import sys

# -I 模式下 cwd 与 PYTHONPATH 均被忽略，必须按文件位置显式加入仓库根
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_REPO_ROOT, "src"))
sys.path.insert(0, _REPO_ROOT)

from packetq.config import ensure_dirs, load_config  # noqa: E402
from packetq.db import init_db  # noqa: E402


def main() -> int:
    if len(sys.argv) < 3:
        print(__doc__)
        return 2
    command, home = sys.argv[1], sys.argv[2]
    if command == "init":
        cfg = load_config(home)
        ensure_dirs(cfg)
        init_db(cfg.db_path)
        print(f"initialized data home: {home}")
        print(f"  db={cfg.db_path}\n  spool={cfg.spool_dir}\n"
              f"  tmp={cfg.tmp_dir}\n  rejected={cfg.reject_dir}")
        return 0

    if command == "ingress":
        from packetq.ingress import main as run
        run(home)
        return 0
    if command == "processor":
        from packetq.processor import main as run
        run(home)
        return 0
    if command == "consumer":
        from packetq.consumer import main as run
        run(home)
        return 0
    if command == "web":
        from packetq.webapp import serve
        port = int(sys.argv[3]) if len(sys.argv) > 3 else 8080
        serve(home, port)
        return 0
    print(f"unknown command: {command}\n{__doc__}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
