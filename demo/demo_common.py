"""演示脚本公共助手：真实启动三个独立进程 + Web，全部走 HTTP / 文件系统交互。"""
from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
RUN = os.path.join(ROOT, "bin", "run.py")


class DemoEnv:
    def __init__(self, home: str, port: int = 18080,
                 limits: dict | None = None):
        self.home = os.path.abspath(home)
        self.port = port
        self.procs: list[subprocess.Popen] = []
        self.logs: dict[str, object] = {}
        self._fresh_home(limits or {})

    @property
    def cfg(self):
        from packetq.config import load_config
        return load_config(self.home)

    def _fresh_home(self, limits: dict) -> None:
        if os.path.exists(self.home):
            shutil.rmtree(self.home)
        os.makedirs(self.home)
        if limits:
            with open(os.path.join(self.home, "limits.json"), "w",
                      encoding="utf-8") as fh:
                json.dump(limits, fh, indent=2)
        subprocess.run([sys.executable, "-I", RUN, "init", self.home],
                       check=True, cwd=ROOT)

    @property
    def spool(self) -> str:
        return os.path.join(self.home, "spool")

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start_component(self, name: str) -> None:
        log = open(os.path.join(self.home, f"{name}.log"), "a",
                   encoding="utf-8")
        cmd = [sys.executable, "-I", RUN, name, self.home]
        if name == "web":
            cmd.append(str(self.port))
        proc = subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=log,
                                preexec_fn=os.setsid)
        self.procs.append(proc)
        self.logs[name] = log
        print(f"  [start] {name} pid={proc.pid}")

    def start_all(self, with_web: bool = True) -> None:
        for name in ("ingress", "processor", "consumer"):
            self.start_component(name)
        if with_web:
            self.start_component("web")
        self.wait_http("/api/state", timeout=10)

    def stop_component(self, name: str) -> None:
        for proc in list(self.procs):
            pass
        target = None
        for proc in self.procs:
            try:
                cmdline = open(f"/proc/{proc.pid}/cmdline", "rb").read()
            except OSError:
                continue
            if f" {name} ".encode() in cmdline.replace(b"\x00", b" "):
                target = proc
                break
        if target:
            os.killpg(os.getpgid(target.pid), signal.SIGTERM)
            target.wait(timeout=10)
            self.procs.remove(target)
            print(f"  [stop]  {name} pid={target.pid}")

    def kill_component(self, name: str) -> None:
        """SIGKILL：模拟进程突然退出，不给清理机会。"""
        for proc in list(self.procs):
            try:
                cmdline = open(f"/proc/{proc.pid}/cmdline", "rb").read()
            except OSError:
                continue
            if f" {name} ".encode() in cmdline.replace(b"\x00", b" "):
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                proc.wait(timeout=10)
                self.procs.remove(proc)
                print(f"  [kill]  {name} pid={proc.pid} (SIGKILL 突然退出)")
                return
        raise RuntimeError(f"component {name} not found")

    def stop_all(self) -> None:
        for proc in self.procs:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except ProcessLookupError:
                pass
        for proc in self.procs:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        for log in self.logs.values():
            if hasattr(log, "close"):
                log.close()
        self.procs.clear()

    # ----------------------- HTTP / 轮询 ----------------------- #
    def http(self, method: str, path: str, data: bytes | None = None,
             headers: dict | None = None, timeout: int = 15):
        req = urllib.request.Request(self.base + path, data=data,
                                     headers=headers or {}, method=method)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def wait_http(self, path: str, timeout: float = 20) -> dict:
        deadline = time.time() + timeout
        last_err = None
        while time.time() < deadline:
            try:
                return self.http("GET", path)
            except (urllib.error.URLError, ConnectionError) as exc:
                last_err = exc
                time.sleep(0.25)
        raise RuntimeError(f"HTTP {path} 未就绪: {last_err}")

    def get(self, path: str) -> dict:
        return self.http("GET", path)

    def upload(self, file_path: str, tenant: str, uid: str) -> dict:
        with open(file_path, "rb") as fh:
            return self.http("POST", "/api/upload", fh.read(), {
                "Content-Type": "application/gzip",
                "X-Tenant-Id": tenant, "X-Client-Uid": uid})

    def wait_packet(self, tenant: str, seq: int, status: str,
                    timeout: float = 20) -> dict:
        deadline = time.time() + timeout
        while time.time() < deadline:
            packets = self.get("/api/packets")["packets"]
            for p in packets:
                if p["tenant_id"] == tenant and p["seq"] == seq:
                    if p["status"] == status:
                        return p
            time.sleep(0.3)
        raise AssertionError(
            f"等待 {tenant}#{seq} 进入 {status} 超时；当前: "
            + json.dumps([(p["tenant_id"], p["seq"], p["status"])
                          for p in self.get("/api/packets")["packets"]],
                         ensure_ascii=False))

    def wait_tenant_complete(self, tenant: str, timeout: float = 25) -> dict:
        deadline = time.time() + timeout
        while time.time() < deadline:
            for t in self.get("/api/state")["tenants"]:
                if t["tenant_id"] == tenant and t["is_complete"]:
                    return t
            time.sleep(0.3)
        raise AssertionError(f"租户 {tenant} 未在超时内完整")

    def drop_spool(self, src: str, name: str) -> str:
        """模拟直接落到 spool 目录的投递（不经过 Web）。"""
        dest = os.path.join(self.spool, name)
        shutil.copyfile(src, dest)
        return dest

    def audit_types(self) -> list[str]:
        return [e["event_type"] for e in
                self.get("/api/audit?limit=200")["events"]]
