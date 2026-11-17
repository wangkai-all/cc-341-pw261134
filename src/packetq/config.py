"""集中式配置：内存 / 临时盘 / 解压额度，全部来自 limits.json。

进程启动时校验一次，后续所有组件只读同一份配置，避免各处硬编码额度。
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass

MB = 1024 * 1024

DEFAULT_LIMITS = {
    # 处理器进程常驻内存软上限（RSS），超过则拒绝继续接新包，等内存回落
    "memory_bytes": 256 * MB,
    # 临时解包目录总量上限（所有租户共享的处理器临时盘额度）
    "temp_disk_bytes": 8 * MB,
    # 单个包解压后未压缩数据上限，超限立刻中止解压并隔离该包
    "max_uncompressed_bytes": 2 * MB,
    # 单个包允许包含的最大消息行数
    "max_messages_per_packet": 5000,
    # 入口接受单个上传/spool 文件的压缩后大小上限
    "max_blob_bytes": 16 * MB,
    # 处理器处理租约：崩溃后超过该时长仍处于 processing 的包被重新接管
    "lease_seconds": 20,
    # 处理器在内存/临时盘满时的退避秒数
    "backoff_seconds": 1.0,
    # 轮询间隔
    "poll_interval": 0.25,
    # 资源采样间隔
    "sample_interval": 1.0,
    # 心跳超时：超过该时长无心跳即认为组件宕机
    "heartbeat_timeout": 8,
    # 消费端结果文件名（外部审计产物）
    "result_filename": "consumer_results.jsonl",
}


@dataclass(frozen=True)
class Config:
    home: str
    memory_bytes: int
    temp_disk_bytes: int
    max_uncompressed_bytes: int
    max_messages_per_packet: int
    max_blob_bytes: int
    lease_seconds: int
    backoff_seconds: float
    poll_interval: float
    sample_interval: float
    heartbeat_timeout: int
    result_filename: str

    @property
    def db_path(self) -> str:
        return os.path.join(self.home, "packetq.db")

    @property
    def spool_dir(self) -> str:
        return os.path.join(self.home, "spool")

    @property
    def reject_dir(self) -> str:
        return os.path.join(self.home, "rejected")

    @property
    def tmp_dir(self) -> str:
        return os.path.join(self.home, "tmp_extract")

    @property
    def result_path(self) -> str:
        return os.path.join(self.home, self.result_filename)

    @property
    def limits_json_path(self) -> str:
        return os.path.join(self.home, "limits.json")


def load_config(home: str) -> Config:
    home = os.path.abspath(home)
    overrides: dict = {}
    limits_file = os.path.join(home, "limits.json")
    if os.path.exists(limits_file):
        with open(limits_file, "r", encoding="utf-8") as fh:
            overrides = json.load(fh)
    merged = {**DEFAULT_LIMITS, **overrides}
    cfg = Config(home=home, **merged)
    for name in ("memory_bytes", "temp_disk_bytes", "max_uncompressed_bytes",
                 "max_messages_per_packet", "max_blob_bytes"):
        if getattr(cfg, name) <= 0:
            raise ValueError(f"limit {name} must be positive")
    return cfg


def ensure_dirs(cfg: Config) -> None:
    for d in (cfg.home, cfg.spool_dir, cfg.reject_dir, cfg.tmp_dir):
        os.makedirs(d, exist_ok=True)
