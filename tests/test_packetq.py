"""不依赖子进程的单元测试：在临时目录中直接驱动组件核心逻辑。

覆盖：
- 入口登记 / 去重 / 回放原位复位 / 控制请求；
- 处理器公平轮转、解压超限、格式损坏、隔离不堵其他租户、租约恢复；
- 消费端租户内顺序、恰好一次、dead 跳号后继续、verify 核对。
"""
from __future__ import annotations

import gzip
import json
import os
import shutil
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "src"))

from packetq.config import ensure_dirs, load_config
from packetq.db import (ST_DEAD, ST_DONE, ST_PROCESSING, ST_QUARANTINED,
                        ST_QUEUED, ST_READY, connect, init_db)
from packetq.ingress import Ingress
from packetq.processor import (ERR_FORMAT_CORRUPT, ERR_UNCOMPRESSED_LIMIT,
                               Processor)
from packetq.consumer import Consumer
from packetq import api

MB = 1024 * 1024


def make_packet(path: str, lines: list[str]) -> None:
    with gzip.open(path, "wb") as gz:
        gz.write(("\n".join(lines) + "\n").encode("utf-8"))


def msg(i: int, payload=None) -> str:
    return json.dumps({"id": f"m{i}", "payload": payload if payload is not None else i})


class PacketQTestBase(unittest.TestCase):
    LIMITS = {"memory_bytes": 256 * MB, "temp_disk_bytes": 8 * MB,
              "max_uncompressed_bytes": 2 * MB, "lease_seconds": 1,
              "poll_interval": 0.05, "sample_interval": 60}

    def setUp(self) -> None:
        self.home = tempfile.mkdtemp(prefix="packetq-test-")
        with open(os.path.join(self.home, "limits.json"), "w",
                  encoding="utf-8") as fh:
            json.dump(self.LIMITS, fh)
        self.cfg = load_config(self.home)
        ensure_dirs(self.cfg)
        init_db(self.cfg.db_path)
        os.makedirs(os.path.join(self.home, "blobs"), exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.home, ignore_errors=True)

    def spool(self, tenant: str, uid: str, lines: list[str],
              replay: str | None = None) -> str:
        name = (f"replay__{replay}__" if replay else "") + f"{tenant}__{uid}.gz"
        path = os.path.join(self.cfg.spool_dir, name)
        make_packet(path, lines)
        return path

    def ingest_all(self) -> Ingress:
        ing = Ingress(self.cfg)
        ing.scan_spool()
        # 两轮：第一轮登记 pending，第二轮大小稳定后入库
        time.sleep(self.cfg.poll_interval * 1.2)
        ing.scan_spool()
        return ing

    def packets(self, conn) -> list[dict]:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM packets ORDER BY tenant_id, seq")]


class TestIngress(PacketQTestBase):
    def test_seq_assignment_and_dedup(self) -> None:
        self.spool("ta", "u1", [msg(1)])
        self.spool("ta", "u2", [msg(2)])
        self.spool("tb", "u1", [msg(3)])
        ing = self.ingest_all()
        conn = ing.conn
        rows = self.packets(conn)
        self.assertEqual([(r["tenant_id"], r["seq"], r["status"]) for r in rows],
                         [("ta", 1, ST_QUEUED), ("ta", 2, ST_QUEUED),
                          ("tb", 1, ST_QUEUED)])
        # 重复 client_uid -> 拒收而不是二次入库
        self.spool("ta", "u1", [msg(9)])
        ing.scan_spool()
        time.sleep(self.cfg.poll_interval * 1.2)
        ing.scan_spool()
        self.assertEqual(conn.execute(
            "SELECT COUNT(*) n FROM packets").fetchone()["n"], 3)
        rejected = os.listdir(self.cfg.reject_dir)
        self.assertTrue(any(x.startswith("duplicate_uid") for x in rejected))
        events = [r["event_type"] for r in
                  conn.execute("SELECT event_type FROM audit_events")]
        self.assertIn("ingest_duplicate", events)

    def test_bad_filename_rejected(self) -> None:
        bad = os.path.join(self.cfg.spool_dir, "no-tenant.gz")
        make_packet(bad, [msg(1)])
        ing = self.ingest_all()
        self.assertEqual(ing.conn.execute(
            "SELECT COUNT(*) n FROM packets").fetchone()["n"], 0)
        self.assertTrue(any(x.startswith("bad_filename")
                            for x in os.listdir(self.cfg.reject_dir)))


class TestProcessorQuarantine(PacketQTestBase):
    def _prepare(self) -> tuple[Ingress, Processor]:
        self.spool("ta", "u1", [msg(1), msg(2)])
        self.spool("tb", "u1", [msg(3)])
        ing = self.ingest_all()
        proc = Processor(self.cfg)
        return ing, proc

    def test_happy_path_extract_and_stage(self) -> None:
        _ing, proc = self._prepare()
        for _ in range(2):
            pkt = proc.pick_next_packet()
            self.assertIsNotNone(pkt)
            proc._process_packet(pkt, 0)
        conn = proc.conn
        rows = self.packets(conn)
        self.assertTrue(all(r["status"] == ST_READY for r in rows))
        msgs = conn.execute(
            "SELECT tenant_id, seq, msg_index FROM messages ORDER BY tenant_id, seq"
        ).fetchall()
        self.assertEqual([(m["tenant_id"], m["seq"], m["msg_index"]) for m in msgs],
                         [("ta", 1, 1), ("ta", 2, 2), ("tb", 1, 1)])

    def test_uncompressed_bomb_quarantined_other_tenants_flow(self) -> None:
        # 高压缩比炸弹
        bomb = os.path.join(self.cfg.spool_dir, "ta__bomb.gz")
        with gzip.open(bomb, "wb", compresslevel=9) as gz:
            line = json.dumps({"id": "x", "payload": "Z" * 4096}) + "\n"
            for _ in range(600):  # ~2.5MB 未压缩
                gz.write(line.encode())
        self.spool("tb", "u1", [msg(1)])
        self.ingest_all()
        proc = Processor(self.cfg)
        # 多轮调度直到没有 queued/processing
        for _ in range(10):
            pkt = proc.pick_next_packet()
            if pkt is None:
                break
            proc._process_packet(pkt, 0)
        conn = proc.conn
        ta = conn.execute(
            "SELECT * FROM packets WHERE tenant_id='ta'").fetchone()
        tb = conn.execute(
            "SELECT * FROM packets WHERE tenant_id='tb'").fetchone()
        self.assertEqual(ta["status"], ST_QUARANTINED)
        self.assertEqual(ta["error_code"], ERR_UNCOMPRESSED_LIMIT)
        self.assertEqual(tb["status"], ST_READY)  # 坏包没堵住 tb
        # 炸弹没有任何消息落库
        self.assertEqual(conn.execute(
            "SELECT COUNT(*) n FROM messages WHERE tenant_id='ta'").fetchone()["n"], 0)

    def test_corrupt_gzip_and_bad_json(self) -> None:
        trunc = os.path.join(self.cfg.spool_dir, "ta__trunc.gz")
        raw = gzip.compress(("\n".join(msg(i) for i in range(50)) + "\n").encode())
        with open(trunc, "wb") as fh:
            fh.write(raw[: len(raw) // 2])
        self.ingest_all()
        proc = Processor(self.cfg)
        pkt = proc.pick_next_packet()
        proc._process_packet(pkt, 0)
        row = proc.conn.execute(
            "SELECT * FROM packets WHERE tenant_id='ta'").fetchone()
        self.assertEqual(row["status"], ST_QUARANTINED)
        self.assertEqual(row["error_code"], ERR_FORMAT_CORRUPT)

    def test_fair_round_robin(self) -> None:
        for t in ("t1", "t2", "t3"):
            for u in range(3):
                self.spool(t, f"u{u}", [msg(u)])
        self.ingest_all()
        proc = Processor(self.cfg)
        order = []
        for _ in range(9):
            pkt = proc.pick_next_packet()
            self.assertIsNotNone(pkt)
            order.append(pkt["tenant_id"])
            # 直接置 ready 模拟处理完成，便于继续调度
            with proc.conn:
                proc.conn.execute(
                    "UPDATE packets SET status=? WHERE id=?",
                    (ST_READY, pkt["id"]))
        # 每轮三个租户各服务一次
        self.assertEqual(sorted(order[:3]), ["t1", "t2", "t3"])
        self.assertEqual(sorted(order[3:6]), ["t1", "t2", "t3"])

    def test_stale_lease_recovery_is_idempotent(self) -> None:
        self.spool("ta", "u1", [msg(1), msg(2)])
        ing = self.ingest_all()
        proc = Processor(self.cfg)
        pkt = proc.pick_next_packet()
        self.assertTrue(proc._acquire_lease(pkt))
        # 模拟崩溃：残留临时目录 + 过期租约
        work = os.path.join(self.cfg.tmp_dir, pkt["id"])
        os.makedirs(work, exist_ok=True)
        with open(os.path.join(work, "leftover"), "w") as fh:
            fh.write("x")
        time.sleep(self.cfg.lease_seconds * 1.1)
        recovered = proc.recover_stale_leases()
        self.assertEqual(recovered, 1)
        self.assertFalse(os.path.exists(work))
        row = proc.conn.execute(
            "SELECT status FROM packets WHERE id=?", (pkt["id"],)).fetchone()
        self.assertEqual(row["status"], ST_QUEUED)
        # 重新处理成功且消息不重复
        pkt2 = proc.pick_next_packet()
        proc._process_packet(pkt2, 0)
        n = proc.conn.execute(
            "SELECT COUNT(*) n FROM messages WHERE packet_id=?",
            (pkt["id"],)).fetchone()["n"]
        self.assertEqual(n, 2)


    def test_quarantined_head_blocks_later_packets_in_same_tenant(self) -> None:
        # seq=1 坏包隔离；seq=2/3 是好包，但在 seq=1 处置前不允许越过解包
        bad = os.path.join(self.cfg.spool_dir, "ta__0-bad.gz")
        with open(bad, "wb") as fh:
            fh.write(gzip.compress(b"x")[:6])
        self.spool("ta", "1-good", [msg(1)])
        self.spool("ta", "2-good", [msg(2), msg(3)])
        self.ingest_all()
        proc = Processor(self.cfg)
        for _ in range(8):
            pkt = proc.pick_next_packet()
            if pkt is None:
                break
            proc._process_packet(pkt, 0)
        conn = proc.conn
        by_seq = {r["seq"]: r["status"] for r in conn.execute(
            "SELECT seq, status FROM packets WHERE tenant_id='ta'")}
        self.assertEqual(by_seq[1], ST_QUARANTINED)
        self.assertEqual(by_seq[2], ST_QUEUED)
        self.assertEqual(by_seq[3], ST_QUEUED)
        # 隔离期没有任何消息落库
        self.assertEqual(conn.execute(
            "SELECT COUNT(*) n FROM messages WHERE tenant_id='ta'").fetchone()["n"], 0)

        # 修正回放 seq=1 后，三个包按序解包，消息序号 1..4 连续
        head = conn.execute(
            "SELECT id FROM packets WHERE tenant_id='ta' AND seq=1").fetchone()["id"]
        self.spool("ta", "fix", [msg(7)], replay=head)
        ing = self.ingest_all()
        for _ in range(8):
            pkt = proc.pick_next_packet()
            if pkt is None:
                break
            proc._process_packet(pkt, 0)
        mseqs = [r["seq"] for r in conn.execute(
            "SELECT seq FROM messages WHERE tenant_id='ta' ORDER BY seq")]
        self.assertEqual(mseqs, [1, 2, 3, 4])


class TestReplayAndGap(PacketQTestBase):
    def _quarantine_one(self) -> tuple[str, str]:
        # 好包文件名保证先被扫描登记为 seq=1，坏包为 seq=2
        good = self.spool("ta", "0-good", [msg(1)])
        bad = os.path.join(self.cfg.spool_dir, "ta__1-bad.gz")
        raw = gzip.compress(b"not-a-gzip-at-all" * 10)
        with open(bad, "wb") as fh:
            fh.write(raw[:8])  # 伪装 gzip 头但内容损坏
        self.ingest_all()
        proc = Processor(self.cfg)
        for _ in range(5):
            pkt = proc.pick_next_packet()
            if pkt is None:
                break
            proc._process_packet(pkt, 0)
        conn = proc.conn
        bad_pkt = conn.execute(
            "SELECT * FROM packets WHERE client_uid='1-bad'").fetchone()
        self.assertEqual(bad_pkt["status"], ST_QUARANTINED)
        return bad_pkt["id"], bad_pkt["tenant_id"]

    def test_replay_requeues_at_original_seq(self) -> None:
        packet_id, tenant = self._quarantine_one()
        # 回放期间又来两个新包
        self.spool(tenant, "late1", [msg(5)])
        self.spool(tenant, "late2", [msg(6)])
        # 修正包走 spool 回放通道
        fix = self.spool(tenant, "fix1", [msg(7), msg(8)], replay=packet_id)
        ing = self.ingest_all()
        conn = ing.conn
        fixed = conn.execute("SELECT * FROM packets WHERE id=?",
                             (packet_id,)).fetchone()
        self.assertEqual(fixed["status"], ST_QUEUED)
        self.assertEqual(fixed["seq"], 2)  # 原位置
        seqs = [r["seq"] for r in conn.execute(
            "SELECT seq FROM packets WHERE tenant_id=? ORDER BY seq", (tenant,))]
        self.assertEqual(seqs, [1, 2, 3, 4])  # 回放不占新号
        # 处理器全部解包
        proc = Processor(self.cfg)
        for _ in range(10):
            pkt = proc.pick_next_packet()
            if pkt is None:
                break
            proc._process_packet(pkt, 0)
        statuses = conn.execute(
            "SELECT seq, status FROM packets WHERE tenant_id=? ORDER BY seq",
            (tenant,)).fetchall()
        self.assertTrue(all(r["status"] == ST_READY for r in statuses))
        # 修正包两条消息，消息全局序号连续
        mseqs = [r["seq"] for r in conn.execute(
            "SELECT seq FROM messages WHERE tenant_id=? ORDER BY seq", (tenant,))]
        self.assertEqual(mseqs, list(range(1, 6)))  # 1(good1) + 2(fix) + 1 + 1

    def test_discard_marks_gap_and_tenant_not_claimed_complete(self) -> None:
        packet_id, tenant = self._quarantine_one()
        conn = connect(self.cfg.db_path)
        conn.execute(
            "INSERT INTO control_requests(ts, request_type, packet_id, status)"
            " VALUES(?, 'discard', ?, 'pending')", (time.time(), packet_id))
        ing = Ingress(self.cfg)
        ing.handle_control_requests()
        row = conn.execute("SELECT status FROM packets WHERE id=?",
                           (packet_id,)).fetchone()
        self.assertEqual(row["status"], ST_DEAD)
        view = {t["tenant_id"]: t for t in api.tenant_view(conn)}
        self.assertEqual(view[tenant]["gaps"][0]["kind"], "dead")
        # 仍有 queued/quarantined 时不完整；dead 之后新包可以越过
        self.spool(tenant, "after", [msg(9)])
        ing.scan_spool()
        time.sleep(self.cfg.poll_interval * 1.2)
        ing.scan_spool()
        proc = Processor(self.cfg)
        proc.recover_stale_leases()  # 与真实主循环一致：先接管过期租约
        for _ in range(5):
            pkt = proc.pick_next_packet()
            if pkt is None:
                break
            proc._process_packet(pkt, 0)
        view = {t["tenant_id"]: t for t in api.tenant_view(conn)}
        # 没有任何处理侧未决包：跳号没有阻塞后续包的解包流动
        # （ready 是等待消费的正常终态，由消费端测试覆盖）
        blocked = sum(view[tenant]["packet_counts"].get(s, 0)
                      for s in ("queued", "processing", "quarantined"))
        self.assertEqual(blocked, 0)
        # 缺口永久展示，且 dead 包不被伪装成成功
        self.assertTrue(view[tenant]["has_gap"])
        self.assertEqual(view[tenant]["gaps"][0]["kind"], "dead")


class TestConsumer(PacketQTestBase):
    def test_ordered_exactly_once_and_verify(self) -> None:
        for tenant, n in [("ta", 4), ("tb", 3)]:
            self.spool(tenant, "u1", [msg(i) for i in range(n)])
        self.ingest_all()
        proc = Processor(self.cfg)
        for _ in range(5):
            pkt = proc.pick_next_packet()
            if pkt is None:
                break
            proc._process_packet(pkt, 0)
        consumer = Consumer(self.cfg)
        consumer.reconcile_output()
        for _ in range(20):
            m = consumer.pick_next_message()
            if m is None:
                break
            consumer._consume_one(m)
        conn = consumer.conn
        # 包推进到 done
        self.assertEqual(conn.execute(
            "SELECT COUNT(*) n FROM packets WHERE status=?",
            (ST_DONE,)).fetchone()["n"], 2)
        # 重复执行同一消费不会产生重复结果（UNIQUE 约束）
        first = conn.execute("SELECT * FROM messages ORDER BY id LIMIT 1").fetchone()
        from packetq.common import sha256_text
        with self.assertRaises(Exception):
            conn.execute(
                "INSERT INTO consumer_results(message_id, tenant_id, seq,"
                " output_sha, processed_at) VALUES(?,?,?,?,?)",
                (first["id"], first["tenant_id"], first["seq"], "x", time.time()))
        report = api.verify_consumer(conn, self.cfg)
        self.assertTrue(report["ok"], report)
        self.assertEqual(report["db_result_rows"], 7)
        self.assertEqual(report["file_result_rows"], 7)
        consumer._fh.close()

    def test_reconcile_rebuilds_output_after_crash(self) -> None:
        self.spool("ta", "u1", [msg(1), msg(2), msg(3)])
        self.ingest_all()
        proc = Processor(self.cfg)
        pkt = proc.pick_next_packet()
        proc._process_packet(pkt, 0)
        c1 = Consumer(self.cfg)
        c1.reconcile_output()
        for _ in range(2):  # 只消费两条，模拟崩溃
            c1._consume_one(c1.pick_next_message())
        c1._fh.close()
        # 往结果文件写一条"脏行"模拟崩溃残留，reconcile 必须重建掉
        with open(self.cfg.result_path, "a", encoding="utf-8") as fh:
            fh.write('{"tenant_id":"ta","seq":99,"output_sha":"dirty"}\n')
        c2 = Consumer(self.cfg)
        c2.reconcile_output()
        while True:
            m = c2.pick_next_message()
            if m is None:
                break
            c2._consume_one(m)
        report = api.verify_consumer(c2.conn, self.cfg)
        self.assertTrue(report["ok"])
        c2._fh.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
