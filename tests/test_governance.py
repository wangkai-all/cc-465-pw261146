"""治理工具端到端验证（stdlib unittest，无第三方依赖）。

运行：python3 -m unittest discover -s tests -v
覆盖：
1. 模拟器形态（倾斜/小文件/冷热/损坏）
2. 分区裁剪与文件级 min/max 裁剪
3. 策略选择（BIN_PACK / SORT_MERGE / TIERED_COMPACT / REPAIR）
4. 合并后文件数下降、SORT_MERGE 后键范围不重叠
5. 合并期间快照隔离（pin 旧快照仍读旧文件）
6. 并发查询一致性（verify.concurrent_read_report）
7. commit 失败回滚恢复原文件引用
8. 重试成功
9. REPAIR 部分恢复损坏文件
10. GC 尊重 pinned 快照
"""
import os
import tempfile
import unittest

# 必须在 import lakegov 之前隔离数据目录
_TMP = tempfile.mkdtemp(prefix="lakegov-test-")
os.environ["LAKEGOV_DATA"] = _TMP
os.environ["LAKEGOV_DB"] = os.path.join(_TMP, "metastore.db")

from lakegov import (compactor, config, metastore, planner, query,  # noqa: E402
                     simulator, storage, verify)  # noqa: E402

TABLE = "events"
config.SIMULATE_LATENCY = False


def fresh_dataset(days=10, seed=7, scale=0.5, corrupt_ratio=0.06):
    metastore.reset_db()
    return simulator.generate_table(TABLE, days=days, seed=seed, scale=scale,
                                    corrupt_ratio=corrupt_ratio)


class TestSimulator(unittest.TestCase):
    def setUp(self):
        self.summary = fresh_dataset()

    def test_profiles_present(self):
        p = self.summary["profiles"]
        self.assertGreater(p["stream"], 0, "应有小文件爆炸分区")
        self.assertGreater(p["skew"], 0, "应有倾斜分区")
        self.assertGreater(p["cold"], 0, "应有冷分区")
        self.assertGreater(self.summary["corrupted_files"], 0, "应有损坏文件")

    def test_skew_and_tiers(self):
        conn = metastore.connect()
        snap = metastore.current_snapshot(conn, TABLE)
        stats = [planner.partition_stats(conn, TABLE, p["part_key"], snap)
                 for p in metastore.list_partitions(conn, TABLE)]
        conn.close()
        self.assertTrue(any(s["skew_factor"] >= config.SKEW_FACTOR
                            for s in stats), "存在倾斜分区")
        tiers = {s["part_key"]: s["tier"] for s in stats}
        self.assertIn("COLD", tiers.values())
        self.assertIn("HOT", tiers.values())
        stream = [s for s in stats if s["small_ratio"] >= 0.9]
        self.assertTrue(stream, "存在小文件占比 >=90% 的分区")


class TestQueryPruning(unittest.TestCase):
    def setUp(self):
        fresh_dataset()

    def test_partition_pruning(self):
        conn = metastore.connect()
        parts = [p["part_key"] for p in metastore.list_partitions(conn, TABLE)]
        conn.close()
        full = query.run_query(TABLE)
        ranged = query.run_query(TABLE, part_start=parts[2], part_end=parts[4])
        self.assertEqual(full["partitions_hit"], len(parts))
        self.assertEqual(ranged["partitions_hit"], 3)
        self.assertEqual(ranged["partitions_pruned"], len(parts) - 3)
        self.assertLess(ranged["bytes_scanned"], full["bytes_scanned"])

    def test_file_pruning_by_key_range(self):
        # 连续键范围分区（normal/cold）：窄键范围应裁剪掉大部分文件
        conn = metastore.connect()
        snap = metastore.current_snapshot(conn, TABLE)
        target = None
        for p in metastore.list_partitions(conn, TABLE):
            st = planner.partition_stats(conn, TABLE, p["part_key"], snap)
            if st and st["file_count"] >= 4 and st["overlap_ratio"] < 0.05 \
                    and st["corrupt_files"] == 0:
                target = p["part_key"]
                break
        conn.close()
        self.assertIsNotNone(target, "应存在不重叠分区")
        r = query.run_query(TABLE, part_start=target, part_end=target,
                            key_min=0, key_max=999)
        self.assertGreater(r["files_pruned"], 0, "窄键范围应裁剪掉部分文件")
        self.assertLess(r["files_hit"], r["files_pruned"] + r["files_hit"])

    def test_corrupt_file_partial_recovery(self):
        conn = metastore.connect()
        snap = metastore.current_snapshot(conn, TABLE)
        corrupt_part = None
        for p in metastore.list_partitions(conn, TABLE):
            files = metastore.visible_files(conn, TABLE, p["part_key"], snap)
            if any(f["corrupt"] for f in files):
                corrupt_part = p["part_key"]
                break
        conn.close()
        self.assertIsNotNone(corrupt_part)
        r = query.run_query(TABLE, part_start=corrupt_part,
                            part_end=corrupt_part)
        self.assertGreaterEqual(r["corrupt_files"], 1)
        self.assertGreater(r["rows_returned"], 0, "损坏文件应被部分恢复")
        with self.assertRaises(storage.CorruptFileError):
            query.run_query(TABLE, part_start=corrupt_part,
                            part_end=corrupt_part, strict=True)


class TestPlanner(unittest.TestCase):
    def setUp(self):
        fresh_dataset()

    def _stats(self, part_key):
        conn = metastore.connect()
        st = planner.partition_stats(conn, TABLE, part_key)
        conn.close()
        return st

    def test_stream_partition_binpack(self):
        conn = metastore.connect()
        snap = metastore.current_snapshot(conn, TABLE)
        stream_pk = None
        for p in metastore.list_partitions(conn, TABLE):
            st = planner.partition_stats(conn, TABLE, p["part_key"], snap)
            if st and st["small_ratio"] >= 0.9:
                stream_pk = p["part_key"]
                break
        conn.close()
        self.assertIsNotNone(stream_pk)
        planned = planner.plan_merge(TABLE, stream_pk)
        self.assertEqual(planned["plan"]["strategy"], "BIN_PACK")
        self.assertGreater(planned["cost"]["files_reduced"], 0)

    def test_cold_partition_tiered(self):
        conn = metastore.connect()
        snap = metastore.current_snapshot(conn, TABLE)
        cold_pk = None
        for p in metastore.list_partitions(conn, TABLE):
            st = planner.partition_stats(conn, TABLE, p["part_key"], snap)
            if st and st["tier"] == "COLD" \
                    and st["file_count"] >= config.MIN_FILES_TO_COMPACT:
                cold_pk = p["part_key"]
                break
        conn.close()
        self.assertIsNotNone(cold_pk)
        planned = planner.plan_merge(TABLE, cold_pk)
        self.assertEqual(planned["plan"]["strategy"], "TIERED_COMPACT")
        self.assertEqual(planned["plan"]["target_size"],
                         config.TARGET_FILE_SIZE_COLD)

    def test_hot_queried_overlapping_sort_merge(self):
        # 构造：stream 分区（重叠严重）+ 多次查询命中 => SORT_MERGE
        conn = metastore.connect()
        snap = metastore.current_snapshot(conn, TABLE)
        pk = None
        for p in metastore.list_partitions(conn, TABLE):
            st = planner.partition_stats(conn, TABLE, p["part_key"], snap)
            if st and st["overlap_ratio"] > config.OVERLAP_RATIO_THRESHOLD:
                pk = p["part_key"]
                break
        conn.close()
        self.assertIsNotNone(pk, "stream 分区键范围应严重重叠")
        for _ in range(config.HOT_QUERY_THRESHOLD):
            query.run_query(TABLE, part_start=pk, part_end=pk,
                            key_min=0, key_max=1000)
        planned = planner.plan_merge(TABLE, pk)
        self.assertEqual(planned["plan"]["strategy"], "SORT_MERGE")

    def test_corrupt_partition_gets_repair(self):
        conn = metastore.connect()
        snap = metastore.current_snapshot(conn, TABLE)
        pk = None
        for p in metastore.list_partitions(conn, TABLE):
            st = planner.partition_stats(conn, TABLE, p["part_key"], snap)
            if st and st["corrupt_files"] > 0 \
                    and st["file_count"] >= config.MIN_FILES_TO_COMPACT:
                pk = p["part_key"]
                break
        conn.close()
        self.assertIsNotNone(pk)
        planned = planner.plan_merge(TABLE, pk)
        self.assertTrue(planned["plan"]["repair"])
        self.assertIn("REPAIR", " ".join(planned["plan"]["reasons"]))


class TestCompaction(unittest.TestCase):
    def setUp(self):
        fresh_dataset()

    def _compactable_part(self, corrupt_free=True):
        conn = metastore.connect()
        snap = metastore.current_snapshot(conn, TABLE)
        best, best_n = None, 0
        for p in metastore.list_partitions(conn, TABLE):
            files = metastore.visible_files(conn, TABLE, p["part_key"], snap)
            if corrupt_free and any(f["corrupt"] for f in files):
                continue
            if len(files) > best_n:
                best, best_n = p["part_key"], len(files)
        conn.close()
        return best

    def test_binpack_reduces_files_and_keeps_rows(self):
        pk = self._compactable_part()
        before = query.run_query(TABLE, part_start=pk, part_end=pk)
        job_id = compactor.create_job(TABLE, pk)
        self.assertIsNotNone(job_id)
        job = compactor.run_job(job_id, sleep=lambda s: None)
        self.assertEqual(job["status"], "COMMITTED")
        after = query.run_query(TABLE, part_start=pk, part_end=pk)
        self.assertLess(after["files_hit"], before["files_hit"],
                        "合并后文件数应减少")
        self.assertEqual(after["rows_returned"], before["rows_returned"],
                         "无损坏合并必须保行数")
        self.assertGreater(after["snapshot"], before["snapshot"])

    def test_snapshot_isolation_during_merge(self):
        pk = self._compactable_part()
        conn = metastore.connect()
        snap_old = metastore.current_snapshot(conn, TABLE)
        token = metastore.pin_snapshot(conn, TABLE, snap_old)
        files_old = {f["id"] for f in
                     metastore.visible_files(conn, TABLE, pk, snap_old)}
        job_id = compactor.create_job(TABLE, pk)
        compactor.run_job(job_id, sleep=lambda s: None)
        # pin 的旧快照仍看到旧文件集合（合并期间保留可读快照）
        still = {f["id"] for f in
                 metastore.visible_files(conn, TABLE, pk, snap_old)}
        self.assertEqual(files_old, still)
        # 旧文件在磁盘上仍在（GC 尊重 pin）
        for fid in files_old:
            row = conn.execute("SELECT path FROM files WHERE id=?",
                               (fid,)).fetchone()
            self.assertTrue(os.path.exists(row["path"]))
        metastore.unpin_snapshot(conn, token)
        conn.close()

    def test_commit_failure_rolls_back(self):
        pk = self._compactable_part()
        conn = metastore.connect()
        snap0 = metastore.current_snapshot(conn, TABLE)
        ids_before = [f["id"] for f in
                      metastore.visible_files(conn, TABLE, pk, snap0)]
        conn.close()
        job_id = compactor.create_job(TABLE, pk, fail_at="commit",
                                      fail_attempts=[1, 2, 3], max_attempts=3)
        job = compactor.run_job(job_id, sleep=lambda s: None)
        self.assertEqual(job["status"], "FAILED")
        self.assertEqual(job["attempts"], 3)
        conn = metastore.connect()
        snap1 = metastore.current_snapshot(conn, TABLE)
        ids_after = [f["id"] for f in
                     metastore.visible_files(conn, TABLE, pk, snap1)]
        conn.close()
        self.assertEqual(ids_before, ids_after,
                         "全部尝试失败后原文件引用必须保持不变")

    def test_write_failure_leaves_no_reference_and_gc_cleans(self):
        pk = self._compactable_part()
        job_id = compactor.create_job(TABLE, pk, fail_at="write",
                                      fail_attempts=[1])
        job = compactor.run_job(job_id, sleep=lambda s: None)
        self.assertEqual(job["status"], "COMMITTED")  # 第 2 次重试成功
        gc_result = compactor.gc(TABLE)
        # 失败尝试写出的孤儿文件应被 GC 清理
        conn = metastore.connect()
        live_paths = {os.path.abspath(f["path"])
                      for f in metastore.all_live_files(conn, TABLE)}
        conn.close()
        for root, _d, names in os.walk(storage.table_dir(TABLE)):
            for n in names:
                self.assertIn(os.path.abspath(os.path.join(root, n)),
                              live_paths, "磁盘上不应有 metastore 未记录的文件")
        self.assertIsInstance(gc_result["removed_orphans"], list)

    def test_retry_succeeds_after_transient_failure(self):
        pk = self._compactable_part()
        job_id = compactor.create_job(TABLE, pk, fail_at="commit",
                                      fail_attempts=[1])
        job = compactor.run_job(job_id, sleep=lambda s: None)
        self.assertEqual(job["status"], "COMMITTED")
        self.assertEqual(job["attempts"], 2)
        self.assertGreater(job["snapshot_after"], job["snapshot_before"])

    def test_repair_recovers_and_removes_corrupt(self):
        # 找一个既有损坏文件又值得合并的分区；没有就人为注入损坏
        conn = metastore.connect()
        snap = metastore.current_snapshot(conn, TABLE)
        pk = None
        for p in metastore.list_partitions(conn, TABLE):
            files = metastore.visible_files(conn, TABLE, p["part_key"], snap)
            if len(files) >= 4 and any(f["corrupt"] for f in files):
                pk = p["part_key"]
                break
        conn.close()
        if not pk:
            pk = self._compactable_part()
            simulator.inject_corruption(TABLE, pk, count=1)
        before = query.run_query(TABLE, part_start=pk, part_end=pk)
        self.assertGreaterEqual(before["corrupt_files"], 1)
        job_id = compactor.create_job(TABLE, pk)
        job = compactor.run_job(job_id, sleep=lambda s: None)
        self.assertEqual(job["status"], "COMMITTED")
        self.assertEqual(job["repair"], 1)
        after = query.run_query(TABLE, part_start=pk, part_end=pk)
        self.assertEqual(after["corrupt_files"], 0, "合并后不应再有损坏文件")
        self.assertGreater(after["rows_returned"], 0,
                           "REPAIR 应恢复损坏点之前的行")

    def test_gc_respects_pinned_snapshot(self):
        pk = self._compactable_part()
        conn = metastore.connect()
        snap_old = metastore.current_snapshot(conn, TABLE)
        token = metastore.pin_snapshot(conn, TABLE, snap_old)
        old_paths = [f["path"] for f in
                     metastore.visible_files(conn, TABLE, pk, snap_old)]
        conn.close()
        job_id = compactor.create_job(TABLE, pk)
        compactor.run_job(job_id, sleep=lambda s: None)
        result = compactor.gc(TABLE)
        for p in old_paths:
            self.assertTrue(os.path.exists(p), "pin 未释放前旧文件不得被 GC")
        self.assertEqual(result["removed_tombstoned"], [])
        conn = metastore.connect()
        metastore.unpin_snapshot(conn, token)
        conn.close()
        result2 = compactor.gc(TABLE)
        for p in old_paths:
            self.assertFalse(os.path.exists(p), "pin 释放后旧文件应被 GC")
        self.assertTrue(result2["removed_tombstoned"])


class TestVerifyReports(unittest.TestCase):
    def setUp(self):
        fresh_dataset()

    def test_concurrent_reads_consistent(self):
        report = verify.concurrent_read_report(TABLE, readers=6)
        self.assertNotIn("error", report)
        self.assertEqual(report["job_status"], "COMMITTED")
        self.assertEqual(report["errors"], [])
        self.assertEqual(report["inconsistent_reads"], [])
        self.assertTrue(report["consistent"])
        self.assertGreater(report["total_reads"], 0)

    def test_retry_report(self):
        report = verify.retry_report(TABLE)
        self.assertNotIn("error", report)
        rj = report["retry_job"]
        self.assertEqual(rj["status"], "COMMITTED")
        self.assertEqual(rj["attempts"], 2)
        self.assertTrue(report["rollback_check"]["references_restored"])
        self.assertTrue(report["commit_check"]["old_files_replaced"])
        self.assertTrue(report["consistent"])


if __name__ == "__main__":
    unittest.main()
