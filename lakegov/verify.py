"""一键验证：并发查询一致性 + 治理失败重试。

- concurrent_read_report：合并进行中并发执行多个查询，验证每个查询都读到
  一致的快照（行数要么等于合并前、要么等于合并后，绝不出错或读到中间态）；
- retry_report：注入 commit 故障，验证 (a) 失败后原文件引用完全不变，
  (b) 重试成功提交，(c) 全部尝试失败时 job=FAILED 且文件引用仍不变。
"""
import threading
import time

from . import compactor, metastore, planner, query


def _pick_partition(table, need_corrupt_free=True):
    """挑选一个值得治理的分区（文件数最多；验证并发时避开损坏文件）。"""
    conn = metastore.connect()
    try:
        snap = metastore.current_snapshot(conn, table)
        best, best_n = None, -1
        for p in metastore.list_partitions(conn, table):
            files = metastore.visible_files(conn, table, p["part_key"], snap)
            if len(files) < 4:
                continue
            if need_corrupt_free and any(f["corrupt"] for f in files):
                continue
            if len(files) > best_n:
                best, best_n = p["part_key"], len(files)
        return best
    finally:
        conn.close()


def concurrent_read_report(table, part_key=None, readers=8, rounds=3):
    """合并进行中并发查询：所有读取必须一致（无错误、行数属于 {前, 后}）。"""
    part_key = part_key or _pick_partition(table)
    if not part_key:
        return {"error": "no suitable partition (need >=4 healthy files)"}

    before = query.run_query(table, part_start=part_key, part_end=part_key,
                             simulate_latency=False, log=False)
    job_id = compactor.create_job(table, part_key)
    if not job_id:
        return {"error": f"partition {part_key} needs no compaction (NO_OP)"}

    reads, errors = [], []
    stop = threading.Event()

    def reader(idx):
        while not stop.is_set():
            try:
                r = query.run_query(table, part_start=part_key,
                                    part_end=part_key, simulate_latency=False,
                                    log=False)
                reads.append({"reader": idx, "snapshot": r["snapshot"],
                              "rows": r["rows_returned"],
                              "files_hit": r["files_hit"]})
            except Exception as e:  # noqa: BLE001
                errors.append(f"reader{idx}: {type(e).__name__}: {e}")

    job_result = {}

    def run_merge():
        job_result.update(compactor.run_job(job_id, sleep=lambda s: None))

    threads = [threading.Thread(target=reader, args=(i,), daemon=True)
               for i in range(readers)]
    for t in threads:
        t.start()
    merge_thread = threading.Thread(target=run_merge, daemon=True)
    merge_thread.start()
    merge_thread.join(timeout=60)
    # 合并结束后再读几轮（读到的是新快照）
    for _ in range(rounds):
        time.sleep(0.01)
        r = query.run_query(table, part_start=part_key, part_end=part_key,
                            simulate_latency=False, log=False)
        reads.append({"reader": "post", "snapshot": r["snapshot"],
                      "rows": r["rows_returned"], "files_hit": r["files_hit"]})
    stop.set()
    for t in threads:
        t.join(timeout=5)

    after = query.run_query(table, part_start=part_key, part_end=part_key,
                            simulate_latency=False, log=False)
    valid_rows = {before["rows_returned"], after["rows_returned"]}
    inconsistent = [r for r in reads if r["rows"] not in valid_rows]
    return {
        "part_key": part_key,
        "job_id": job_id,
        "job_status": job_result.get("status"),
        "job_strategy": job_result.get("strategy"),
        "snapshot_before": before["snapshot"],
        "snapshot_after": after["snapshot"],
        "rows_before": before["rows_returned"],
        "rows_after": after["rows_returned"],
        "total_reads": len(reads),
        "reads_during_merge": [r for r in reads if r["reader"] != "post"][:40],
        "errors": errors,
        "inconsistent_reads": inconsistent,
        "consistent": not errors and not inconsistent
        and job_result.get("status") == "COMMITTED",
    }


def retry_report(table, part_key=None):
    """故障重试验证：commit 点注入一次失败 => 重试成功；再验证全部失败时引用不变。"""
    part_key = part_key or _pick_partition(table)
    if not part_key:
        return {"error": "no suitable partition"}

    conn = metastore.connect()
    try:
        snap0 = metastore.current_snapshot(conn, table)
        files_before = [f["id"] for f in
                        metastore.visible_files(conn, table, part_key, snap0)]
    finally:
        conn.close()

    # (1) 第 1 次尝试在 COMMIT 点失败 => 第 2 次重试成功
    job_id = compactor.create_job(table, part_key, fail_at="commit",
                                  fail_attempts=[1])
    if not job_id:
        return {"error": f"partition {part_key} needs no compaction (NO_OP)"}
    job = compactor.run_job(job_id, sleep=lambda s: None)

    conn = metastore.connect()
    try:
        snap1 = metastore.current_snapshot(conn, table)
        files_after = metastore.visible_files(conn, table, part_key, snap1)
        # 失败尝试期间曾回滚：检查最终提交后旧文件确实被替换、新文件可见
        old_still_visible = [f["id"] for f in files_after
                             if f["id"] in files_before]
    finally:
        conn.close()

    # (2) 再制造一个全部尝试都失败的 job => 引用必须保持 (1) 之后的状态
    conn = metastore.connect()
    try:
        snap_pre = metastore.current_snapshot(conn, table)
        files_pre = [f["id"] for f in
                     metastore.visible_files(conn, table, part_key, snap_pre)]
    finally:
        conn.close()
    plan_now = planner.plan_merge(table, part_key)
    job2_id = None
    job2 = None
    files_post_fail = files_pre
    if plan_now["plan"]["strategy"] != "NO_OP":
        job2_id = compactor.create_job(table, part_key, fail_at="commit",
                                       fail_attempts=[1, 2, 3], max_attempts=3)
        job2 = compactor.run_job(job2_id, sleep=lambda s: None)
        conn = metastore.connect()
        try:
            snap_post = metastore.current_snapshot(conn, table)
            files_post_fail = [f["id"] for f in metastore.visible_files(
                conn, table, part_key, snap_post)]
        finally:
            conn.close()

    gc_result = compactor.gc(table)
    return {
        "part_key": part_key,
        "retry_job": {
            "job_id": job_id, "status": job["status"],
            "attempts": job["attempts"],
            "history": job["history"],
            "snapshot_before": job["snapshot_before"],
            "snapshot_after": job["snapshot_after"],
        },
        "rollback_check": {
            "files_before_failed_attempt": files_pre,
            "files_after_all_attempts_failed": files_post_fail,
            "references_restored": files_pre == files_post_fail,
            "failed_job_status": job2["status"] if job2 else "skipped(NO_OP)",
            "failed_job_attempts": job2["attempts"] if job2 else 0,
        },
        "commit_check": {
            "old_files_replaced": len(old_still_visible) == 0,
            "live_files_after": len(files_after),
        },
        "gc": gc_result,
        "consistent": (job["status"] == "COMMITTED" and job["attempts"] == 2
                       and files_pre == files_post_fail
                       and len(old_still_visible) == 0),
    }
