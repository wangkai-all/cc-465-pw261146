"""合并执行器：原子提交、故障注入、重试与孤儿文件 GC。

核心保证：
1. 合并期间保留可读快照 —— 新文件先落盘，提交只是单事务里的元数据切换，
   读查询 pin 的旧快照在整个合并期间始终可读；
2. 失败时恢复原文件引用 —— 提交是原子的：任何失败发生在 commit 之前，
   metastore 中原文件的 deleted_snapshot 从未被修改，引用天然不变；
   已写出的新文件成为磁盘孤儿，由 GC 清理；
3. 重试 —— 指数退避，达到 max_attempts 后标记 FAILED，原文件依然在线。
"""
import json
import os
import time
import uuid

from . import config, metastore, planner, storage


class InjectedFailure(Exception):
    """故障注入（测试/演示用）。"""


class StalePlanError(Exception):
    """计划基于的快照已过期（输入文件已不可见）。"""


# ---------------- job 生命周期 ----------------

def create_job(table, part_key, fail_at=None, fail_attempts=None,
               max_attempts=None):
    """根据当前统计生成计划并创建 job。返回 job_id（NO_OP 时返回 None）。"""
    planned = planner.plan_merge(table, part_key)
    if "error" in planned:
        raise ValueError(planned["error"])
    plan, cost = planned["plan"], planned["cost"]
    if plan["strategy"] == "NO_OP":
        return None
    job_id = uuid.uuid4().hex[:12]
    now = time.time()
    with metastore.connect() as conn:
        conn.execute(
            "INSERT INTO merge_jobs(id, table_name, part_key, strategy, repair,"
            " status, plan_json, cost_json, attempts, max_attempts, fail_at,"
            " fail_attempts, snapshot_before, history, created_at, updated_at)"
            " VALUES(?,?,?,?,?,?,?,?,0,?,?,?,?,'[]',?,?)",
            (job_id, table, part_key, plan["strategy"], int(plan["repair"]),
             "PLANNED", json.dumps(plan), json.dumps(cost),
             max_attempts or config.MAX_ATTEMPTS, fail_at,
             json.dumps(list(fail_attempts or [])), plan["snapshot"], now, now))
        conn.commit()
    return job_id


def get_job(conn, job_id):
    r = conn.execute("SELECT * FROM merge_jobs WHERE id=?", (job_id,)).fetchone()
    return dict(r) if r else None


def list_jobs(conn, table=None, limit=50):
    if table:
        rows = conn.execute(
            "SELECT * FROM merge_jobs WHERE table_name=? ORDER BY created_at DESC"
            " LIMIT ?", (table, limit)).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM merge_jobs ORDER BY created_at DESC LIMIT ?",
            (limit,)).fetchall()
    return [dict(r) for r in rows]


def _update_job(conn, job_id, **fields):
    fields["updated_at"] = time.time()
    cols = ", ".join(f"{k}=?" for k in fields)
    conn.execute(f"UPDATE merge_jobs SET {cols} WHERE id=?",
                 (*fields.values(), job_id))
    conn.commit()


def _append_history(conn, job, entry):
    hist = json.loads(job["history"] or "[]")
    hist.append(entry)
    _update_job(conn, job["id"], history=json.dumps(hist))


# ---------------- 单次尝试 ----------------

def _read_inputs(files, repair):
    """读取全部输入文件。repair=True 时损坏文件部分恢复，否则抛错。"""
    units = []   # 每个输入文件一个行列表（BIN_PACK 的装箱单元）
    all_rows = []
    corrupt_seen = 0
    for f in files:
        rows, corrupt, _trunc = storage.read_rows(f, strict=not repair)
        if corrupt:
            corrupt_seen += 1
        units.append({"rows": rows, "size": f["size_bytes"]})
        all_rows.extend(rows)
    return units, all_rows, corrupt_seen


def _write_outputs(table, part_key, strategy, units, all_rows, target_size):
    """按策略写出新文件，返回新文件元数据列表。"""
    metas = []
    if strategy == "BIN_PACK" or strategy == "TIERED_COMPACT":
        # FFD 装箱：把输入文件（单元）装入目标大小的箱子，逐箱写出
        sizes = [u["size"] for u in units]
        order = sorted(range(len(units)), key=lambda i: -sizes[i])
        bins, bin_of = [], {}
        for i in order:
            placed = False
            for bi, b in enumerate(bins):
                if sum(sizes[j] for j in b) + sizes[i] <= target_size:
                    b.append(i)
                    placed = True
                    break
            if not placed:
                bins.append([i])
        for b in bins:
            rows = [r for i in b for r in units[i]["rows"]]
            if rows:
                metas.append(storage.write_rows(table, part_key, rows))
    else:  # SORT_MERGE：全局排序后按目标大小切块 => 键范围互不重叠
        all_rows.sort(key=lambda r: r["key"])
        chunk_rows, chunk_size = [], 0
        for r in all_rows:
            chunk_rows.append(r)
            chunk_size += 110  # 单行近似字节数
            if chunk_size >= target_size:
                metas.append(storage.write_rows(table, part_key, chunk_rows))
                chunk_rows, chunk_size = [], 0
        if chunk_rows:
            metas.append(storage.write_rows(table, part_key, chunk_rows))
    return metas


def _commit(conn, table, part_key, input_ids, output_metas, job_id):
    """单事务提交：新快照 + 新文件 added + 旧文件 deleted + job 状态。

    调用方需持有 metastore.COMMIT_LOCK。失败时事务回滚，原文件引用不变。
    """
    new_snap = metastore.new_snapshot(conn, table, "MERGE_COMMIT",
                                      f"job={job_id} part={part_key}")
    for meta in output_metas:
        metastore.insert_file(conn, table, part_key, meta, new_snap)
    conn.execute(
        f"UPDATE files SET deleted_snapshot=? WHERE id IN"
        f" ({','.join('?' * len(input_ids))})",
        (new_snap, *input_ids))
    return new_snap


def _attempt(job, attempt_no):
    """执行一次合并尝试。失败抛异常，由 run_job 捕获并重试。"""
    table, part_key = job["table_name"], job["part_key"]
    plan = json.loads(job["plan_json"])
    fail_at = job["fail_at"]
    fail_attempts = set(json.loads(job["fail_attempts"] or "[]"))
    should_fail = lambda point: fail_at == point and attempt_no in fail_attempts

    conn = metastore.connect()
    try:
        snap = metastore.current_snapshot(conn, table)
        visible = {f["id"]: f
                   for f in metastore.visible_files(conn, table, part_key, snap)}
        input_ids = plan["input_file_ids"]
        stale = [i for i in input_ids if i not in visible]
        if stale:
            raise StalePlanError(f"input files no longer visible: {stale}")
        inputs = [visible[i] for i in input_ids]

        if should_fail("read"):
            raise InjectedFailure("injected failure at READ")
        units, all_rows, corrupt_seen = _read_inputs(inputs, plan["repair"])

        if should_fail("write"):
            # 写出一个文件后崩溃 => 留下磁盘孤儿，验证 GC 能清理
            if all_rows:
                storage.write_rows(table, part_key, all_rows[:10])
            raise InjectedFailure("injected failure at WRITE")

        outputs = _write_outputs(table, part_key, plan["strategy"], units,
                                 all_rows, plan["target_size"])

        with metastore.COMMIT_LOCK:
            conn.execute("BEGIN IMMEDIATE")
            try:
                if should_fail("commit"):
                    raise InjectedFailure("injected failure at COMMIT")
                new_snap = _commit(conn, table, part_key, input_ids, outputs,
                                   job["id"])
                conn.commit()
            except Exception:
                conn.rollback()   # 原文件引用随事务回滚自动恢复
                raise
        return new_snap, len(outputs), corrupt_seen
    finally:
        conn.close()


def run_job(job_id, sleep=time.sleep):
    """运行 job（含重试）。返回最终 job 记录。"""
    conn = metastore.connect()
    try:
        job = get_job(conn, job_id)
        if not job:
            raise ValueError(f"job {job_id} not found")
        max_attempts = job["max_attempts"]
        for attempt in range(1, max_attempts + 1):
            job = get_job(conn, job_id)
            _update_job(conn, job_id, status="RUNNING", attempts=attempt)
            t0 = time.time()
            try:
                new_snap, n_out, n_corrupt = _attempt(job, attempt)
                _append_history(conn, get_job(conn, job_id), {
                    "attempt": attempt, "result": "committed",
                    "ms": round((time.time() - t0) * 1000, 1),
                    "outputs": n_out, "corrupt_inputs": n_corrupt})
                _update_job(conn, job_id, status="COMMITTED",
                            snapshot_after=new_snap, error=None)
                return get_job(conn, job_id)
            except Exception as e:  # noqa: BLE001 — 任何失败都要回滚+重试
                _append_history(conn, get_job(conn, job_id), {
                    "attempt": attempt, "result": "failed",
                    "error": f"{type(e).__name__}: {e}",
                    "ms": round((time.time() - t0) * 1000, 1)})
                # 回滚审计：记录一条 MERGE_ROLLBACK 快照（不改变任何文件可见性）
                with metastore.COMMIT_LOCK:
                    metastore.new_snapshot(
                        conn, job["table_name"], "MERGE_ROLLBACK",
                        f"job={job_id} attempt={attempt} err={e}")
                    conn.commit()
                if attempt < max_attempts:
                    _update_job(conn, job_id, status="RETRYING",
                                error=str(e))
                    sleep(config.RETRY_BACKOFF_BASE_S * (2 ** (attempt - 1)))
                else:
                    _update_job(conn, job_id, status="FAILED", error=str(e))
        return get_job(conn, job_id)
    finally:
        conn.close()


# ---------------- 垃圾回收 ----------------

def gc(table):
    """清理不再被任何快照引用的文件。

    安全条件：文件的 deleted_snapshot <= 当前最小 pinned 快照（无 pin 时取
    当前快照）——即没有任何读者还能看见它。另清理磁盘上的孤儿文件
    （metastore 中无记录，来自失败的合并尝试）。
    """
    conn = metastore.connect()
    try:
        min_pin = metastore.min_pinned_snapshot(conn, table)
        threshold = (min_pin if min_pin is not None
                     else metastore.current_snapshot(conn, table))
        dead = conn.execute(
            "SELECT * FROM files WHERE table_name=? AND deleted_snapshot IS NOT NULL"
            " AND deleted_snapshot<=?", (table, threshold)).fetchall()
        removed, kept_paths = [], set()
        for f in dead:
            if storage.remove_file_quiet(f["path"]):
                removed.append(os.path.basename(f["path"]))
            conn.execute("DELETE FROM files WHERE id=?", (f["id"],))
        for r in conn.execute("SELECT path FROM files WHERE table_name=?",
                              (table,)).fetchall():
            kept_paths.add(os.path.abspath(r["path"]))
        conn.commit()

        orphans = []
        tdir = storage.table_dir(table)
        for root, _dirs, names in os.walk(tdir):
            for n in names:
                p = os.path.join(root, n)
                if os.path.abspath(p) not in kept_paths:
                    if storage.remove_file_quiet(p):
                        orphans.append(n)
        return {"removed_tombstoned": removed, "removed_orphans": orphans,
                "threshold_snapshot": threshold}
    finally:
        conn.close()
