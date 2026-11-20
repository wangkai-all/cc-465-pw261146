"""查询引擎：分区裁剪 + 文件级 min/max 裁剪 + 快照隔离读取。

- 查询开始时 pin 当前快照，整个查询期间看到一致的文件集合，
  即使同时有合并在提交（合并期间保留可读快照）；
- 分区裁剪：按 part_key 范围过滤；
- 文件裁剪：按文件 (min_key, max_key) 与查询键范围是否相交过滤；
- 损坏文件：默认部分恢复并计入统计，strict 模式直接报错；
- 每次查询写入 query_log，供治理规划统计查询热度。
"""
import json
import time

from . import config, metastore, storage


def _overlap(a_lo, a_hi, b_lo, b_hi):
    return a_lo <= b_hi and b_lo <= a_hi


def run_query(table, part_start=None, part_end=None, key_min=None,
              key_max=None, strict=False, simulate_latency=None,
              log=True):
    """执行一次范围查询，返回行与裁剪统计。"""
    if simulate_latency is None:
        simulate_latency = config.SIMULATE_LATENCY
    t0 = time.time()
    conn = metastore.connect()
    try:
        snap = metastore.current_snapshot(conn, table)
        token = metastore.pin_snapshot(conn, table, snap)  # 固定可读快照
        try:
            parts = metastore.list_partitions(conn, table)
            hit_parts, pruned_parts = [], 0
            files_hit = files_pruned = corrupt_files = 0
            bytes_scanned = 0
            rows_out = []
            key_lo = key_min if key_min is not None else -2**63
            key_hi = key_max if key_max is not None else 2**63 - 1

            for p in parts:
                pk = p["part_key"]
                if part_start and pk < part_start:
                    pruned_parts += 1
                    continue
                if part_end and pk > part_end:
                    pruned_parts += 1
                    continue
                hit_parts.append(pk)
                files = metastore.visible_files(conn, table, pk, snap)
                for f in files:
                    if not _overlap(key_lo, key_hi, f["min_key"], f["max_key"]):
                        files_pruned += 1
                        continue
                    files_hit += 1
                    bytes_scanned += f["size_bytes"]
                    if simulate_latency:
                        mbps = (config.HOT_READ_MBPS if p["tier"] == "HOT"
                                else config.COLD_READ_MBPS)
                        time.sleep(min(0.05, f["size_bytes"] /
                                       (mbps * 1024 * 1024)))
                    rows, corrupt, truncated = storage.read_rows(
                        f, strict=strict)
                    if corrupt:
                        corrupt_files += 1
                    for r in rows:
                        if key_lo <= r["key"] <= key_hi:
                            rows_out.append(r)
                metastore.touch_partition(conn, table, pk)

            duration_ms = (time.time() - t0) * 1000
            result = {
                "table": table,
                "snapshot": snap,
                "partitions_hit": len(hit_parts),
                "partitions_pruned": pruned_parts,
                "hit_parts": hit_parts,
                "files_hit": files_hit,
                "files_pruned": files_pruned,
                "corrupt_files": corrupt_files,
                "bytes_scanned": bytes_scanned,
                "rows_returned": len(rows_out),
                "duration_ms": round(duration_ms, 2),
                "rows": rows_out[: config.MAX_ROWS_RETURN],
            }
            if log:
                metastore.log_query(conn, {
                    "table": table, "part_start": part_start,
                    "part_end": part_end, "key_min": key_min,
                    "key_max": key_max, "snapshot": snap,
                    "partitions_hit": len(hit_parts),
                    "partitions_pruned": pruned_parts,
                    "files_hit": files_hit, "files_pruned": files_pruned,
                    "corrupt_files": corrupt_files,
                    "bytes_scanned": bytes_scanned,
                    "rows_returned": len(rows_out),
                    "duration_ms": result["duration_ms"],
                    "hit_parts": json.dumps(hit_parts),
                })
            return result
        finally:
            metastore.unpin_snapshot(conn, token)
    finally:
        conn.close()
