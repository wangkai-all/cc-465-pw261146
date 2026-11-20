"""治理规划器：根据查询范围（热度）与文件统计选择合并策略，并估算成本。

策略：
- BIN_PACK        小文件装箱：小文件占比高时，按目标大小贪心装箱，不重排序
- SORT_MERGE      排序合并：查询热度高且键范围重叠严重时，重排为互不重叠的
                  有序文件，大幅提升 min/max 裁剪命中率；倾斜分区也用它摊平
- TIERED_COMPACT  冷区归档合并：冷层且长期无查询，按更大的目标大小合并降本
- REPAIR（修饰）  存在损坏文件时叠加：跳过损坏点之后的数据，尽量恢复行
- NO_OP           文件数少且大小健康，无需治理
"""
import json
import math
import time

from . import config, metastore

STRATEGIES = ("BIN_PACK", "SORT_MERGE", "TIERED_COMPACT", "NO_OP")


def _merged_union(intervals):
    """区间并集总长度（用于计算键范围重叠度）。"""
    if not intervals:
        return 0
    iv = sorted(intervals)
    total, cur_lo, cur_hi = 0, iv[0][0], iv[0][1]
    for lo, hi in iv[1:]:
        if lo > cur_hi + 1:
            total += cur_hi - cur_lo + 1
            cur_lo, cur_hi = lo, hi
        else:
            cur_hi = max(cur_hi, hi)
    total += cur_hi - cur_lo + 1
    return total


def partition_stats(conn, table, part_key, snapshot=None):
    """单个分区的文件统计 + 查询热度。"""
    snap = snapshot or metastore.current_snapshot(conn, table)
    files = metastore.visible_files(conn, table, part_key, snap)
    if not files:
        return None
    part = metastore.get_partition(conn, table, part_key)
    sizes = [f["size_bytes"] for f in files]
    total = sum(sizes)
    intervals = [(f["min_key"], f["max_key"]) for f in files]
    span_total = sum(hi - lo + 1 for lo, hi in intervals)
    union = _merged_union(intervals)
    overlap_ratio = (1 - union / span_total) if span_total else 0.0

    # 倾斜度：相对全部活跃分区大小的中位数
    siblings = []
    for p in metastore.list_partitions(conn, table):
        s = sum(f["size_bytes"]
                for f in metastore.visible_files(conn, table, p["part_key"], snap))
        if s:
            siblings.append(s)
    siblings.sort()
    median = siblings[len(siblings) // 2] if siblings else total or 1
    skew_factor = total / median if median else 1.0

    # 查询热度：最近 100 次查询中命中本分区的次数与平均文件裁剪率
    heat_queries, prune_rates = 0, []
    for q in metastore.recent_queries(conn, table, limit=100):
        try:
            hits = json.loads(q["hit_parts"] or "[]")
        except (ValueError, TypeError):
            hits = []
        if part_key in hits:
            heat_queries += 1
            tot = q["files_hit"] + q["files_pruned"]
            if tot:
                prune_rates.append(q["files_pruned"] / tot)

    small = sum(1 for s in sizes if s < config.SMALL_FILE_THRESHOLD)
    return {
        "table": table, "part_key": part_key, "snapshot": snap,
        "tier": part["tier"] if part else "HOT",
        "last_accessed": part["last_accessed"] if part else None,
        "file_count": len(files),
        "total_bytes": total,
        "avg_bytes": total // len(files),
        "min_file_bytes": min(sizes), "max_file_bytes": max(sizes),
        "small_files": small,
        "small_ratio": small / len(files),
        "corrupt_files": sum(1 for f in files if f["corrupt"]),
        "overlap_ratio": round(overlap_ratio, 3),
        "skew_factor": round(skew_factor, 2),
        "heat_queries": heat_queries,
        "avg_file_prune_rate": (round(sum(prune_rates) / len(prune_rates), 3)
                                if prune_rates else None),
        "files": files,
    }


def _binpack_groups(sizes, target):
    """首次适应递减（FFD）装箱，返回每组的大小列表。"""
    bins = []
    for s in sorted(sizes, reverse=True):
        placed = False
        for b in bins:
            if sum(b) + s <= target:
                b.append(s)
                placed = True
                break
        if not placed:
            bins.append([s])
    return bins


def choose_strategy(stats):
    """按查询热度与文件统计选择策略，返回 (strategy, repair, target_size, reasons)。"""
    reasons = []
    repair = stats["corrupt_files"] > 0
    if repair:
        reasons.append(
            f"发现 {stats['corrupt_files']} 个损坏文件 => 叠加 REPAIR（跳过损坏段、尽量恢复行）")

    if stats["file_count"] < config.MIN_FILES_TO_COMPACT and not repair:
        return "NO_OP", False, 0, ["文件数少且无需修复，不治理"]

    hot_queried = stats["heat_queries"] >= config.HOT_QUERY_THRESHOLD
    if hot_queried and stats["overlap_ratio"] > config.OVERLAP_RATIO_THRESHOLD:
        reasons.append(
            f"近期查询命中 {stats['heat_queries']} 次且键范围重叠度 "
            f"{stats['overlap_ratio']:.0%} => SORT_MERGE 重排为不重叠文件，提升裁剪")
        return "SORT_MERGE", repair, config.TARGET_FILE_SIZE_HOT, reasons

    if (stats["tier"] == "COLD" and stats["heat_queries"] == 0
            and stats["file_count"] >= config.MIN_FILES_TO_COMPACT):
        reasons.append("冷层且近期无查询 => TIERED_COMPACT 归档合并（更大目标文件）")
        return "TIERED_COMPACT", repair, config.TARGET_FILE_SIZE_COLD, reasons

    if (stats["small_ratio"] >= config.SMALL_FILE_RATIO
            and stats["file_count"] >= config.MIN_FILES_TO_COMPACT):
        reasons.append(
            f"小文件占比 {stats['small_ratio']:.0%}（{stats['small_files']}/"
            f"{stats['file_count']}）=> BIN_PACK 装箱合并")
        return "BIN_PACK", repair, config.TARGET_FILE_SIZE_HOT, reasons

    if stats["skew_factor"] >= config.SKEW_FACTOR:
        reasons.append(
            f"分区倾斜（{stats['skew_factor']:.1f}x 中位数）=> SORT_MERGE 摊平文件大小")
        return "SORT_MERGE", repair, config.TARGET_FILE_SIZE_HOT, reasons

    if stats["file_count"] >= config.MIN_FILES_TO_COMPACT * 2:
        reasons.append("文件数偏多 => BIN_PACK 默认治理")
        return "BIN_PACK", repair, config.TARGET_FILE_SIZE_HOT, reasons

    return "NO_OP", repair, 0, ["文件分布健康，无需治理"]


def estimate_cost(stats, strategy, target_size, repair):
    """成本估计：读写字节、耗时、文件数变化、裁剪改善预估。"""
    files = stats["files"]
    good = [f for f in files if not (repair and f["corrupt"])]
    corrupt = [f for f in files if repair and f["corrupt"]]
    read_bytes = sum(f["size_bytes"] for f in files)
    # 损坏文件按 60% 可恢复估计写出量
    write_bytes = sum(f["size_bytes"] for f in good) + int(
        0.6 * sum(f["size_bytes"] for f in corrupt))

    if strategy == "NO_OP":
        files_out = stats["file_count"]
    elif strategy == "BIN_PACK":
        sizes = [f["size_bytes"] for f in good]
        if corrupt:
            sizes.append(int(0.6 * sum(f["size_bytes"] for f in corrupt)))
        files_out = max(1, len(_binpack_groups(sizes, target_size)))
    else:  # SORT_MERGE / TIERED_COMPACT
        files_out = max(1, math.ceil(write_bytes / target_size))

    mbps = config.HOT_READ_MBPS if stats["tier"] == "HOT" else config.COLD_READ_MBPS
    read_ms = read_bytes / (mbps * 1024 * 1024) * 1000
    write_ms = write_bytes / (config.WRITE_MBPS * 1024 * 1024) * 1000
    commit_ms = (config.COMMIT_BASE_MS +
                 config.COMMIT_PER_FILE_MS * (stats["file_count"] + files_out))
    overlap_after = 0.0 if strategy == "SORT_MERGE" else stats["overlap_ratio"]
    return {
        "read_bytes": read_bytes,
        "write_bytes": write_bytes,
        "files_in": stats["file_count"],
        "files_out": files_out,
        "files_reduced": stats["file_count"] - files_out,
        "est_duration_ms": round(read_ms + write_ms + commit_ms, 1),
        "corrupt_inputs": len(corrupt),
        "overlap_ratio_before": stats["overlap_ratio"],
        "overlap_ratio_after": overlap_after,
    }


def plan_merge(table, part_key):
    """生成治理计划（不落库，落库由 compactor 创建 job 时完成）。"""
    conn = metastore.connect()
    try:
        stats = partition_stats(conn, table, part_key)
        if not stats:
            return {"error": f"partition {part_key} not found or empty"}
        strategy, repair, target, reasons = choose_strategy(stats)
        cost = estimate_cost(stats, strategy, target, repair)
        plan = {
            "table": table, "part_key": part_key,
            "strategy": strategy, "repair": repair,
            "target_size": target, "reasons": reasons,
            "input_file_ids": [f["id"] for f in stats["files"]],
            "snapshot": stats["snapshot"],
        }
        stats_view = {k: v for k, v in stats.items() if k != "files"}
        return {"plan": plan, "cost": cost, "stats": stats_view}
    finally:
        conn.close()
