"""数据模拟器：生成带典型湖表问题的数据集。

模拟的四类问题：
1. 分区倾斜   —— 约 1/5 的分区数据量是普通分区的 3~5 倍（profile=skew）
2. 小文件爆炸 —— 最近 3 天的热分区模拟流式写入：30~50 个几 KB 的小文件，
                 且键范围随机重叠（裁剪效果差）
3. 冷热分层   —— 7 天前的分区为 COLD 层（文件少而大），近期为 HOT 层
4. 部分文件损坏 —— 随机挑选约 5% 的文件物理截断+写入垃圾字节
"""
import random
import shutil
import string
import time
from datetime import date, timedelta

from . import metastore, storage

KEY_SPACE = 20000  # 每个分区的排序键空间 [0, KEY_SPACE)


def _rand_payload(rng, n=None):
    n = n or rng.randint(40, 80)
    return "".join(rng.choices(string.ascii_letters + string.digits, k=n))


def _make_rows(rng, keys, part_key):
    return [{"key": k, "ts": f"{part_key}T{rng.randint(0,23):02d}:00:00Z",
             "payload": _rand_payload(rng)} for k in keys]


def _contiguous_ranges(n_files, key_space=KEY_SPACE):
    """连续不重叠键范围（批量写入的特征）。"""
    step = key_space // n_files
    return [(i * step, (i + 1) * step - 1) for i in range(n_files)]


def _overlapping_ranges(rng, n_files, key_space=KEY_SPACE):
    """随机重叠键范围（流式小批量写入的特征 => min/max 裁剪几乎失效）。"""
    ranges = []
    for _ in range(n_files):
        lo = rng.randint(0, int(key_space * 0.9))
        hi = min(key_space - 1, lo + rng.randint(int(key_space * 0.05),
                                                 int(key_space * 0.2)))
        ranges.append((lo, hi))
    return ranges


# 各画像的 (文件数范围, 每文件行数范围)
PROFILES = {
    "stream": ((30, 50), (20, 120)),      # 小文件爆炸（热区流式写入）
    "skew":   ((8, 12), (1500, 2500)),    # 倾斜分区（3~5 倍数据量）
    "normal": ((4, 8), (400, 800)),       # 普通热分区
    "cold":   ((3, 5), (800, 1500)),      # 冷分区（已被归档过，文件少而大）
}


def wipe_table(conn, table):
    """删除表的全部元数据与磁盘文件。"""
    for t in ("files", "partitions", "snapshots", "merge_jobs", "query_log",
              "pinned_snapshots"):
        conn.execute(f"DELETE FROM {t} WHERE table_name=?", (table,))
    conn.commit()
    shutil.rmtree(storage.table_dir(table), ignore_errors=True)


def generate_table(table="events", days=14, seed=42, scale=1.0,
                   corrupt_ratio=0.05):
    """生成模拟数据集，返回摘要。"""
    rng = random.Random(seed)
    metastore.init_db()
    with metastore.connect() as conn:
        wipe_table(conn, table)
        snap = metastore.new_snapshot(conn, table, "INIT",
                                      f"simulator days={days} seed={seed}")
        today = date.today()
        skew_idx = set(rng.sample(range(days), k=max(1, days // 5)))
        all_files = []
        profile_count = {"stream": 0, "skew": 0, "normal": 0, "cold": 0}

        for i in range(days):
            part_key = (today - timedelta(days=days - 1 - i)).isoformat()
            age = days - 1 - i
            tier = "COLD" if age >= 7 else "HOT"
            metastore.add_partition(conn, table, part_key, tier)
            if age <= 2:
                profile = "stream"
            elif i in skew_idx:
                profile = "skew"
            elif tier == "COLD":
                profile = "cold"
            else:
                profile = "normal"
            profile_count[profile] += 1

            (nf_lo, nf_hi), (nr_lo, nr_hi) = PROFILES[profile]
            n_files = rng.randint(nf_lo, nf_hi)
            ranges = (_overlapping_ranges(rng, n_files) if profile == "stream"
                      else _contiguous_ranges(n_files))
            for lo, hi in ranges:
                n_rows = max(1, int(rng.randint(nr_lo, nr_hi) * scale))
                keys = [rng.randint(lo, hi) for _ in range(n_rows)]
                keys.sort()
                meta = storage.write_rows(table, part_key,
                                          _make_rows(rng, keys, part_key))
                fid = metastore.insert_file(conn, table, part_key, meta, snap)
                all_files.append({"id": fid, "path": meta["path"],
                                  "part_key": part_key})
        conn.commit()

        # 部分文件损坏（模拟后台审计已发现 => corrupt 标记置 1）
        n_corrupt = max(1, int(len(all_files) * corrupt_ratio))
        corrupted = rng.sample(all_files, k=min(n_corrupt, len(all_files)))
        for f in corrupted:
            storage.corrupt_file(f["path"], rng)
            metastore.mark_corrupt(conn, f["id"], 1)

        return {
            "table": table, "snapshot": snap, "days": days,
            "partitions": days, "files": len(all_files),
            "corrupted_files": len(corrupted),
            "profiles": profile_count,
            "corrupt_parts": sorted({f["part_key"] for f in corrupted}),
        }


def inject_corruption(table, part_key=None, count=1):
    """运行期注入新的损坏（演示用）：随机挑选未损坏的活文件。"""
    rng = random.Random()
    with metastore.connect() as conn:
        snap = metastore.current_snapshot(conn, table)
        parts = ([{"part_key": part_key}] if part_key else
                 metastore.list_partitions(conn, table))
        candidates = []
        for p in parts:
            for f in metastore.visible_files(conn, table, p["part_key"], snap):
                if not f["corrupt"]:
                    candidates.append(f)
        rng.shuffle(candidates)
        picked = candidates[:count]
        for f in picked:
            storage.corrupt_file(f["path"], rng)
            metastore.mark_corrupt(conn, f["id"], 1)
        return [{"id": f["id"], "part_key": f["part_key"],
                 "path": f["path"]} for f in picked]
