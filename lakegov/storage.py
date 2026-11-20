"""文件存储层：JSONL 数据文件 + SHA256 校验 + 损坏模拟与部分恢复。

数据文件布局：DATA_DIR/<table>/dt=<part_key>/file-<id>.jsonl
每行一个 JSON：{"key": int, "ts": str, "payload": str}
"""
import hashlib
import json
import os
import random
import uuid

from . import config


class CorruptFileError(Exception):
    """文件校验和不匹配（损坏）。"""


def table_dir(table):
    return os.path.join(config.DATA_DIR, table)


def part_dir(table, part_key):
    return os.path.join(table_dir(table), f"dt={part_key}")


def write_rows(table, part_key, rows):
    """写一个新数据文件，返回元数据（大小/行数/键范围/校验和）。"""
    os.makedirs(part_dir(table, part_key), exist_ok=True)
    path = os.path.join(part_dir(table, part_key),
                        f"file-{uuid.uuid4().hex[:12]}.jsonl")
    buf = "".join(json.dumps(r, separators=(",", ":")) + "\n" for r in rows)
    data = buf.encode()
    with open(path, "wb") as f:
        f.write(data)
    keys = [r["key"] for r in rows] or [0]
    return {
        "path": path,
        "size_bytes": len(data),
        "row_count": len(rows),
        "min_key": min(keys),
        "max_key": max(keys),
        "checksum": hashlib.sha256(data).hexdigest(),
    }


def read_rows(file_meta, strict=False):
    """读取数据文件。

    返回 (rows, corrupt, truncated)：
    - corrupt: 校验和不匹配（文件已损坏）
    - truncated: 损坏导致只恢复了损坏点之前的行
    strict=True 时损坏文件直接抛 CorruptFileError。
    """
    with open(file_meta["path"], "rb") as f:
        data = f.read()
    corrupt = hashlib.sha256(data).hexdigest() != file_meta["checksum"]
    if corrupt and strict:
        raise CorruptFileError(
            f"checksum mismatch: {os.path.basename(file_meta['path'])}")
    rows, truncated = [], False
    for line in data.split(b"\n"):
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except (json.JSONDecodeError, UnicodeDecodeError):
            # 损坏点之后的行不可信，截断（部分恢复）
            truncated = True
            break
    return rows, corrupt, (truncated and corrupt)


def corrupt_file(path, rng=None):
    """物理损坏一个文件：截断到 40%~80% 并追加随机垃圾字节。

    之后文件 checksum 不再匹配 metastore 记录，且损坏点之后的行不可解析。
    """
    rng = rng or random
    with open(path, "rb") as f:
        data = f.read()
    cut = max(1, int(len(data) * rng.uniform(0.4, 0.8)))
    with open(path, "wb") as f:
        f.write(data[:cut])
        f.write(os.urandom(48))


def remove_file_quiet(path):
    try:
        os.remove(path)
        return True
    except OSError:
        return False
