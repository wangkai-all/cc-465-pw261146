#!/usr/bin/env python3
"""一键演示：生成模拟数据集并启动看板服务。

用法：python3 run_demo.py [--port 8000] [--days 14] [--seed 42]
"""
import sys

from lakegov import metastore, simulator
from lakegov.server import main


def _arg(name, default):
    return (type(default)(sys.argv[sys.argv.index(name) + 1])
            if name in sys.argv else default)


if __name__ == "__main__":
    port = _arg("--port", 8000)
    days = _arg("--days", 14)
    seed = _arg("--seed", 42)
    metastore.init_db()
    summary = simulator.generate_table(days=days, seed=seed)
    print("模拟数据集已生成：")
    print(f"  分区 {summary['partitions']} 个 · 文件 {summary['files']} 个 · "
          f"损坏 {summary['corrupted_files']} 个")
    print(f"  画像分布: {summary['profiles']}")
    main(port)
