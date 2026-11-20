# lakegov — 湖表分区治理工具（模拟）

零第三方依赖（Python 3.8+ 标准库），模拟湖表（Lakehouse）分区治理的完整闭环：
**问题模拟 → 查询裁剪 → 治理规划 → 快照安全的合并执行 → 失败回滚与重试 → 可视化看板**。

## 快速开始

```bash
python3 run_demo.py            # 生成模拟数据并启动服务
# 打开 http://localhost:8000
python3 -m unittest discover -s tests -v   # 运行 18 个端到端验证
```

## 模拟的四类问题（`lakegov/simulator.py`）

| 问题 | 模拟方式 |
|---|---|
| 分区倾斜 | 约 1/5 分区数据量为普通分区 3~5 倍（`skew` 画像） |
| 小文件爆炸 | 最近 3 天热分区模拟流式写入：30~50 个几 KB 小文件，键范围随机重叠 |
| 冷热分层 | ≥7 天的分区为 COLD 层（文件少而大、读吞吐更低），近期为 HOT 层 |
| 部分文件损坏 | ~5% 文件被物理截断+写入垃圾字节，checksum 不再匹配 |

## 核心机制

**MVCC 快照隔离**（`metastore.py`）：每个文件记录 `added_snapshot / deleted_snapshot`；
查询开始时 `pin` 当前快照，整个查询看到一致的文件集合 —— 合并期间旧快照始终可读。

**治理规划**（`planner.py`）：按查询热度（`query_log`）与文件统计选择策略：

- `SORT_MERGE` —— 查询热度高且键范围重叠严重 / 分区倾斜：重排为互不重叠的有序文件，提升 min/max 裁剪
- `BIN_PACK` —— 小文件占比高：FFD 装箱到目标大小（热 256KB）
- `TIERED_COMPACT` —— 冷层且无查询：归档到更大目标（1MB）
- `REPAIR`（修饰）—— 有损坏文件时叠加：跳过损坏段、尽量恢复行
- 成本估计：读/写字节、文件数变化、预计耗时、重叠度改善

**原子提交与回滚**（`compactor.py`）：新文件先落盘，提交是单个 SQLite 事务
（旧文件置 `deleted_snapshot` + 新文件置 `added_snapshot` + 新快照行）。
任何失败发生在 commit 前 → 事务回滚 → **原文件引用天然不变**；
磁盘孤儿文件由 GC 清理（GC 尊重 pinned 快照）。失败按指数退避重试，
支持 `read/write/commit` 三处故障注入用于验证。

## 看板页面

分区大小分布（按层着色、倾斜标记）、文件大小直方图（小文件阈值线）、
分区明细表（大小/文件数/小文件占比/键重叠度/倾斜度/查询热度/损坏）、
查询面板（裁剪命中统计）、治理面板（策略+理由+成本估计+故障注入合并）、
一致性验证（并发查询 / 故障重试一键验证）、合并任务与快照时间线。

## API 摘要

```
GET  /api/overview            总览（KPI + 分区统计 + 任务 + 快照）
GET  /api/plan?part_key=      治理计划与成本估计
POST /api/simulate            重新生成数据 {days, seed, scale, corrupt_ratio}
POST /api/query               范围查询 {part_start, part_end, key_min, key_max, strict}
POST /api/merge               触发合并 {part_key, fail_at?, fail_attempts?}
POST /api/corrupt             注入新的文件损坏
POST /api/gc                  垃圾回收（尊重 pinned 快照）
POST /api/verify/concurrent   验证：合并期间并发查询一致性
POST /api/verify/retry        验证：commit 故障回滚 + 重试成功
```

## 测试覆盖（`tests/test_governance.py`，18 项）

模拟器形态 · 分区/文件两级裁剪 · 损坏文件部分恢复与严格模式 ·
四种策略选择 · 合并减文件且保行数 · **合并期间快照隔离** ·
**commit 失败回滚恢复原引用** · write 失败孤儿文件 GC · 重试成功 ·
REPAIR 修复 · GC 尊重 pin · 并发查询一致性报告 · 故障重试报告。
