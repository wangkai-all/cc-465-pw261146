"""lakegov — 湖表分区治理工具（模拟）。

模块划分：
- config     全局配置（路径、阈值、成本模型参数）
- metastore  SQLite 元数据存储，MVCC 快照隔离
- storage    数据文件读写、校验和、损坏模拟与部分恢复
- simulator  数据集生成：分区倾斜 / 小文件爆炸 / 冷热分层 / 文件损坏
- query      查询引擎：分区裁剪 + 文件级 min/max 裁剪 + 快照 pin
- planner    治理规划：按查询热度与文件统计选择合并策略 + 成本估计
- compactor  合并执行：原子提交、故障注入、重试、孤儿文件 GC
- verify     并发查询一致性与故障重试的一键验证
- server     HTTP API + 静态页面
"""

__version__ = "0.1.0"
