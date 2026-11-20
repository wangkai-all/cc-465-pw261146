"""全局配置：路径、治理阈值、成本模型参数。"""
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.environ.get("LAKEGOV_DATA", os.path.join(BASE_DIR, "data"))
DB_PATH = os.environ.get("LAKEGOV_DB", os.path.join(DATA_DIR, "metastore.db"))

# ---- 文件大小阈值（模拟环境按比例缩小，单位：字节） ----
TARGET_FILE_SIZE_HOT = 256 * 1024     # 热区分合并目标大小 256KB
TARGET_FILE_SIZE_COLD = 1024 * 1024   # 冷区归档合并目标大小 1MB
SMALL_FILE_THRESHOLD = 64 * 1024      # < 64KB 记为小文件

# ---- 策略选择阈值 ----
SKEW_FACTOR = 3.0              # 分区大小超过兄弟分区中位数 3 倍 => 倾斜
OVERLAP_RATIO_THRESHOLD = 0.3  # 文件键范围重叠度超过该值 => 裁剪效果差
SMALL_FILE_RATIO = 0.5         # 小文件占比超过该值 => 小文件爆炸
MIN_FILES_TO_COMPACT = 3       # 文件数少于此值不值得合并
HOT_QUERY_THRESHOLD = 3        # 近期命中查询数超过该值 => 热查询分区

# ---- 成本模型（模拟吞吐） ----
HOT_READ_MBPS = 80.0
COLD_READ_MBPS = 25.0
WRITE_MBPS = 60.0
COMMIT_BASE_MS = 200.0
COMMIT_PER_FILE_MS = 5.0

# ---- 合并重试 ----
MAX_ATTEMPTS = 3
RETRY_BACKOFF_BASE_S = 0.2

# ---- 查询模拟 ----
MAX_ROWS_RETURN = 200          # API 返回的最大行数（防止响应过大）
SIMULATE_LATENCY = True        # 按扫描字节数模拟读延迟（测试中关闭）
