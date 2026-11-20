"""HTTP API + 静态页面（仅标准库）。

运行：python3 -m lakegov.server [--port 8000]
"""
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from . import compactor, config, metastore, planner, query, simulator, verify

DEFAULT_TABLE = "events"


# ---------------- 聚合视图 ----------------

def overview(table):
    conn = metastore.connect()
    try:
        snap = metastore.current_snapshot(conn, table)
        parts = []
        tot_files = tot_small = tot_corrupt = tot_bytes = 0
        for p in metastore.list_partitions(conn, table):
            st = planner.partition_stats(conn, table, p["part_key"], snap)
            if not st:
                continue
            st.pop("files", None)
            parts.append(st)
            tot_files += st["file_count"]
            tot_small += st["small_files"]
            tot_corrupt += st["corrupt_files"]
            tot_bytes += st["total_bytes"]
        sizes = sorted(p["total_bytes"] for p in parts)
        median = sizes[len(sizes) // 2] if sizes else 0
        return {
            "table": table,
            "snapshot": snap,
            "kpi": {
                "partitions": len(parts),
                "files": tot_files,
                "total_bytes": tot_bytes,
                "small_files": tot_small,
                "small_ratio": (round(tot_small / tot_files, 3)
                                if tot_files else 0),
                "corrupt_files": tot_corrupt,
                "median_part_bytes": median,
                "skewed_parts": sum(1 for p in parts
                                    if p["skew_factor"] >= config.SKEW_FACTOR),
            },
            "partitions": parts,
            "snapshots": metastore.list_snapshots(conn, table, limit=30),
            "jobs": compactor.list_jobs(conn, table, limit=30),
            "queries": metastore.recent_queries(conn, table, limit=20),
        }
    finally:
        conn.close()


def file_size_histogram(table, buckets=12):
    conn = metastore.connect()
    try:
        files = metastore.all_live_files(conn, table)
        if not files:
            return {"buckets": []}
        sizes = [f["size_bytes"] for f in files]
        lo, hi = min(sizes), max(sizes)
        step = max(1, (hi - lo + 1) // buckets)
        counts = [0] * (buckets + 1)
        for s in sizes:
            counts[min((s - lo) // step, buckets)] += 1
        labels = []
        for i in range(buckets + 1):
            a = lo + i * step
            labels.append(f"{a // 1024}K")
        return {"buckets": [{"label": labels[i], "count": counts[i]}
                            for i in range(buckets + 1) if counts[i] or i < buckets],
                "small_threshold": config.SMALL_FILE_THRESHOLD}
    finally:
        conn.close()


# ---------------- HTTP 层 ----------------

class Handler(BaseHTTPRequestHandler):
    server_version = "lakegov/0.1"

    def _json(self, obj, status=200):
        body = json.dumps(obj, ensure_ascii=False, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        if not n:
            return {}
        return json.loads(self.rfile.read(n) or b"{}")

    def log_message(self, *a):  # 静音访问日志
        pass

    def do_GET(self):
        u = urlparse(self.path)
        qs = parse_qs(u.query)
        table = qs.get("table", [DEFAULT_TABLE])[0]
        try:
            if u.path == "/" or u.path == "/index.html":
                with open(config.BASE_DIR + "/lakegov/static/index.html",
                          "rb") as f:
                    body = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif u.path == "/api/overview":
                self._json(overview(table))
            elif u.path == "/api/histogram":
                self._json(file_size_histogram(table))
            elif u.path == "/api/plan":
                self._json(planner.plan_merge(table, qs["part_key"][0]))
            elif u.path == "/api/jobs":
                conn = metastore.connect()
                try:
                    self._json({"jobs": compactor.list_jobs(conn, table)})
                finally:
                    conn.close()
            elif u.path == "/api/query_log":
                conn = metastore.connect()
                try:
                    self._json({"queries": metastore.recent_queries(conn, table)})
                finally:
                    conn.close()
            else:
                self._json({"error": "not found"}, 404)
        except Exception as e:  # noqa: BLE001
            self._json({"error": f"{type(e).__name__}: {e}"}, 500)

    def do_POST(self):
        u = urlparse(self.path)
        try:
            body = self._body()
            table = body.get("table", DEFAULT_TABLE)
            if u.path == "/api/simulate":
                result = simulator.generate_table(
                    table=table,
                    days=int(body.get("days", 14)),
                    seed=int(body.get("seed", 42)),
                    scale=float(body.get("scale", 1.0)),
                    corrupt_ratio=float(body.get("corrupt_ratio", 0.05)))
                self._json(result)
            elif u.path == "/api/query":
                r = query.run_query(
                    table,
                    part_start=body.get("part_start") or None,
                    part_end=body.get("part_end") or None,
                    key_min=body.get("key_min"),
                    key_max=body.get("key_max"),
                    strict=bool(body.get("strict", False)))
                r.pop("rows", None)  # 页面只展示统计与样例行数
                self._json(r)
            elif u.path == "/api/merge":
                job_id = compactor.create_job(
                    table, body["part_key"],
                    fail_at=body.get("fail_at") or None,
                    fail_attempts=body.get("fail_attempts") or [])
                if not job_id:
                    self._json({"job_id": None, "status": "NO_OP"})
                else:
                    threading.Thread(target=compactor.run_job, args=(job_id,),
                                     daemon=True).start()
                    self._json({"job_id": job_id, "status": "STARTED"})
            elif u.path == "/api/corrupt":
                self._json({"corrupted": simulator.inject_corruption(
                    table, body.get("part_key"),
                    int(body.get("count", 1)))})
            elif u.path == "/api/gc":
                self._json(compactor.gc(table))
            elif u.path == "/api/verify/concurrent":
                self._json(verify.concurrent_read_report(
                    table, body.get("part_key") or None))
            elif u.path == "/api/verify/retry":
                self._json(verify.retry_report(
                    table, body.get("part_key") or None))
            else:
                self._json({"error": "not found"}, 404)
        except Exception as e:  # noqa: BLE001
            self._json({"error": f"{type(e).__name__}: {e}"}, 500)


def main(port=8000):
    metastore.init_db()
    srv = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"lakegov UI: http://localhost:{port}")
    srv.serve_forever()


if __name__ == "__main__":
    import sys
    port = int(sys.argv[sys.argv.index("--port") + 1]) if "--port" in sys.argv else 8000
    main(port)
