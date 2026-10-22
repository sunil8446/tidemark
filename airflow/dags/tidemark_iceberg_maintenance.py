"""Iceberg table maintenance through Trino, with before/after measurements.

For each streaming table:
  optimize            rewrite small files into ~128 MB files (Trino's rewrite_data_files)
  expire_snapshots    drop snapshots older than the retention window
  remove_orphan_files delete files no snapshot references (older than the retention window)

File count, bytes, snapshot count and a benchmark query's median latency are
recorded before and after in iceberg.ops.maintenance_runs, which is where
bench/compaction.md comes from.
"""

import statistics
import time
from datetime import datetime, timedelta, timezone

import trino
from airflow.sdk import dag, task

RETENTION = "1d"  # local dev; production would keep 7d of time travel
TABLES = ["raw_trades", "book_deltas", "gap_events"]
BENCH = {
    "raw_trades": "SELECT venue, product_id, count(*), sum(size) FROM raw_trades "
    "WHERE event_ts > current_timestamp - INTERVAL '1' DAY GROUP BY 1, 2",
    "book_deltas": "SELECT venue, product_id, count(*) FROM book_deltas "
    "WHERE event_ts > current_timestamp - INTERVAL '1' DAY AND checksum_ok GROUP BY 1, 2",
    "gap_events": "SELECT status, count(*) FROM gap_events GROUP BY 1",
}


def connect():
    return trino.dbapi.connect(host="trino", port=8080, user="tidemark", catalog="iceberg", schema="market")


def query(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


def measure(conn, table):
    files, size = query(conn, f'SELECT count(*), coalesce(sum(file_size_in_bytes), 0) FROM "{table}$files"')[0]
    snapshots = query(conn, f'SELECT count(*) FROM "{table}$snapshots"')[0][0]
    query(conn, BENCH[table])  # warm the metadata cache
    timings = []
    for _ in range(5):
        start = time.monotonic()
        query(conn, BENCH[table])
        timings.append((time.monotonic() - start) * 1000)
    return {"files": files, "bytes": size, "snapshots": snapshots, "query_ms": statistics.median(timings)}


@dag(
    dag_id="tidemark_iceberg_maintenance",
    schedule="0 */6 * * *",
    start_date=datetime(2026, 10, 22),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["tidemark", "lakehouse"],
    doc_md=__doc__,
)
def tidemark_iceberg_maintenance():
    @task
    def ensure_ops_table():
        conn = connect()
        query(conn, "CREATE SCHEMA IF NOT EXISTS iceberg.ops")
        query(
            conn,
            """CREATE TABLE IF NOT EXISTS iceberg.ops.maintenance_runs (
                run_ts timestamp(6) with time zone, table_name varchar,
                files_before bigint, files_after bigint, bytes_before bigint, bytes_after bigint,
                snapshots_before bigint, snapshots_after bigint,
                query_ms_before double, query_ms_after double,
                optimize_s double, expire_s double, orphans_s double)
               WITH (format = 'PARQUET')""",
        )

    @task
    def maintain(table: str) -> dict:
        conn = connect()
        before = measure(conn, table)
        t0 = time.monotonic()
        query(conn, f"ALTER TABLE {table} EXECUTE optimize(file_size_threshold => '128MB')")
        t1 = time.monotonic()
        query(conn, f"ALTER TABLE {table} EXECUTE expire_snapshots(retention_threshold => '{RETENTION}')")
        t2 = time.monotonic()
        query(conn, f"ALTER TABLE {table} EXECUTE remove_orphan_files(retention_threshold => '{RETENTION}')")
        t3 = time.monotonic()
        after = measure(conn, table)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        query(
            conn,
            f"""INSERT INTO iceberg.ops.maintenance_runs VALUES (
                from_iso8601_timestamp('{now}'), '{table}',
                {before['files']}, {after['files']}, {before['bytes']}, {after['bytes']},
                {before['snapshots']}, {after['snapshots']},
                {before['query_ms']:.3f}, {after['query_ms']:.3f},
                {t1 - t0:.3f}, {t2 - t1:.3f}, {t3 - t2:.3f})""",
        )
        result = {"table": table, "before": before, "after": after, "optimize_s": round(t1 - t0, 2)}
        print(result)
        return result

    ensure_ops_table() >> maintain.expand(table=TABLES)


tidemark_iceberg_maintenance()
