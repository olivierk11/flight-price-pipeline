"""Phase 4 - Load the enriched data and KPI tables from MySQL into PostgreSQL.

Latency    : PostgreSQL COPY FROM STDIN (bulk streaming) instead of row-by-row INSERTs.
Consistency: source tables read in ONE consistent MySQL snapshot; target TRUNCATE + COPY
             of all tables in ONE PostgreSQL transaction (TRUNCATE is transactional in
             PostgreSQL), so readers see either the full old data or the full new data.
"""
import csv
import io
import logging
import time
from decimal import Decimal

from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from flight_pipeline import config as cfg
from flight_pipeline import schemas as sc

log = logging.getLogger(__name__)

S = cfg.PG_SCHEMA
TABLES_TO_LOAD = [sc.ENRICHED_TABLE] + sc.KPI_TABLES
LOAD_LOG = f"{S}.{cfg.PG_LOAD_LOG_TABLE}"


def _pg(table: str) -> str:
    return f"{S}.{table}"


# --------------------------------------------------------------------------
# Task 1: schema, tables, indexes, views
# --------------------------------------------------------------------------
def _views() -> list:
    return [
        f"""CREATE OR REPLACE VIEW {S}.vw_airline_scorecard AS
SELECT b.booking_rank, a.airline, b.booking_count, b.booking_share_pct,
       a.avg_total_fare_bdt, a.fare_rank,
       p.peak_avg_fare_bdt, p.non_peak_avg_fare_bdt, p.pct_variation AS peak_premium_pct
FROM {_pg(sc.KPI_AVG_FARE)} a
JOIN {_pg(sc.KPI_BOOKINGS)} b USING (airline)
LEFT JOIN {_pg(sc.KPI_PEAK)} p USING (airline)
ORDER BY b.booking_rank, a.airline""",
        f"""CREATE OR REPLACE VIEW {S}.vw_top_10_routes AS
SELECT route_rank, route, source_name, destination_name, route_type,
       booking_count, avg_total_fare_bdt
FROM {_pg(sc.KPI_ROUTES)}
WHERE route_rank <= 10
ORDER BY route_rank, route""",
        f"""CREATE OR REPLACE VIEW {S}.vw_seasonal_fares AS
SELECT seasonality, season_type, booking_count, avg_total_fare_bdt,
       median_total_fare_bdt, pct_diff_vs_non_peak
FROM {_pg(sc.KPI_SEASONAL)}
ORDER BY avg_total_fare_bdt DESC""",
    ]


def create_analytics_schema() -> None:
    hook = PostgresHook(postgres_conn_id=cfg.POSTGRES_CONN_ID)
    enriched = _pg(sc.ENRICHED_TABLE)
    statements = (
        [f"CREATE SCHEMA IF NOT EXISTS {S}"]
        + [sc.ddl(t, "postgres", schema=S) for t in TABLES_TO_LOAD]
        + [
            f"CREATE INDEX IF NOT EXISTS idx_enriched_airline ON {enriched} (airline)",
            f"CREATE INDEX IF NOT EXISTS idx_enriched_route ON {enriched} (route)",
            f"CREATE INDEX IF NOT EXISTS idx_enriched_season ON {enriched} (seasonality)",
            f"""CREATE TABLE IF NOT EXISTS {LOAD_LOG} (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(250) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    rows_loaded INTEGER NOT NULL,
    load_seconds NUMERIC(10,3) NOT NULL,
    loaded_at TIMESTAMP DEFAULT now()
)""",
        ]
        + _views()
    )
    hook.run(statements)  # runs in one transaction, committed at the end
    log.info("Analytics schema '%s' ready: %s tables + load log + 3 views", S, len(TABLES_TO_LOAD))


# --------------------------------------------------------------------------
# Task 2: extract (consistent snapshot) + load (single transaction, COPY)
# --------------------------------------------------------------------------
def _extract_snapshot() -> dict:
    """Read every table inside one consistent MySQL snapshot."""
    conn = MySqlHook(mysql_conn_id=cfg.MYSQL_CONN_ID).get_conn()
    cur = conn.cursor()
    data = {}
    try:
        cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        cur.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        for t in TABLES_TO_LOAD:
            cols = sc.columns(t)
            cur.execute(f"SELECT {', '.join(f'`{c}`' for c in cols)} FROM {t}")
            data[t] = list(cur.fetchall())
        conn.commit()
    finally:
        cur.close()
        conn.close()
    return data


def _csv_buffer(rows: list, bool_positions: set) -> io.StringIO:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    for row in rows:
        writer.writerow([
            r"\N" if v is None
            else ("true" if v else "false") if i in bool_positions
            else v
            for i, v in enumerate(row)
        ])
    buf.seek(0)
    return buf


def load_to_postgres(batch_id: str) -> dict:
    t0 = time.perf_counter()
    data = _extract_snapshot()
    extract_secs = time.perf_counter() - t0

    conn = PostgresHook(postgres_conn_id=cfg.POSTGRES_CONN_ID).get_conn()
    conn.autocommit = False
    cur = conn.cursor()
    stats = []
    try:
        cur.execute("TRUNCATE " + ", ".join(_pg(t) for t in TABLES_TO_LOAD))
        for t in TABLES_TO_LOAD:
            cols = sc.columns(t)
            bools = {cols.index(c) for c in sc.boolean_columns(t)}
            start = time.perf_counter()
            buf = _csv_buffer(data[t], bools)
            col_sql = ", ".join(f'"{c}"' for c in cols)
            cur.copy_expert(
                f"COPY {_pg(t)} ({col_sql}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf
            )
            secs = round(time.perf_counter() - start, 3)
            stats.append((batch_id, t, len(data[t]), secs))
            log.info("COPY %-32s %7d rows in %.3fs", t, len(data[t]), secs)
        cur.executemany(
            f"INSERT INTO {LOAD_LOG} (batch_id, table_name, rows_loaded, load_seconds) "
            f"VALUES (%s, %s, %s, %s)",
            stats,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("PostgreSQL load failed - rolled back, analytics data unchanged.")
        raise
    finally:
        cur.close()
        conn.close()

    summary = {
        "tables_loaded": {t: n for _, t, n, _ in stats},
        "extract_seconds": round(extract_secs, 3),
        "load_seconds": round(sum(s for *_, s in stats), 3),
        "total_seconds": round(time.perf_counter() - t0, 3),
    }
    log.info("PostgreSQL load summary: %s", summary)
    return summary


# --------------------------------------------------------------------------
# Task 3: verify MySQL and PostgreSQL hold identical data
# --------------------------------------------------------------------------
def verify_postgres_load(batch_id: str) -> dict:
    mysql = MySqlHook(mysql_conn_id=cfg.MYSQL_CONN_ID)
    pg = PostgresHook(postgres_conn_id=cfg.POSTGRES_CONN_ID)
    results, failures = {}, {}

    for t in TABLES_TO_LOAD:
        numeric = [c for c, typ in sc.TABLES[t]["columns"] if typ.startswith(("DECIMAL", "INT"))]
        m_sql = "SELECT COUNT(*)" + "".join(f", COALESCE(SUM(`{c}`), 0)" for c in numeric) + f" FROM {t}"
        p_sql = "SELECT COUNT(*)" + "".join(f', COALESCE(SUM("{c}"), 0)' for c in numeric) + f" FROM {_pg(t)}"
        m_vals = [Decimal(str(v)) for v in mysql.get_first(m_sql)]
        p_vals = [Decimal(str(v)) for v in pg.get_first(p_sql)]
        mismatched = [
            name for name, a, b in zip(["row_count"] + numeric, m_vals, p_vals) if a != b
        ]
        batches = [r[0] for r in pg.get_records(f"SELECT DISTINCT batch_id FROM {_pg(t)}")]
        if batches != [batch_id]:
            mismatched.append(f"batch_id {batches} != {batch_id}")
        results[t] = {"rows": int(p_vals[0]), "columns_checksummed": len(numeric)}
        if mismatched:
            failures[t] = mismatched

    if failures:
        raise ValueError(f"MySQL vs PostgreSQL mismatch: {failures}")
    log.info("PostgreSQL verification PASSED (row counts + column checksums): %s", results)
    return results
