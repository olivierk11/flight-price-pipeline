"""Phase 1 - Ingestion: load the raw CSV into the MySQL staging table."""
import logging

import numpy as np
import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

from flight_pipeline import config as cfg

log = logging.getLogger(__name__)


def _staging_ddl() -> str:
    cols = ",\n    ".join(f"`{col}` {sql_type}" for _, col, sql_type in cfg.STAGING_COLUMNS)
    # Business columns are deliberately NULLable: staging must accept the raw data
    # as-is; nulls and bad values are handled in the validation phase.
    return f"""
CREATE TABLE IF NOT EXISTS {cfg.STAGING_TABLE} (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `source_row_number` INT NOT NULL,
    {cols},
    `batch_id` VARCHAR(250) NOT NULL,
    `ingested_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_airline (`airline`),
    INDEX idx_route (`source`, `destination`),
    INDEX idx_batch (`batch_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def create_staging_table() -> None:
    hook = MySqlHook(mysql_conn_id=cfg.MYSQL_CONN_ID)
    hook.run(_staging_ddl())
    log.info("Staging table %s is ready.", cfg.STAGING_TABLE)


def read_csv_typed(path: str):
    """Read the CSV as text, then convert each column to its target type.

    Returns the typed DataFrame and a report of how many non-empty values
    could not be converted (those become NULL and are handled in validation).
    """
    raw = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    raw.columns = [c.strip() for c in raw.columns]

    missing = [c for c in cfg.COLUMN_MAP if c not in raw.columns]
    if missing:
        raise ValueError(f"CSV structure changed - missing columns: {missing}")
    extra = [c for c in raw.columns if c not in cfg.COLUMN_MAP]
    if extra:
        log.warning("Ignoring unexpected extra columns: %s", extra)

    df = raw[list(cfg.COLUMN_MAP)].rename(columns=cfg.COLUMN_MAP)
    df = df.apply(lambda s: s.str.strip())
    df = df.mask(df == "")  # empty strings -> NaN (NULL in MySQL)

    conversion_losses = {}

    for col in cfg.DECIMAL_COLUMNS:
        before = df[col].notna().sum()
        df[col] = pd.to_numeric(df[col], errors="coerce").round(4 if col == "duration_hrs" else 2)
        conversion_losses[col] = int(before - df[col].notna().sum())

    for col in cfg.INTEGER_COLUMNS:
        before = df[col].notna().sum()
        num = pd.to_numeric(df[col], errors="coerce")
        num = num.where(num.isna() | (num % 1 == 0))  # reject non-whole numbers
        df[col] = num.astype("Int64")
        conversion_losses[col] = int(before - df[col].notna().sum())

    for col in cfg.DATETIME_COLUMNS:
        before = df[col].notna().sum()
        df[col] = pd.to_datetime(df[col], errors="coerce")
        conversion_losses[col] = int(before - df[col].notna().sum())

    df.insert(0, "source_row_number", range(1, len(df) + 1))
    return df, conversion_losses


def _to_py(v):
    """Convert pandas/numpy values to plain Python types the MySQL driver accepts."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    if isinstance(v, np.generic):
        return v.item()
    return v


def load_csv_to_mysql(batch_id: str) -> dict:
    df, conversion_losses = read_csv_typed(cfg.CSV_PATH)

    columns = list(df.columns) + ["batch_id"]
    col_sql = ", ".join(f"`{c}`" for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f"INSERT INTO {cfg.STAGING_TABLE} ({col_sql}) VALUES ({placeholders})"

    rows = [
        tuple(_to_py(v) for v in rec) + (batch_id,)
        for rec in df.itertuples(index=False, name=None)
    ]

    hook = MySqlHook(mysql_conn_id=cfg.MYSQL_CONN_ID)
    conn = hook.get_conn()
    hook.set_autocommit(conn, False)
    cur = conn.cursor()
    try:
        # DELETE (not TRUNCATE): TRUNCATE commits implicitly, so a failed load
        # would leave staging empty. DELETE + inserts run in ONE transaction,
        # so a re-run either fully replaces the data or changes nothing.
        cur.execute(f"DELETE FROM {cfg.STAGING_TABLE}")
        for start in range(0, len(rows), cfg.INSERT_BATCH_SIZE):
            cur.executemany(insert_sql, rows[start:start + cfg.INSERT_BATCH_SIZE])
            log.info("Inserted %s / %s rows", min(start + cfg.INSERT_BATCH_SIZE, len(rows)), len(rows))
        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("Load failed - transaction rolled back, staging unchanged.")
        raise
    finally:
        cur.close()
        conn.close()

    summary = {
        "batch_id": batch_id,
        "csv_rows": int(len(df)),
        "csv_total_fare_sum": float(df["total_fare_bdt"].sum()),
        "conversion_losses": conversion_losses,
    }
    log.info("Load summary: %s", summary)
    return summary


def verify_staging_load(load_summary: dict) -> dict:
    hook = MySqlHook(mysql_conn_id=cfg.MYSQL_CONN_ID)
    t = cfg.STAGING_TABLE
    batch_id = load_summary["batch_id"]

    # 1. Row counts: everything from the CSV is there, and nothing stale is left
    batch_rows = hook.get_first(f"SELECT COUNT(*) FROM {t} WHERE batch_id = %s", parameters=(batch_id,))[0]
    total_rows = hook.get_first(f"SELECT COUNT(*) FROM {t}")[0]
    if batch_rows != load_summary["csv_rows"]:
        raise ValueError(f"Row count mismatch: CSV={load_summary['csv_rows']} MySQL={batch_rows}")
    if total_rows != batch_rows:
        raise ValueError(f"Staging has {total_rows - batch_rows} rows from other batches")

    # 2. Column types in MySQL match the intended schema
    actual = dict(hook.get_records(
        "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
        parameters=(t,),
    ))
    type_errors = {
        col: (sql_type, actual.get(col))
        for _, col, sql_type in cfg.STAGING_COLUMNS
        if actual.get(col) != sql_type.split("(")[0].lower()
    }
    if type_errors:
        raise ValueError(f"Column type mismatch (expected, actual): {type_errors}")

    # 3. Values survived the transfer: fare totals match the CSV
    db_fare_sum = float(hook.get_first(f"SELECT COALESCE(SUM(total_fare_bdt), 0) FROM {t}")[0])
    if abs(db_fare_sum - load_summary["csv_total_fare_sum"]) > 1.0:
        raise ValueError(f"Total fare sum mismatch: CSV={load_summary['csv_total_fare_sum']} MySQL={db_fare_sum}")

    # 4. Report values that could not be type-converted (handled in Phase 2)
    losses = {k: v for k, v in load_summary["conversion_losses"].items() if v}
    if losses:
        log.warning("Values that could not be converted and were stored as NULL: %s", losses)

    result = {"rows_loaded": int(batch_rows), "total_fare_sum": db_fare_sum, "conversion_losses": losses}
    log.info("Staging verification PASSED: %s", result)
    return result
