"""Phase 2 - Validation: check, correct, flag or reject staging rows.

Every rule has a severity:
  REJECT  -> row moves to the rejected table with its reasons
  FLAG    -> row is kept; the issue is recorded in its dq_flags column
  CORRECT -> value is fixed in place (and also recorded as a flag)
"""
import csv
import logging

import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

from flight_pipeline import config as cfg
from flight_pipeline.db_utils import read_df, write_tables_atomically

log = logging.getLogger(__name__)

BUSINESS_COLS = [c for _, c, _ in cfg.STAGING_COLUMNS]
TEXT_COLS = [c for _, c, t in cfg.STAGING_COLUMNS if t.startswith("VARCHAR")]
FARE_COLS = ["base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt"]
REQUIRED_TEXT_COLS = ["airline", "source", "destination"]
OPTIONAL_TEXT_COLS = [c for c in TEXT_COLS if c not in REQUIRED_TEXT_COLS]


# --------------------------------------------------------------------------
# Task 1: required columns exist
# --------------------------------------------------------------------------
def check_required_columns() -> dict:
    with open(cfg.CSV_PATH, encoding="utf-8-sig", newline="") as f:
        csv_header = [h.strip() for h in next(csv.reader(f))]
    missing_in_csv = [c for c in cfg.REQUIRED_CSV_COLUMNS if c not in csv_header]

    hook = MySqlHook(mysql_conn_id=cfg.MYSQL_CONN_ID)
    staging_cols = {r[0] for r in hook.get_records(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
        parameters=(cfg.STAGING_TABLE,),
    )}
    required_staging = [cfg.COLUMN_MAP[c] for c in cfg.REQUIRED_CSV_COLUMNS]
    missing_in_staging = [c for c in required_staging if c not in staging_cols]

    if missing_in_csv or missing_in_staging:
        raise ValueError(
            f"Required columns missing. CSV: {missing_in_csv} | staging: {missing_in_staging}"
        )
    result = {"required_columns": cfg.REQUIRED_CSV_COLUMNS, "status": "PASSED"}
    log.info("Required column check PASSED: %s", result)
    return result


# --------------------------------------------------------------------------
# Core rules (pure pandas, no database - easy to unit test)
# --------------------------------------------------------------------------
def normalise_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in FARE_COLS + ["duration_hrs"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
    df["days_before_departure"] = pd.to_numeric(df["days_before_departure"], errors="coerce").astype("Int64")
    for c in cfg.DATETIME_COLUMNS:
        df[c] = pd.to_datetime(df[c], errors="coerce")
    for c in TEXT_COLS:
        s = df[c].astype("string").str.strip().str.replace(r"\s+", " ", regex=True)
        df[c] = s.mask(s == "")
    df["source"] = df["source"].str.upper()
    df["destination"] = df["destination"].str.upper()
    return df


def apply_rules(df: pd.DataFrame):
    """Returns (clean_df, rejected_df, rule_stats)."""
    df = normalise_types(df)
    marks = {
        "REJECT": pd.Series("", index=df.index, dtype="object"),
        "FLAG": pd.Series("", index=df.index, dtype="object"),
    }
    stats = []

    def rule(mask, name, severity, action):
        mask = pd.Series(mask, index=df.index).fillna(False).astype(bool)
        n = int(mask.sum())
        stats.append({"rule_name": name, "severity": severity, "affected_rows": n, "action_taken": action})
        if n:
            marks[severity] = marks[severity].where(~mask, marks[severity] + name + ";")
        return mask

    # -- 1. Missing / empty required categorical fields ----------------------
    for c in REQUIRED_TEXT_COLS:
        rule(df[c].isna(), f"missing_{c}", "REJECT", "row rejected")

    # -- 2. Data types: categorical fields must be real text ----------------
    for c in REQUIRED_TEXT_COLS + ["source_name", "destination_name"]:
        no_letters = df[c].notna() & ~df[c].str.contains(r"[A-Za-z]", regex=True).fillna(False)
        sev = "REJECT" if c in REQUIRED_TEXT_COLS else "FLAG"
        rule(no_letters, f"invalid_text_{c}", sev, "row rejected" if sev == "REJECT" else "kept, flagged")

    # -- 3. Missing fares: derive one missing component, reject if 2+ missing
    n_missing = df[FARE_COLS].isna().sum(axis=1)
    rule(n_missing >= 2, "missing_fare_values", "REJECT", "row rejected (cannot derive)")

    m = rule((n_missing == 1) & df["base_fare_bdt"].isna(), "derived_base_fare", "FLAG",
             "base = total - tax")
    df.loc[m, "base_fare_bdt"] = df.loc[m, "total_fare_bdt"] - df.loc[m, "tax_surcharge_bdt"]

    m = rule((n_missing == 1) & df["tax_surcharge_bdt"].isna(), "derived_tax_surcharge", "FLAG",
             "tax = total - base")
    df.loc[m, "tax_surcharge_bdt"] = df.loc[m, "total_fare_bdt"] - df.loc[m, "base_fare_bdt"]

    rule((n_missing == 1) & df["total_fare_bdt"].isna(), "missing_total_fare", "FLAG",
         "computed in transformation phase")

    # -- 4. Fare inconsistencies -------------------------------------------
    rule((df[FARE_COLS] < 0).any(axis=1), "negative_fare", "REJECT", "row rejected")
    rule(df["base_fare_bdt"] == 0, "zero_base_fare", "FLAG", "kept, flagged")
    mismatch = (df["total_fare_bdt"] - (df["base_fare_bdt"] + df["tax_surcharge_bdt"])).abs() > cfg.FARE_TOLERANCE_BDT
    rule(mismatch, "total_fare_mismatch", "FLAG", "recomputed in transformation phase")

    # -- 5. Invalid city / airport codes -----------------------------------
    for c in ["source", "destination"]:
        bad_format = df[c].notna() & ~df[c].str.fullmatch(r"[A-Z]{3}").fillna(False)
        rule(bad_format, f"invalid_{c}_code", "REJECT", "row rejected")
        unknown = df[c].notna() & ~bad_format & ~df[c].isin(list(cfg.KNOWN_AIRPORTS))
        rule(unknown, f"unknown_{c}_airport", "FLAG", "kept, flagged")
    rule(df["source"] == df["destination"], "same_source_destination", "REJECT", "row rejected")

    for code_col, name_col in [("source", "source_name"), ("destination", "destination_name")]:
        n_names = df.groupby(code_col)[name_col].transform("nunique")
        rule(n_names > 1, f"inconsistent_{code_col}_name", "FLAG", "kept, flagged")

    # -- 6. Other consistency checks ---------------------------------------
    for c in cfg.DATETIME_COLUMNS:
        rule(df[c].isna(), f"missing_{c}", "FLAG", "kept, flagged")
    rule(df["arrival_datetime"] < df["departure_datetime"], "arrival_before_departure", "FLAG", "kept, flagged")
    rule(df["duration_hrs"] <= 0, "non_positive_duration", "FLAG", "kept, flagged")
    rule(df["days_before_departure"] < 0, "negative_days_before_departure", "FLAG", "kept, flagged")
    rule(df.duplicated(subset=BUSINESS_COLS, keep="first"), "duplicate_record", "FLAG", "kept, flagged")

    # -- 7. Fill missing optional categorical values -----------------------
    for c in OPTIONAL_TEXT_COLS:
        m = rule(df[c].isna(), f"filled_missing_{c}", "FLAG", "filled with 'Unknown'")
        df.loc[m, c] = "Unknown"

    rejected_mask = marks["REJECT"] != ""
    clean = df[~rejected_mask].copy()
    clean["dq_flags"] = marks["FLAG"][~rejected_mask].str.rstrip(";").replace("", None)
    rejected = df[rejected_mask].copy()
    rejected["reject_reasons"] = marks["REJECT"][rejected_mask].str.rstrip(";")
    return clean, rejected, stats


def profile(df: pd.DataFrame) -> dict:
    """Quick profile of the clean data - informs the KPI phase."""
    return {
        "seasonality_counts": {str(k): int(v) for k, v in df["seasonality"].value_counts().items()},
        "airline_count": int(df["airline"].nunique()),
        "airports": sorted(set(df["source"].dropna()) | set(df["destination"].dropna())),
        "classes": sorted(df["travel_class"].dropna().unique().tolist()),
    }


# --------------------------------------------------------------------------
# Task 2: validate + write clean / rejected / audit tables
# --------------------------------------------------------------------------
def _typed_cols_ddl() -> str:
    return ",\n    ".join(f"`{col}` {t}" for _, col, t in cfg.STAGING_COLUMNS)


def create_validation_tables(hook) -> None:
    hook.run(f"""
CREATE TABLE IF NOT EXISTS {cfg.CLEAN_TABLE} (
    `source_row_number` INT PRIMARY KEY,
    {_typed_cols_ddl()},
    `dq_flags` VARCHAR(1000),
    `batch_id` VARCHAR(250) NOT NULL,
    `validated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
    hook.run(f"""
CREATE TABLE IF NOT EXISTS {cfg.REJECTED_TABLE} (
    `source_row_number` INT PRIMARY KEY,
    {_typed_cols_ddl()},
    `reject_reasons` VARCHAR(1000) NOT NULL,
    `batch_id` VARCHAR(250) NOT NULL,
    `rejected_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
    hook.run(f"""
CREATE TABLE IF NOT EXISTS {cfg.DQ_LOG_TABLE} (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `batch_id` VARCHAR(250) NOT NULL,
    `rule_name` VARCHAR(100) NOT NULL,
    `severity` VARCHAR(10) NOT NULL,
    `affected_rows` INT NOT NULL,
    `action_taken` VARCHAR(255),
    `checked_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_dq_batch (`batch_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")


def validate_and_clean(batch_id: str) -> dict:
    hook = MySqlHook(mysql_conn_id=cfg.MYSQL_CONN_ID)
    col_sql = ", ".join(f"`{c}`" for c in ["source_row_number"] + BUSINESS_COLS)
    df = read_df(hook, f"SELECT {col_sql} FROM {cfg.STAGING_TABLE}")
    if df.empty:
        raise ValueError("Staging table is empty - nothing to validate.")

    clean, rejected, stats = apply_rules(df)
    clean["batch_id"] = batch_id
    rejected["batch_id"] = batch_id
    stats_df = pd.DataFrame(stats)
    stats_df["batch_id"] = batch_id

    create_validation_tables(hook)
    base_cols = ["source_row_number"] + BUSINESS_COLS
    write_tables_atomically(hook, [
        {"table": cfg.CLEAN_TABLE, "df": clean, "columns": base_cols + ["dq_flags", "batch_id"],
         "delete_sql": f"DELETE FROM {cfg.CLEAN_TABLE}"},
        {"table": cfg.REJECTED_TABLE, "df": rejected, "columns": base_cols + ["reject_reasons", "batch_id"],
         "delete_sql": f"DELETE FROM {cfg.REJECTED_TABLE}"},
        {"table": cfg.DQ_LOG_TABLE, "df": stats_df,
         "columns": ["batch_id", "rule_name", "severity", "affected_rows", "action_taken"],
         "delete_sql": f"DELETE FROM {cfg.DQ_LOG_TABLE} WHERE batch_id = %s",
         "delete_params": (batch_id,)},
    ])

    triggered = {s["rule_name"]: s["affected_rows"] for s in stats if s["affected_rows"]}
    summary = {
        "batch_id": batch_id,
        "total_rows": int(len(df)),
        "clean_rows": int(len(clean)),
        "rejected_rows": int(len(rejected)),
        "reject_ratio": round(len(rejected) / len(df), 6),
        "rules_triggered": triggered,
        "rules_checked": len(stats),
        "profile": profile(clean),
    }
    log.info("Validation summary: %s", summary)
    for s in stats:
        log.info("  %-35s %-6s %7d  %s", s["rule_name"], s["severity"], s["affected_rows"], s["action_taken"])
    return summary


# --------------------------------------------------------------------------
# Task 3: quality gate
# --------------------------------------------------------------------------
def data_quality_gate(summary: dict) -> dict:
    if summary["clean_rows"] == 0:
        raise ValueError("No clean rows left after validation.")
    if summary["reject_ratio"] > cfg.MAX_REJECT_RATIO:
        raise ValueError(
            f"Reject ratio {summary['reject_ratio']:.2%} exceeds limit {cfg.MAX_REJECT_RATIO:.0%}. "
            f"Inspect {cfg.REJECTED_TABLE}."
        )
    log.info("Data quality gate PASSED: %s clean rows, %.2f%% rejected",
             summary["clean_rows"], summary["reject_ratio"] * 100)
    return {"clean_rows": summary["clean_rows"], "reject_ratio": summary["reject_ratio"]}
