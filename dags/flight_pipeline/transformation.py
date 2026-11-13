"""Phase 3 - Transformation & KPI computation.

Input : stg_flight_prices_clean (MySQL)
Output: trf_flight_prices_enriched + 5 KPI tables (MySQL), loaded to PostgreSQL in Phase 4.
"""
import logging

import numpy as np
import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

from flight_pipeline import config as cfg
from flight_pipeline import schemas as sc
from flight_pipeline.db_utils import read_df, write_tables_atomically

log = logging.getLogger(__name__)

FARES = ["base_fare_bdt", "tax_surcharge_bdt", "total_fare_bdt"]


# --------------------------------------------------------------------------
# Pure functions (no database)
# --------------------------------------------------------------------------
def enrich(df: pd.DataFrame) -> pd.DataFrame:
    e = df.copy()
    for c in FARES + ["duration_hrs"]:
        e[c] = pd.to_numeric(e[c], errors="coerce").astype("float64")
    e["days_before_departure"] = pd.to_numeric(e["days_before_departure"], errors="coerce").astype("Int64")
    for c in ["departure_datetime", "arrival_datetime"]:
        e[c] = pd.to_datetime(e[c], errors="coerce")

    # Total Fare = Base Fare + Tax & Surcharge (lab formula).
    # Keep the source value; replace it only where missing or inconsistent.
    computed = (e["base_fare_bdt"] + e["tax_surcharge_bdt"]).round(2)
    e["reported_total_fare_bdt"] = e["total_fare_bdt"]
    needs_fix = e["total_fare_bdt"].isna() | (
        (e["total_fare_bdt"] - computed).abs() > cfg.FARE_TOLERANCE_BDT
    )
    e["total_fare_bdt"] = np.where(needs_fix, computed, e["total_fare_bdt"])
    e["total_fare_recomputed"] = needs_fix.astype(bool)

    # Route attributes
    e["route"] = e["source"] + "-" + e["destination"]
    domestic = e["source"].isin(cfg.DOMESTIC_AIRPORTS) & e["destination"].isin(cfg.DOMESTIC_AIRPORTS)
    e["route_type"] = np.where(domestic, "Domestic", "International")

    # Season attributes
    e["is_peak_season"] = e["seasonality"].isin(cfg.PEAK_SEASONS).astype(bool)
    e["season_type"] = np.where(e["is_peak_season"], "Peak", "Non-Peak")

    # Date attributes
    e["departure_date"] = e["departure_datetime"].dt.date
    e["departure_month"] = e["departure_datetime"].dt.month.astype("Int64")
    e["departure_day_of_week"] = e["departure_datetime"].dt.day_name()
    return e


def _money(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    for c in cols:
        df[c] = df[c].astype("float64").round(2)
    return df


def compute_kpis(e: pd.DataFrame) -> dict:
    kpis = {}
    total_bookings = len(e)

    # KPI 1 - Average fare by airline
    a = (
        e.groupby("airline")
        .agg(
            booking_count=("total_fare_bdt", "size"),
            avg_total_fare_bdt=("total_fare_bdt", "mean"),
            median_total_fare_bdt=("total_fare_bdt", "median"),
            min_total_fare_bdt=("total_fare_bdt", "min"),
            max_total_fare_bdt=("total_fare_bdt", "max"),
            avg_base_fare_bdt=("base_fare_bdt", "mean"),
            avg_tax_surcharge_bdt=("tax_surcharge_bdt", "mean"),
        )
        .reset_index()
    )
    a = _money(a, [c for c in a.columns if c.endswith("_bdt")])
    a["fare_rank"] = a["avg_total_fare_bdt"].rank(ascending=False, method="min").astype(int)
    kpis[sc.KPI_AVG_FARE] = a.sort_values("fare_rank")

    # KPI 2 - Booking count by airline
    b = e.groupby("airline").size().reset_index(name="booking_count")
    b["booking_share_pct"] = (b["booking_count"] / total_bookings * 100).round(2)
    b["booking_rank"] = b["booking_count"].rank(ascending=False, method="min").astype(int)
    kpis[sc.KPI_BOOKINGS] = b.sort_values("booking_rank")

    # KPI 3a - Seasonal fare variation per season, vs the non-peak baseline
    baseline = e.loc[~e["is_peak_season"], "total_fare_bdt"].mean()
    s = (
        e.groupby(["seasonality", "season_type"])
        .agg(
            booking_count=("total_fare_bdt", "size"),
            avg_total_fare_bdt=("total_fare_bdt", "mean"),
            median_total_fare_bdt=("total_fare_bdt", "median"),
            avg_base_fare_bdt=("base_fare_bdt", "mean"),
        )
        .reset_index()
    )
    s["pct_diff_vs_non_peak"] = ((s["avg_total_fare_bdt"] - baseline) / baseline * 100).round(2)
    s = _money(s, ["avg_total_fare_bdt", "median_total_fare_bdt", "avg_base_fare_bdt"])
    kpis[sc.KPI_SEASONAL] = s.sort_values("avg_total_fare_bdt", ascending=False)

    # KPI 3b - Peak vs non-peak, overall and per airline
    def peak_vs_non_peak(frame: pd.DataFrame, label: str) -> dict:
        peak = frame.loc[frame["is_peak_season"], "total_fare_bdt"]
        non_peak = frame.loc[~frame["is_peak_season"], "total_fare_bdt"]
        p_avg = peak.mean() if len(peak) else np.nan
        n_avg = non_peak.mean() if len(non_peak) else np.nan
        diff = p_avg - n_avg
        return {
            "airline": label,
            "peak_booking_count": int(len(peak)),
            "non_peak_booking_count": int(len(non_peak)),
            "peak_avg_fare_bdt": p_avg,
            "non_peak_avg_fare_bdt": n_avg,
            "fare_difference_bdt": diff,
            "pct_variation": (diff / n_avg * 100) if n_avg else np.nan,
        }

    rows = [peak_vs_non_peak(e, "ALL AIRLINES")]
    rows += [peak_vs_non_peak(g, name) for name, g in e.groupby("airline")]
    p = pd.DataFrame(rows)
    p = _money(p, ["peak_avg_fare_bdt", "non_peak_avg_fare_bdt", "fare_difference_bdt"])
    p["pct_variation"] = p["pct_variation"].round(2)
    kpis[sc.KPI_PEAK] = p

    # KPI 4 - Most popular routes
    r = (
        e.groupby(["route", "source", "source_name", "destination", "destination_name", "route_type"])
        .agg(booking_count=("total_fare_bdt", "size"), avg_total_fare_bdt=("total_fare_bdt", "mean"))
        .reset_index()
    )
    r = _money(r, ["avg_total_fare_bdt"])
    r["route_rank"] = r["booking_count"].rank(ascending=False, method="min").astype(int)
    kpis[sc.KPI_ROUTES] = r.sort_values(["route_rank", "route"])

    return kpis


# --------------------------------------------------------------------------
# Airflow task functions
# --------------------------------------------------------------------------
def transform_and_enrich(batch_id: str) -> dict:
    hook = MySqlHook(mysql_conn_id=cfg.MYSQL_CONN_ID)
    df = read_df(hook, f"SELECT * FROM {cfg.CLEAN_TABLE}")
    if df.empty:
        raise ValueError("Clean table is empty - nothing to transform.")

    e = enrich(df)
    e["batch_id"] = batch_id

    hook.run(sc.ddl(sc.ENRICHED_TABLE, "mysql"))
    write_tables_atomically(hook, [{
        "table": sc.ENRICHED_TABLE,
        "df": e,
        "columns": sc.columns(sc.ENRICHED_TABLE),
        "delete_sql": f"DELETE FROM {sc.ENRICHED_TABLE}",
    }])

    summary = {
        "rows_enriched": int(len(e)),
        "total_fare_recomputed": int(e["total_fare_recomputed"].sum()),
        "peak_rows": int(e["is_peak_season"].sum()),
        "domestic_rows": int((e["route_type"] == "Domestic").sum()),
    }
    log.info("Transformation summary: %s", summary)
    return summary


def compute_and_store_kpis(batch_id: str) -> dict:
    hook = MySqlHook(mysql_conn_id=cfg.MYSQL_CONN_ID)
    e = read_df(hook, f"SELECT * FROM {sc.ENRICHED_TABLE}")
    for c in sc.boolean_columns(sc.ENRICHED_TABLE):
        e[c] = e[c].astype(int).astype(bool)  # MySQL returns BOOLEAN as 0/1
    for c in FARES:
        e[c] = pd.to_numeric(e[c]).astype("float64")

    kpis = compute_kpis(e)
    writes = []
    for table, kdf in kpis.items():
        kdf["batch_id"] = batch_id
        hook.run(sc.ddl(table, "mysql"))
        writes.append({"table": table, "df": kdf, "columns": sc.columns(table),
                       "delete_sql": f"DELETE FROM {table}"})
    write_tables_atomically(hook, writes)

    overall = kpis[sc.KPI_PEAK].iloc[0].to_dict()
    summary = {
        "kpi_rows": {t: int(len(d)) for t, d in kpis.items()},
        "top_5_avg_fare": kpis[sc.KPI_AVG_FARE].head(5)[["airline", "avg_total_fare_bdt"]].values.tolist(),
        "top_5_bookings": kpis[sc.KPI_BOOKINGS].head(5)[["airline", "booking_count"]].values.tolist(),
        "top_5_routes": kpis[sc.KPI_ROUTES].head(5)[["route", "booking_count"]].values.tolist(),
        "seasonal": kpis[sc.KPI_SEASONAL][["seasonality", "avg_total_fare_bdt", "pct_diff_vs_non_peak"]].values.tolist(),
        "peak_vs_non_peak_overall": {k: (float(v) if isinstance(v, (int, float, np.number)) else v)
                                     for k, v in overall.items() if k != "batch_id"},
    }
    log.info("KPI summary: %s", summary)
    return summary


def reconcile_kpis(transform_summary: dict) -> dict:
    """Sanity checks proving the KPIs are consistent with the enriched data."""
    hook = MySqlHook(mysql_conn_id=cfg.MYSQL_CONN_ID)
    n = transform_summary["rows_enriched"]
    checks = {
        "enriched_rows_match_clean": (
            hook.get_first(f"SELECT COUNT(*) FROM {sc.ENRICHED_TABLE}")[0],
            hook.get_first(f"SELECT COUNT(*) FROM {cfg.CLEAN_TABLE}")[0],
        ),
        "total_equals_base_plus_tax": (
            hook.get_first(
                f"SELECT COUNT(*) FROM {sc.ENRICHED_TABLE} "
                f"WHERE ABS(total_fare_bdt - (base_fare_bdt + tax_surcharge_bdt)) > %s",
                parameters=(cfg.FARE_TOLERANCE_BDT,),
            )[0],
            0,
        ),
        "airline_bookings_sum_to_total": (
            int(hook.get_first(f"SELECT SUM(booking_count) FROM {sc.KPI_BOOKINGS}")[0]), n),
        "route_bookings_sum_to_total": (
            int(hook.get_first(f"SELECT SUM(booking_count) FROM {sc.KPI_ROUTES}")[0]), n),
        "season_bookings_sum_to_total": (
            int(hook.get_first(f"SELECT SUM(booking_count) FROM {sc.KPI_SEASONAL}")[0]), n),
        "booking_share_sums_to_100": (
            round(float(hook.get_first(f"SELECT SUM(booking_share_pct) FROM {sc.KPI_BOOKINGS}")[0])), 100),
    }
    failed = {k: v for k, v in checks.items() if v[0] != v[1]}
    if failed:
        raise ValueError(f"KPI reconciliation FAILED (actual, expected): {failed}")
    log.info("KPI reconciliation PASSED: %s", {k: v[0] for k, v in checks.items()})
    return {k: v[0] for k, v in checks.items()}
