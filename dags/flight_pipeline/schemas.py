"""Database-neutral schemas for the transformed and KPI tables.

Defined once, used to generate DDL for MySQL (Phase 3) and PostgreSQL (Phase 4),
so both databases always have identical structures.
Neutral types: VARCHAR(n), DECIMAL(p,s), INT, DATETIME, DATE, BOOLEAN.
"""
import re

MONEY = "DECIMAL(14,2)"
PCT = "DECIMAL(8,2)"

ENRICHED_TABLE = "trf_flight_prices_enriched"
KPI_AVG_FARE = "kpi_avg_fare_by_airline"
KPI_BOOKINGS = "kpi_booking_count_by_airline"
KPI_SEASONAL = "kpi_seasonal_fare_variation"
KPI_PEAK = "kpi_peak_vs_non_peak"
KPI_ROUTES = "kpi_popular_routes"

KPI_TABLES = [KPI_AVG_FARE, KPI_BOOKINGS, KPI_SEASONAL, KPI_PEAK, KPI_ROUTES]

TABLES = {
    ENRICHED_TABLE: {
        "pk": ["source_row_number"],
        "columns": [
            ("source_row_number", "INT"),
            ("airline", "VARCHAR(100)"),
            ("source", "VARCHAR(10)"),
            ("source_name", "VARCHAR(150)"),
            ("destination", "VARCHAR(10)"),
            ("destination_name", "VARCHAR(150)"),
            ("route", "VARCHAR(25)"),
            ("route_type", "VARCHAR(15)"),
            ("departure_datetime", "DATETIME"),
            ("arrival_datetime", "DATETIME"),
            ("departure_date", "DATE"),
            ("departure_month", "INT"),
            ("departure_day_of_week", "VARCHAR(10)"),
            ("duration_hrs", "DECIMAL(10,4)"),
            ("stopovers", "VARCHAR(20)"),
            ("aircraft_type", "VARCHAR(100)"),
            ("travel_class", "VARCHAR(30)"),
            ("booking_source", "VARCHAR(100)"),
            ("base_fare_bdt", MONEY),
            ("tax_surcharge_bdt", MONEY),
            ("reported_total_fare_bdt", MONEY),
            ("total_fare_bdt", MONEY),
            ("total_fare_recomputed", "BOOLEAN"),
            ("seasonality", "VARCHAR(50)"),
            ("season_type", "VARCHAR(10)"),
            ("is_peak_season", "BOOLEAN"),
            ("days_before_departure", "INT"),
            ("dq_flags", "VARCHAR(1000)"),
            ("batch_id", "VARCHAR(250)"),
        ],
    },
    KPI_AVG_FARE: {
        "pk": ["airline"],
        "columns": [
            ("airline", "VARCHAR(100)"),
            ("booking_count", "INT"),
            ("avg_total_fare_bdt", MONEY),
            ("median_total_fare_bdt", MONEY),
            ("min_total_fare_bdt", MONEY),
            ("max_total_fare_bdt", MONEY),
            ("avg_base_fare_bdt", MONEY),
            ("avg_tax_surcharge_bdt", MONEY),
            ("fare_rank", "INT"),
            ("batch_id", "VARCHAR(250)"),
        ],
    },
    KPI_BOOKINGS: {
        "pk": ["airline"],
        "columns": [
            ("airline", "VARCHAR(100)"),
            ("booking_count", "INT"),
            ("booking_share_pct", PCT),
            ("booking_rank", "INT"),
            ("batch_id", "VARCHAR(250)"),
        ],
    },
    KPI_SEASONAL: {
        "pk": ["seasonality"],
        "columns": [
            ("seasonality", "VARCHAR(50)"),
            ("season_type", "VARCHAR(10)"),
            ("booking_count", "INT"),
            ("avg_total_fare_bdt", MONEY),
            ("median_total_fare_bdt", MONEY),
            ("avg_base_fare_bdt", MONEY),
            ("pct_diff_vs_non_peak", PCT),
            ("batch_id", "VARCHAR(250)"),
        ],
    },
    KPI_PEAK: {
        "pk": ["airline"],
        "columns": [
            ("airline", "VARCHAR(100)"),  # 'ALL AIRLINES' row = overall comparison
            ("peak_booking_count", "INT"),
            ("non_peak_booking_count", "INT"),
            ("peak_avg_fare_bdt", MONEY),
            ("non_peak_avg_fare_bdt", MONEY),
            ("fare_difference_bdt", MONEY),
            ("pct_variation", PCT),
            ("batch_id", "VARCHAR(250)"),
        ],
    },
    KPI_ROUTES: {
        "pk": ["route"],
        "columns": [
            ("route", "VARCHAR(25)"),
            ("source", "VARCHAR(10)"),
            ("source_name", "VARCHAR(150)"),
            ("destination", "VARCHAR(10)"),
            ("destination_name", "VARCHAR(150)"),
            ("route_type", "VARCHAR(15)"),
            ("booking_count", "INT"),
            ("avg_total_fare_bdt", MONEY),
            ("route_rank", "INT"),
            ("batch_id", "VARCHAR(250)"),
        ],
    },
}


def columns(table: str) -> list:
    return [c for c, _ in TABLES[table]["columns"]]


def boolean_columns(table: str) -> list:
    return [c for c, t in TABLES[table]["columns"] if t == "BOOLEAN"]


def _map_type(neutral: str, dialect: str) -> str:
    if dialect == "postgres":
        if neutral == "DATETIME":
            return "TIMESTAMP"
        if neutral == "INT":
            return "INTEGER"
        return re.sub(r"^DECIMAL", "NUMERIC", neutral)
    return neutral  # MySQL accepts the neutral names (BOOLEAN = TINYINT(1))


def ddl(table: str, dialect: str = "mysql", schema: str = None) -> str:
    q = "`" if dialect == "mysql" else '"'
    name = f"{schema}.{table}" if schema else table
    spec = TABLES[table]
    cols = [f"{q}{c}{q} {_map_type(t, dialect)}" for c, t in spec["columns"]]
    cols.append(f"PRIMARY KEY ({', '.join(q + c + q for c in spec['pk'])})")
    body = ",\n    ".join(cols)
    suffix = " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4" if dialect == "mysql" else ""
    return f"CREATE TABLE IF NOT EXISTS {name} (\n    {body}\n){suffix}"
