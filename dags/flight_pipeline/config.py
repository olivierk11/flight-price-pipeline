"""Central configuration for the flight price pipeline."""
import os

MYSQL_CONN_ID = "mysql_staging"
POSTGRES_CONN_ID = "postgres_analytics"

CSV_PATH = os.environ.get(
    "FLIGHT_CSV_PATH", "/opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv"
)

STAGING_TABLE = "stg_flight_prices"

# Single source of truth for the staging schema:
# (CSV header, staging column name, MySQL type)
# The DDL, the column mapping and the type verification are all generated from this.
STAGING_COLUMNS = [
    ("Airline",               "airline",               "VARCHAR(100)"),
    ("Source",                "source",                "VARCHAR(10)"),
    ("Source Name",           "source_name",           "VARCHAR(150)"),
    ("Destination",           "destination",           "VARCHAR(10)"),
    ("Destination Name",      "destination_name",      "VARCHAR(150)"),
    ("Departure Date & Time", "departure_datetime",    "DATETIME"),
    ("Arrival Date & Time",   "arrival_datetime",      "DATETIME"),
    ("Duration (hrs)",        "duration_hrs",          "DECIMAL(10,4)"),
    ("Stopovers",             "stopovers",             "VARCHAR(20)"),
    ("Aircraft Type",         "aircraft_type",         "VARCHAR(100)"),
    ("Class",                 "travel_class",          "VARCHAR(30)"),
    ("Booking Source",        "booking_source",        "VARCHAR(100)"),
    ("Base Fare (BDT)",       "base_fare_bdt",         "DECIMAL(14,2)"),
    ("Tax & Surcharge (BDT)", "tax_surcharge_bdt",     "DECIMAL(14,2)"),
    ("Total Fare (BDT)",      "total_fare_bdt",        "DECIMAL(14,2)"),
    ("Seasonality",           "seasonality",           "VARCHAR(50)"),
    ("Days Before Departure", "days_before_departure", "INT"),
]

COLUMN_MAP = {csv: col for csv, col, _ in STAGING_COLUMNS}

DECIMAL_COLUMNS = [c for _, c, t in STAGING_COLUMNS if t.startswith("DECIMAL")]
INTEGER_COLUMNS = [c for _, c, t in STAGING_COLUMNS if t == "INT"]
DATETIME_COLUMNS = [c for _, c, t in STAGING_COLUMNS if t == "DATETIME"]

# Columns the lab explicitly requires (used again in Phase 2 validation)
REQUIRED_CSV_COLUMNS = [
    "Airline",
    "Source",
    "Destination",
    "Base Fare (BDT)",
    "Tax & Surcharge (BDT)",
    "Total Fare (BDT)",
]

INSERT_BATCH_SIZE = 5000

# ---------------- Phase 2: validation ----------------
CLEAN_TABLE = "stg_flight_prices_clean"
REJECTED_TABLE = "stg_flight_prices_rejected"
DQ_LOG_TABLE = "dq_validation_log"

# Pipeline fails if more than this share of rows is rejected
MAX_REJECT_RATIO = 0.05

# Tolerance (BDT) when checking Total Fare = Base Fare + Tax & Surcharge
FARE_TOLERANCE_BDT = 1.0

# Reference list of airports. Codes must be 3 uppercase letters (IATA format);
# well-formed codes missing from this list are FLAGGED (not rejected) so the
# list can be extended rather than silently losing real routes.
KNOWN_AIRPORTS = {
    # Bangladesh (domestic)
    "DAC": "Dhaka", "CGP": "Chittagong", "ZYL": "Sylhet", "CXB": "Cox's Bazar",
    "JSR": "Jessore", "RJH": "Rajshahi", "SPD": "Saidpur", "BZL": "Barisal",
    # International
    "CCU": "Kolkata", "DEL": "Delhi", "BOM": "Mumbai", "MAA": "Chennai",
    "KTM": "Kathmandu", "BKK": "Bangkok", "SIN": "Singapore", "KUL": "Kuala Lumpur",
    "DXB": "Dubai", "AUH": "Abu Dhabi", "DOH": "Doha", "JED": "Jeddah",
    "RUH": "Riyadh", "MCT": "Muscat", "KWI": "Kuwait", "BAH": "Bahrain",
    "IST": "Istanbul", "LHR": "London", "JFK": "New York", "YYZ": "Toronto",
    "CAN": "Guangzhou", "HKG": "Hong Kong", "NRT": "Tokyo", "MLE": "Male",
    "CMB": "Colombo",
}

# ---------------- Phase 3: transformation & KPIs ----------------
# Bangladeshi airports -> used to classify routes as Domestic / International
DOMESTIC_AIRPORTS = {"DAC", "CGP", "ZYL", "CXB", "JSR", "RJH", "SPD", "BZL"}

# Seasonality values treated as peak demand periods. Everything else
# (in this dataset: "Regular") is non-peak and serves as the baseline.
PEAK_SEASONS = {"Eid", "Hajj", "Winter Holidays"}

# ---------------- Phase 4: loading into PostgreSQL ----------------
PG_SCHEMA = "analytics"
PG_LOAD_LOG_TABLE = "pipeline_load_log"
