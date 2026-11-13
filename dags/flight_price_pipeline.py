"""
# Flight Price Analysis Pipeline (Bangladesh)

End-to-end pipeline: CSV -> MySQL staging -> validation -> transformation & KPIs -> PostgreSQL.

**Phase 1 - Ingestion**
- `create_staging_table`: creates `stg_flight_prices` in MySQL if missing
- `load_csv_to_mysql`: reads the CSV, casts types, replaces staging data in one transaction
- `verify_staging_load`: checks row count, column types and fare totals against the CSV

**Phase 2 - Validation**
- `check_required_columns`: required columns exist in the CSV and in staging
- `validate_and_clean`: applies data quality rules; writes clean rows, rejected rows
  (with reasons) and a per-rule audit log to MySQL in one transaction
- `data_quality_gate`: stops the pipeline if too many rows were rejected

**Phase 3 - Transformation & KPIs**
- `transform_and_enrich`: Total Fare = Base + Tax where missing/inconsistent, adds route,
  route type, season type (peak / non-peak) and date attributes
- `compute_kpis`: average fare by airline, booking count by airline, seasonal fare
  variation, peak vs non-peak comparison, most popular routes
- `reconcile_kpis`: proves KPI totals add up to the enriched data

**Phase 4 - Load into PostgreSQL**
- `create_analytics_schema`: `analytics` schema, tables, indexes, load log and ranked views
- `load_to_postgres`: consistent MySQL snapshot -> TRUNCATE + COPY of all tables in one
  PostgreSQL transaction
- `verify_postgres_load`: row counts and per-column checksums match MySQL exactly
"""
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from flight_pipeline import ingestion, loading, transformation, validation

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="flight_price_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["flight", "bangladesh"],
    doc_md=__doc__,
)
def flight_price_pipeline():

    # ---------------- Phase 1: Ingestion ----------------
    @task
    def create_staging_table():
        ingestion.create_staging_table()

    @task
    def load_csv_to_mysql(**context) -> dict:
        return ingestion.load_csv_to_mysql(batch_id=context["run_id"])

    @task
    def verify_staging_load(load_summary: dict) -> dict:
        return ingestion.verify_staging_load(load_summary)

    # ---------------- Phase 2: Validation ----------------
    @task
    def check_required_columns() -> dict:
        return validation.check_required_columns()

    @task
    def validate_and_clean(**context) -> dict:
        return validation.validate_and_clean(batch_id=context["run_id"])

    @task
    def data_quality_gate(summary: dict) -> dict:
        return validation.data_quality_gate(summary)

    # ---------------- Phase 3: Transformation & KPIs ----------------
    @task
    def transform_and_enrich(**context) -> dict:
        return transformation.transform_and_enrich(batch_id=context["run_id"])

    @task
    def compute_kpis(**context) -> dict:
        return transformation.compute_and_store_kpis(batch_id=context["run_id"])

    @task
    def reconcile_kpis(transform_summary: dict) -> dict:
        return transformation.reconcile_kpis(transform_summary)

    # ---------------- Phase 4: Load into PostgreSQL ----------------
    @task
    def create_analytics_schema():
        loading.create_analytics_schema()

    @task
    def load_to_postgres(**context) -> dict:
        return loading.load_to_postgres(batch_id=context["run_id"])

    @task
    def verify_postgres_load(**context) -> dict:
        return loading.verify_postgres_load(batch_id=context["run_id"])

    created = create_staging_table()
    load_summary = load_csv_to_mysql()
    verified = verify_staging_load(load_summary)
    columns_ok = check_required_columns()
    validation_summary = validate_and_clean()

    created >> load_summary
    verified >> columns_ok >> validation_summary
    gate = data_quality_gate(validation_summary)

    transform_summary = transform_and_enrich()
    kpis = compute_kpis()
    gate >> transform_summary >> kpis
    reconciled = reconcile_kpis(transform_summary)
    kpis >> reconciled

    schema_ready = create_analytics_schema()
    pg_load = load_to_postgres()
    reconciled >> schema_ready >> pg_load >> verify_postgres_load()


flight_price_pipeline()
