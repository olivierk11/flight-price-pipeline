# Flight Price Analysis Pipeline (Bangladesh)

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10-017CEE?logo=apacheairflow&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-8.0-4479A1?logo=mysql&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![pandas](https://img.shields.io/badge/pandas-150458?logo=pandas&logoColor=white)

An end-to-end data pipeline built with **Apache Airflow** that ingests the *Flight Price Dataset of Bangladesh* (Kaggle), validates and transforms it in a **MySQL** staging database, computes KPIs, and loads the results into a **PostgreSQL** analytics database.

| Result | Value |
|---|---|
| Rows processed | 57,000 (0 lost, 0 rejected) |
| Validation rules | 35 |
| Fare totals corrected | 2,522 (Total = Base + Tax & Surcharge) |
| Peak-season fare premium | +20.43% |
| End-to-end runtime | ~49 s (`dags test`), ~80 s (scheduled run) |
| MySQL → PostgreSQL transfer | 5.1 s, verified by 33 column checksums |

## Architecture

```
            ┌──────────────────── Apache Airflow (12 tasks) ────────────────────┐
            │                                                                    │
  CSV ──► [MySQL staging] ──► [validated] ──► [enriched + KPIs] ──► [PostgreSQL analytics]
          stg_flight_prices   _clean/_rejected  trf_* + kpi_*        analytics schema + views
          verify: rows,       35 rules,         reconcile KPIs       COPY in 1 transaction,
          types, fare sum     5% quality gate                         checksums vs MySQL
```

Every phase ends with a verification task. Any mismatch fails the run, so unverified data never reaches PostgreSQL.

## Tech stack

- Apache Airflow 2.10 (LocalExecutor, TaskFlow API)
- MySQL 8.0: staging
- PostgreSQL 16: analytics
- Python 3.11, pandas
- Docker Compose

## Project structure

```
flight-price-pipeline/
├── docker-compose.yaml
|___ requirements.txt
├── .env.example                  copy to .env
├── data/                         put the CSV here (not committed)
├── docs/                         project report and screenshots
└── dags/
    ├── flight_price_pipeline.py  main DAG
    ├── 00_environment_check.py   connection smoke test
    ├── .airflowignore
    └── flight_pipeline/
        ├── config.py             connections, rules, peak seasons
        ├── schemas.py            shared table definitions (MySQL + PostgreSQL)
        ├── db_utils.py           transactional writes, type conversion
        ├── ingestion.py          Phase 1: CSV -> MySQL
        ├── validation.py         Phase 2: data quality rules
        ├── transformation.py     Phase 3: enrichment + KPIs
        └── loading.py            Phase 4: MySQL -> PostgreSQL
```

## How to run

**Requirements:** Docker Desktop with at least 4 GB of memory.

1. Clone the repository and create your environment file:
   ```bash
   git clone https://github.com/<your-username>/flight-price-pipeline.git
   cd flight-price-pipeline
   cp .env.example .env          # Windows PowerShell: copy .env.example .env
   ```
2. Download `Flight_Price_Dataset_of_Bangladesh.csv` from Kaggle into `data/`.
3. Start the stack:
   ```bash
   docker compose up -d
   docker compose ps -a          # wait until airflow-webserver is healthy
   ```
4. Run the pipeline. Either:
   - open http://localhost:8080 (admin / admin), unpause `flight_price_pipeline` and trigger it, or
   - run it from the terminal:
     ```bash
     docker compose exec airflow-scheduler airflow dags test flight_price_pipeline
     ```
5. Query the results:
   ```bash
   docker compose exec postgres-analytics psql -U analytics_user -d flight_analytics -P pager=off \
     -c "SELECT * FROM analytics.vw_top_10_routes;" \
     -c "SELECT * FROM analytics.vw_seasonal_fares;" \
     -c "SELECT * FROM analytics.vw_airline_scorecard;"
   ```

| Service | Host port |
|---|---|
| Airflow UI | 8080 |
| MySQL staging | 3307 |
| PostgreSQL analytics | 5433 |

### Makefile shortcuts

If `make` is installed, the steps above become one-word commands. Run `make help` for the full list.

| Command | What it does |
|---|---|
| `make up` | Creates `.env` if missing, checks the CSV is present, starts all containers |
| `make run` | Runs the full pipeline once |
| `make trigger` | Triggers a run that appears in the Airflow UI |
| `make kpis` | Prints the KPI views from PostgreSQL |
| `make dq` | Prints the data quality results from MySQL |
| `make load-log` | Prints PostgreSQL load timings |
| `make down` / `make clean` | Stops containers / stops them and deletes all data |

On Windows, install `make` with `winget install ezwinports.make`, then reopen the terminal.

## DAG tasks

| Phase | Tasks |
|---|---|
| Ingestion | `create_staging_table` → `load_csv_to_mysql` → `verify_staging_load` |
| Validation | `check_required_columns` → `validate_and_clean` → `data_quality_gate` |
| Transformation | `transform_and_enrich` → `compute_kpis` → `reconcile_kpis` |
| Loading | `create_analytics_schema` → `load_to_postgres` → `verify_postgres_load` |

## KPIs

| KPI | Table / view | Headline |
|---|---|---|
| Average fare by airline | `kpi_avg_fare_by_airline` | Turkish Airlines highest at 74,738.63 BDT |
| Seasonal fare variation | `kpi_seasonal_fare_variation`, `kpi_peak_vs_non_peak`, `vw_seasonal_fares` | Hajj +42.85%, Eid +34.83%, Winter +17.70% vs Regular |
| Booking count by airline | `kpi_booking_count_by_airline`, `vw_airline_scorecard` | US-Bangla Airlines 4,496 bookings |
| Most popular routes | `kpi_popular_routes`, `vw_top_10_routes` | Rajshahi → Singapore, 417 bookings |

Peak seasons are Eid, Hajj and Winter Holidays; Regular is the non-peak baseline. The setting is `PEAK_SEASONS` in `config.py`.

## Key design decisions

- **Idempotent loads.** Re-running replaces data instead of duplicating it. MySQL uses `DELETE` + `INSERT` in one transaction, because `TRUNCATE` auto-commits there.
- **Atomic PostgreSQL load.** `TRUNCATE` and `COPY` run in one transaction, so readers never see a partial load.
- **Consistent extract.** All source tables are read in one MySQL snapshot.
- **No silent data loss.** Original fare totals are kept in `reported_total_fare_bdt`. Rejected rows are kept with their reasons, and every rule's result is logged in `dq_validation_log`.
- **One schema definition.** Both databases' tables are generated from `schemas.py`.

## Troubleshooting

| Problem | Fix |
|---|---|
| `check_csv` / `load_csv_to_mysql` fails: CSV not found | Check the file is exactly `data/Flight_Price_Dataset_of_Bangladesh.csv` |
| `psql` output seems frozen | Press `q`; add `-P pager=off` |
| Webserver keeps restarting | `docker compose logs airflow-init`; give Docker at least 4 GB of memory |
| Start again from scratch | `docker compose down -v` (deletes all database data), then `docker compose up -d` |

## Documentation

The full project report is in [`docs/`](docs/). It covers the architecture, each task, KPI definitions, results, and the challenges encountered and how they were resolved.
