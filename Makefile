# Flight Price Analysis Pipeline - shortcuts for common tasks
# Usage: make <target>        List all targets: make help

# Load credentials from .env (created by `make setup`)
-include .env

COMPOSE   = docker compose
DAG_ID    = flight_price_pipeline
CSV       = data/Flight_Price_Dataset_of_Bangladesh.csv
AIRFLOW   = $(COMPOSE) exec airflow-scheduler airflow
PSQL      = $(COMPOSE) exec postgres-analytics psql -U $(PG_USER) -d $(PG_DATABASE) -P pager=off
MYSQL     = $(COMPOSE) exec mysql-staging mysql -u$(MYSQL_USER) -p$(MYSQL_PASSWORD) $(MYSQL_DATABASE)

ifeq ($(OS),Windows_NT)
  COPY = copy
else
  COPY = cp
endif

.DEFAULT_GOAL := help
.PHONY: help setup check-data up down restart ps logs env-check run trigger kpis dq load-log psql mysql clean

help:
	@echo Flight Price Analysis Pipeline
	@echo   make setup       Create .env from .env.example if it does not exist
	@echo   make up          Start all containers: Airflow, MySQL, PostgreSQL
	@echo   make ps          Show container status
	@echo   make env-check   Run the connection test DAG
	@echo   make run         Run the full pipeline once with airflow dags test
	@echo   make trigger     Trigger a scheduled run visible in the Airflow UI
	@echo   make kpis        Show KPI results from PostgreSQL
	@echo   make dq          Show data quality results from MySQL
	@echo   make load-log    Show PostgreSQL load timings
	@echo   make psql        Open a PostgreSQL shell
	@echo   make mysql       Open a MySQL shell
	@echo   make logs        Follow container logs
	@echo   make down        Stop containers, data is kept
	@echo   make restart     Restart all containers
	@echo   make clean       Stop containers and DELETE all database data

# ---------- Setup ----------
setup:
	$(if $(wildcard .env),@echo .env already exists,$(COPY) .env.example .env)

check-data:
	$(if $(wildcard $(CSV)),@echo Dataset found: $(CSV),$(error Dataset missing: download it from Kaggle to $(CSV)))

# ---------- Containers ----------
up: setup check-data
	$(COMPOSE) up -d
	@echo Airflow UI: http://localhost:8080

down:
	$(COMPOSE) down

restart:
	$(COMPOSE) restart

ps:
	$(COMPOSE) ps -a

logs:
	$(COMPOSE) logs -f --tail=100

# ---------- Pipeline ----------
env-check:
	$(AIRFLOW) dags test 00_environment_check

run:
	$(AIRFLOW) dags test $(DAG_ID)

trigger:
	$(AIRFLOW) dags unpause $(DAG_ID)
	$(AIRFLOW) dags trigger $(DAG_ID)
	@echo Run triggered - follow it at http://localhost:8080

# ---------- Results ----------
kpis:
	$(PSQL) -c "SELECT * FROM analytics.vw_seasonal_fares;" -c "SELECT * FROM analytics.vw_top_10_routes;" -c "SELECT booking_rank, airline, booking_count, avg_total_fare_bdt, peak_premium_pct FROM analytics.vw_airline_scorecard LIMIT 10;"

dq:
	$(MYSQL) -e "SELECT rule_name, severity, affected_rows, action_taken FROM dq_validation_log WHERE affected_rows > 0 ORDER BY id DESC LIMIT 35; SELECT COUNT(*) AS clean_rows FROM stg_flight_prices_clean; SELECT COUNT(*) AS rejected_rows FROM stg_flight_prices_rejected;"

load-log:
	$(PSQL) -c "SELECT table_name, rows_loaded, load_seconds, loaded_at FROM analytics.pipeline_load_log ORDER BY id DESC LIMIT 6;"

psql:
	$(COMPOSE) exec postgres-analytics psql -U $(PG_USER) -d $(PG_DATABASE)

mysql:
	$(COMPOSE) exec mysql-staging mysql -u$(MYSQL_USER) -p$(MYSQL_PASSWORD) $(MYSQL_DATABASE)

# ---------- Cleanup ----------
clean:
	$(COMPOSE) down -v
	@echo All containers and database volumes removed.
