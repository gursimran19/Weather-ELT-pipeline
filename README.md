# Weather ELT Pipeline

A small, scheduled ELT (Extract–Load–Transform) pipeline that pulls weather
forecast data from [Open-Meteo](https://open-meteo.com/) (free, no API key),
lands it raw in Postgres, and transforms it into clean hourly and daily
tables — all orchestrated by Apache Airflow and run with Docker.

Built as a portfolio project to demonstrate pipeline design, orchestration,
and warehouse modeling — the core skills junior data engineering roles ask
for beyond what a typical data-analyst background covers.

## Why this project

Junior data engineer job postings consistently ask for experience with:
- **Orchestration** (Airflow, Prefect) — scheduling and monitoring recurring jobs
- **Scheduled, repeatable pipelines** — not one-off scripts
- **Warehouse modeling** — raw landing data → cleaned, typed tables → aggregates

This project demonstrates all three end to end, on top of a free public API.

## Architecture

Docker Desktop runs two containers on your machine: one for **Postgres**
(the database) and one for **Airflow** (the scheduler). They talk to each
other over Docker's internal network using plain SQL.

```
Open-Meteo API
      |
  extract.py        --> writes raw JSON to data/raw/
      |
   load.py           --> inserts raw JSON into raw_weather (Postgres staging table)
      |
 transform.py         --> unpacks JSON into weather_hourly (typed, deduped)
      |                   then rolls up into weather_daily_summary
      v
Postgres warehouse (queryable via psql, Power BI, Streamlit, etc.)
```

All three steps are wired into one Airflow DAG (`dags/weather_pipeline.py`)
that runs on an hourly schedule: `ensure_tables -> extract -> load -> transform`.

### How the pieces actually connect

- **`docker-compose.yml`** is the build recipe — it spins up a `postgres`
  container and an `airflow` container (webserver + scheduler) and gives
  Airflow a connection string to find Postgres:
  ```
  postgresql+psycopg2://airflow:airflow@postgres/airflow
  ```
  Inside Docker's internal network, each container is reachable by its
  *service name* (`postgres`) — no manual IP configuration needed.

- **`sql/create_tables.sql`** is plain SQL `CREATE TABLE` statements
  defining three tables: `raw_weather` (messy JSON dump), `weather_hourly`
  (cleaned, typed rows), `weather_daily_summary` (daily aggregates).

- **`scripts/extract.py`, `load.py`, `transform.py`** are ordinary Python —
  nothing Airflow-specific. `extract_all()` calls the weather API and writes
  JSON to disk. `load_all()` reads that JSON and inserts it into
  `raw_weather` via SQLAlchemy. `run_transform()` runs SQL that reshapes
  `raw_weather` into the clean tables. All three connect to Postgres using
  the same connection string, passed in as the `WAREHOUSE_DB_URI`
  environment variable.

- **`dags/weather_pipeline.py`** is where Airflow enters the picture. It
  imports the three plain functions above and declares the run order:
  ```python
  ensure_tables_task >> extract_task >> load_task >> transform_task
  ```
  The `>>` means "this task must finish before the next starts." That
  sequence of dependent tasks is the whole DAG (directed acyclic graph).
  Airflow's only job is scheduling and tracking runs — the real logic lives
  in the plain Python functions.

## Project structure

```
weather-elt-pipeline/
├── docker-compose.yml     # Postgres + Airflow (webserver, scheduler, init)
├── requirements.txt       # extra Python deps installed into the Airflow containers
├── dags/
│   └── weather_pipeline.py
├── scripts/
│   ├── __init__.py
│   ├── extract.py          # calls Open-Meteo, writes raw JSON
│   ├── load.py              # raw JSON -> raw_weather table
│   └── transform.py         # raw_weather -> weather_hourly -> weather_daily_summary
├── sql/
│   └── create_tables.sql    # DDL for raw_weather, weather_hourly, weather_daily_summary
└── data/raw/                # raw JSON landing zone (gitignored)
```

## Running it

Requires [Docker Desktop](https://www.docker.com/products/docker-desktop/).

```bash
cd weather-elt-pipeline
docker compose up airflow-init   # one-time: creates DB schema + admin user
docker compose up                # starts Postgres + Airflow webserver + scheduler
```

Open **http://localhost:8080** (login: `admin` / `admin`), find
**weather_elt_pipeline** in the DAG list, toggle it on, and trigger a run
with the ▶️ button. Four tasks should go green in order: `ensure_tables` →
`extract` → `load` → `transform`.

To inspect the data directly:

```bash
docker compose exec postgres psql -U airflow -d airflow
```
```sql
SELECT * FROM weather_daily_summary ORDER BY observed_date DESC LIMIT 5;
```

To stop everything (data persists in the Docker volume between runs):
```bash
# Ctrl+C in the terminal running `docker compose up`, then:
docker compose down
```

## Troubleshooting

**`AttributeError: module 'sqlalchemy.orm.attributes' has no attribute
'ScalarAttributeImpl'`** — this happens if `requirements.txt` or
`_PIP_ADDITIONAL_REQUIREMENTS` pulls in SQLAlchemy 2.x. Airflow 2.9.3
requires SQLAlchemy `<2.0` internally, so installing a newer version on top
breaks the webserver and scheduler. Fix: don't pin a SQLAlchemy version at
all — let Airflow's bundled version be used. If you hit this, run
`docker compose down -v` to clear the broken state, fix the version
pin, then `docker compose up airflow-init` followed by `docker compose up`.

**`failed to connect to the docker API`** — Docker Desktop isn't running.
Open the Docker Desktop app and wait for it to report "running" before
retrying any `docker compose` command.