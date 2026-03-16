# Transit Radar 411

A real-time data engineering pipeline that ingests live aircraft position data,
detects meaningful flight events.

## Status

🚧 Work in progress

## Stack

| Layer | Tool |
| --- | --- |
| Event streaming | Apache Kafka |
| Database | PostgreSQL 15 |
| Orchestration | Apache Airflow |
| Data lake | Delta Lake |
| Transformations | Apache Spark + dbt |
| Dashboard | Metabase |
| Containers | Docker + Docker Compose |

## Setup

Copy the environment variable template and fill in your own values:

```
copy .env.example .env
```

Then start all containers:

```
docker compose up -d
```

## Architecture

This pipeline mirrors the architecture of a banking transaction system.

| Banking concept | This project |
| --- | --- |
| Account number | `icao24` — permanent aircraft identifier |
| Customer table | `dim_aircraft` |
| Daily transactions | `daily_flight_events` |
| Historical transactions | `hist_flight_events` |
| End-of-day roll-up | Airflow DAG at 23:59:59 |