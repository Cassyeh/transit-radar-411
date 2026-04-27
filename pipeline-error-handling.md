# Transit Radar 411 — Pipeline Error Handling & Resilience

**Project:** Transit Radar 411  
**Author:** Cassandra Ijezie  
**Last updated:** April 2026

---

## Overview

This document describes how each component of the Transit Radar 411 pipeline handles failures, partial data, and unexpected shutdowns. It covers the live ingestion layer, the nightly Airflow rollup, the Silver enrichment layer, and the database itself.

---

## 1. Live Ingestion — `opensky_ingestor.py`

### 1.1 Wi-Fi / Internet goes down mid-cycle

**What happens:** The OpenSky API call throws a `requests.exceptions.ConnectionError` or a `requests.exceptions.Timeout`.

**How the pipeline handles it:** The ingestor wraps every API call in a try/except. On a connection failure it logs a WARNING and skips that polling cycle entirely. It does not crash. The next cycle begins after the configured sleep interval (60 seconds).

```
WARNING  OpenSky API error: Connection aborted. Skipping cycle.
INFO     Sleeping...
```

No data is lost from previous cycles. No partial writes occur because the failure happens before any database write.

**Recovery:** Automatic. When the internet comes back, the next 60-second cycle succeeds and ingestion resumes as normal. There is no backfill for the missed cycles — gaps in `daily_flight_events` during the outage period are expected and acceptable.

---

### 1.2 OpenSky returns a connection reset (Error 10054)

**What happens:** OpenSky forcibly closes the connection mid-response. This is common on the free API tier under load.

**How the pipeline handles it:** Caught by the same try/except as above. Logged as a WARNING and the cycle is skipped. The ingestor retries automatically on the next cycle.

**Recovery:** Automatic. No action required.

---

### 1.3 OpenSky returns HTTP 429 (rate limited)

**What happens:** Too many requests have been made. OpenSky returns a 429 response.

**How the pipeline handles it:** The ingestor catches 429 responses and skips the cycle. Credits reset at midnight UTC.

> ⚠️ **Implementation note:** Exponential backoff (60s → 120s → 180s) on 429 responses is a recommended improvement but should be verified in `opensky_ingestor.py`. Search for `429` or `backoff` in the file to confirm whether it is implemented. If not, add a retry loop with increasing sleep intervals before the cycle is skipped.

**Recovery:** Automatic. The cycle is skipped and normal polling resumes on the next cycle.

---

### 1.4 OpenSky returns no data (empty response)

**What happens:** The API returns a valid response but with no aircraft states — can happen when the bounding box is too narrow or the API is under maintenance.

**How the pipeline handles it:** The ingestor detects the empty response, logs `INFO: No data returned. Sleeping...` and skips the cycle. No database writes occur.

**Recovery:** Automatic.

---

### 1.5 Aircraft not in `dim_aircraft` (FK violation)

**What happens:** The ingestor tries to insert a flight event for an `icao24` that does not exist in `dim_aircraft`. Military, experimental, or unregistered aircraft are not in the 520k aircraft reference table.

**How the pipeline handles it:** The `write_flight_event()` function catches the FK violation (`psycopg2.errors.ForeignKeyViolation`) and silently skips the unknown aircraft. A DEBUG log entry is written but no exception is raised and the ingestor continues.

**Data impact:** Unknown aircraft are never stored in `daily_flight_events`. This is intentional — only trackable registered aircraft are processed.

---

### 1.6 Kafka broker is unavailable

**What happens:** The ingestor tries to dual-write to Kafka but the broker is down or unreachable.

**How the pipeline handles it:** Kafka is explicitly non-critical in this architecture. The ingestor catches the Kafka connection error, logs a WARNING, and continues writing to PostgreSQL only. The pipeline does not stop.

```
WARNING  Kafka unavailable — writing to PostgreSQL only.
```

**Recovery:** When Kafka comes back up, new events resume dual-writing. Events missed during the outage are not replayed to Kafka — they exist only in PostgreSQL.

---

### 1.7 PostgreSQL is unavailable during ingestion

**What happens:** The database container is down or the connection is refused.

**How the pipeline handles it:** The connection attempt fails with a `psycopg2.OperationalError`. The ingestor logs an ERROR and skips the cycle. It does not crash permanently — the next cycle will attempt a fresh connection.

**Recovery:** Automatic when PostgreSQL comes back up. No partial writes can occur because each write is wrapped in a transaction that either commits fully or rolls back.

A **transaction rollback** means: if the machine crashes or the connection drops after a write begins but before it completes, PostgreSQL undoes everything in that transaction as if it never happened. The table is left exactly as it was before the write started — no half-written rows, no corrupted data. PostgreSQL achieves this using its WAL (Write-Ahead Log), which records every intended change before applying it, allowing incomplete changes to be reversed on next startup.

---

### 1.8 Logs — terminal vs log files

By default, running `opensky_ingestor.py` directly from the terminal prints logs to the terminal only. When the terminal window is closed, those logs are gone permanently.

To persist logs to rotating files, add the following to the top of `opensky_ingestor.py` and any other long-running script:

```python
import logging
import os
from logging.handlers import RotatingFileHandler

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-10s %(message)s",
    handlers=[
        logging.StreamHandler(),                          # still prints to terminal
        RotatingFileHandler(
            "logs/ingestor.log",
            maxBytes=10 * 1024 * 1024,                   # 10MB per file
            backupCount=5                                 # keeps last 5 rotated files
        )
    ]
)

log = logging.getLogger(__name__)
```

Use different filenames per script:

| Script | Log file |
|---|---|
| `opensky_ingestor.py` | `logs/ingestor.log` |
| `enrich_flights.py` | `logs/enrichment.log` |
| `app.py` | `logs/api.log` |

`RotatingFileHandler` creates a new file when the log reaches 10MB and keeps the last 5 rotated files, preventing the disk from filling up.

---

### 1.8 System suddenly turns off mid-ingestion

**What happens:** The machine loses power or is force-shut down while the ingestor is writing a batch of flight events.

**How the pipeline handles it:** PostgreSQL uses WAL (Write-Ahead Logging) to guarantee durability. Any transaction that had not committed at the time of shutdown is automatically rolled back on next startup. No partial or corrupted rows are written.

`synchronous_commit=off` is set in this project for performance during bulk loads. This means a small number of the most recent committed transactions (within the last WAL buffer flush) could theoretically be lost on a hard crash — but in practice this affects at most the last 1-2 seconds of data, which is negligible for a flight tracking pipeline.

**Recovery:** Restart Docker and run `docker compose up -d`. The ingestor resumes from where it left off. A small gap in `daily_flight_events` around the crash time is expected.

---

## 2. Airflow EOD Rollup DAG — `eod_rollup_dag.py`

The DAG runs nightly at 23:59 WAT and moves data from daily tables into historical tables.

### DAG task order

```
validate_daily_counts
        ↓
rollup_flight_events
        ↓
rollup_territory_crossings
        ↓
reconcile_counts
        ↓
update_inflight_crossings
        ↓
truncate_daily_tables
        ↓
    dbt_run
        ↓
send_completion_email  (trigger_rule=all_done — always runs)
```

---

### 2.1 Task fails mid-DAG

**What happens:** Any task in the chain throws an exception.

**How the pipeline handles it:** Airflow marks that task as FAILED and all downstream tasks are marked UPSTREAM_FAILED and do not run. The `send_completion_email` task has `trigger_rule=all_done` so it always runs regardless of upstream failures — you always receive an email.

`retries=0` is set deliberately — the pipeline fails fast and notifies rather than silently retrying and potentially double-writing data.

`truncate_daily_tables` is intentionally the last substantive task in the DAG. If any rollup task fails before it, the daily tables are never cleared. This means the data is still intact and the DAG can be safely re-triggered once the issue is fixed. If truncation ran earlier and a downstream task then failed, the daily data would be permanently gone with no way to re-roll it into hist.

**Recovery:** Fix the root cause, delete the bad batch from hist tables if needed, then manually re-trigger the DAG. The `WHERE NOT EXISTS` duplicate protection on both INSERT statements means re-running is safe.

---

### 2.2 `validate_daily_counts` — counts below threshold

**What happens:** Fewer flight events or territory crossings than the configured minimum thresholds are found in the daily tables.

**How the pipeline handles it:** A warning email is sent to `cassandraijezie@gmail.com` detailing the low counts. The task does **not** fail — it is informational only. The rollup proceeds regardless.

**When this triggers:** If the ingestor was down for most of the day, or the bounding box returned very few flights.

---

### 2.3 `rollup_flight_events` — duplicate protection

**What happens:** The DAG is triggered multiple times for the same date (e.g. during testing or after a manual re-run).

**How the pipeline handles it:** The INSERT uses `WHERE NOT EXISTS` checking `(icao24, event_timestamp, event_type)`. Rows already in `hist_flight_events` from a previous run are skipped. Only genuinely new rows are inserted.

**Data guarantee:** Idempotent. Running the rollup 10 times produces the same result as running it once.

---

### 2.4 `rollup_territory_crossings` — FK dependency on flight events

**What happens:** Territory crossings reference `hist_flight_events.event_id` via a foreign key. If `rollup_flight_events` hasn't completed yet, there are no matching hist event IDs to join against.

**How the pipeline handles it:** The sequential task order guarantees `rollup_flight_events` always completes before `rollup_territory_crossings` starts. The JOIN through `daily_flight_events → hist_flight_events` on `(icao24, event_timestamp, event_type)` retrieves the correct hist `event_id` for each crossing.

**If the join finds no match:** The crossing is silently skipped and a WARNING is logged with the count of orphaned crossings. This can happen for aircraft that were already airborne when the ingestor started and have no DEPARTED event.

---

### 2.5 `truncate_daily_tables` — only truncates daily tables

**What happens:** At the end of the DAG, `daily_flight_events` and `daily_territory_crossings` are cleared for the next day.

**How the pipeline handles it:** Only the two daily tables are truncated — never the hist tables. `TRUNCATE ... CASCADE` is used which also removes dependent rows correctly.

**Risk:** If `truncate_daily_tables` runs before `rollup_territory_crossings` completes (which it cannot given the task order), territory crossings would be orphaned. The sequential task order prevents this entirely.

---

### 2.6 DAG does not fire at 23:59

**Possible causes:**

| Cause | How to detect | Fix |
|---|---|---|
| Docker Desktop not running | No containers running | Start Docker Desktop |
| `airflow-scheduler` container stopped | `docker ps` shows it missing | `docker compose up -d` |
| DAG is paused in UI | Toggle shows OFF | Toggle ON in Airflow UI |
| System was asleep at 23:59 | Check Next Run time in UI | Trigger manually |

`restart: always` on the scheduler container means it auto-restarts if it crashes — but only if Docker itself is running.

**Getting an email when the DAG doesn't fire:**

Airflow cannot send an email about a DAG that never ran because nothing is running to send it. The solution is a **watchdog DAG** — a second DAG that runs at 00:15 WAT every night and checks whether `eod_rollup_dag` completed successfully. If it did not, it sends an alert email.

Create `airflow/dags/watchdog_dag.py`:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timezone
import sys
sys.path.append('/opt/airflow')
from utils.email_utils import send_email

def check_eod_ran(**context):
    from airflow.models import DagRun
    from airflow.utils.state import State

    today = datetime.now(timezone.utc).date()

    runs = DagRun.find(
        dag_id="eod_rollup_dag",
        execution_start_date=datetime(today.year, today.month, today.day, 0, 0, 0, tzinfo=timezone.utc),
    )

    successful_runs = [r for r in runs if r.state == State.SUCCESS]

    if not successful_runs:
        send_email(
            subject=f"Transit Radar 411 — WARNING: EOD Rollup did not run on {today}",
            body=(
                f"The nightly EOD rollup DAG did not complete successfully on {today}.\n\n"
                f"Possible causes:\n"
                f"  - Docker was not running at 23:59\n"
                f"  - The DAG was paused in the Airflow UI\n"
                f"  - The machine was asleep or powered off\n\n"
                f"Action required: Log into Airflow at http://localhost:8080 "
                f"and manually trigger eod_rollup_dag."
            ),
            to="cassandraijezie@gmail.com"
        )

with DAG(
    dag_id="watchdog_dag",
    description="Checks that eod_rollup_dag ran successfully each night",
    schedule_interval="15 0 * * *",   # 00:15 WAT daily
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["watchdog", "monitoring"],
) as dag:

    t1 = PythonOperator(
        task_id="check_eod_ran",
        python_callable=check_eod_ran,
    )
```

This runs 16 minutes after the EOD DAG should have finished. If no successful run is found, you receive an alert. If everything ran fine, the watchdog completes silently.

---

### 2.7 System turns off before DAG completes

**What happens:** The machine shuts down while the DAG is mid-run — for example during `rollup_flight_events`.

**How the pipeline handles it:** PostgreSQL WAL guarantees any uncommitted transaction is rolled back. The hist tables are in a consistent state — either the full batch is there or none of it is. No partial rollup data is written.

**Recovery:** On next startup, check the Airflow UI for the interrupted run. It will show as FAILED or RUNNING (stuck). Manually mark it as failed, delete any partial hist data for that batch_date using pgAdmin, and re-trigger the DAG. The idempotent INSERT statements handle the re-run safely.

---

## 3. Silver Enrichment — `enrich_flights.py`

### 3.1 OpenSky 429 during enrichment

**What happens:** The enrichment script hits the 4,000 daily credit limit.

**How the pipeline handles it:** `API_SLEEP_SECONDS = 3` between calls. On a 429 response, exponential backoff is applied (60s → 120s → 180s). If all retries are exhausted for a particular aircraft, that flight's `origin_iata` and `destination_iata` remain NULL for that day.

**Data impact:** NULL origin/destination for some flights. These are enriched on subsequent runs if the daily data is still present.

---

### 3.2 Credits reset at midnight UTC

**Recommendation:** Run `enrich_flights.py` after the ingestor has been running for at least 1 hour, and before midnight UTC to maximise the credit window for that day.

---

## 4. Historical Data Loading — `load_historical_states.py`

### 4.1 Crash mid-load

**What happens:** The machine crashes or the script is killed while loading the 48 monthly Zenodo files.

**How the pipeline handles it:** A `.checkpoint` file at `seeds/historical/.checkpoint` tracks which files have been fully processed. On restart, the script reads the checkpoint and skips all completed files automatically.

**Recovery:** Simply re-run `python seeds\load_historical_states.py`. It picks up from the last incomplete file.

---

### 4.2 PostgreSQL WAL pressure crash during bulk load

**Symptom:** PostgreSQL crashes with "checkpoints occurring too frequently".

**How the pipeline handles it:** `synchronous_commit=off`, `max_wal_size=4GB`, and `checkpoint_completion_target=0.9` are configured in `docker-compose.yml`. `NUM_WORKERS` is reduced to 2 to limit concurrent write pressure.

**Recovery:** Restart Docker, re-run the loader. The checkpoint file ensures no file is processed twice.

---

## 5. Database Integrity

### 5.1 Is loading incremental?

Yes. Both the live ingestor and the historical loader are incremental:

- The ingestor writes individual events as they are detected — it does not reload past events.
- The historical loader processes one monthly file at a time, tracked by the checkpoint file.
- The EOD rollup moves only today's data — not all historical data.

### 5.2 Is loading idempotent?

Yes, for the rollup DAG. The `WHERE NOT EXISTS` pattern on both `hist_flight_events` and `hist_territory_crossings` INSERTs means re-running the DAG for the same date produces no duplicates.

The live ingestor is **not** idempotent by design — each 60-second poll captures new real-time state changes. Restarting the ingestor mid-day will produce a small gap in events around the restart time but will not create duplicates.

### 5.3 Foreign key violations

Two FK relationships require careful ordering:

| Child table | Parent table | Handling |
|---|---|---|
| `daily_flight_events` | `dim_aircraft` | Unknown `icao24` silently skipped in ingestor |
| `daily_territory_crossings` | `daily_flight_events` | Only detected aircraft with a known event_id get crossings |
| `hist_territory_crossings` | `hist_flight_events` | Rollup joins through daily tables to get correct hist event_id |

---

## 6. Email Notifications

The pipeline sends emails at the following points:

| Event | Email sent |
|---|---|
| DAG starts, counts look healthy | "EOD Rollup Starting" |
| DAG starts, counts below threshold | "EOD WARNING" with details |
| DAG completes successfully | "EOD Rollup Complete" with row counts |
| DAG completes with failures | "EOD Rollup Complete" with failure details |
| Historical file loads successfully | Per-file success notification |
| Historical file fails after max retries | Per-file error notification |
| Historical load fully complete | Summary with total/success/failed counts |

If `EMAIL_SENDER` or `EMAIL_APP_PASSWORD` are not set in the environment, email is skipped silently with a WARNING log — the pipeline never fails because of a missing email.

The email utility tries STARTTLS (port 587) first, then falls back to SSL (port 465) automatically if STARTTLS fails.

---

## 7. Summary Table

| Failure scenario | Pipeline response | Data lost? | Manual action needed? |
|---|---|---|---|
| Internet goes down during ingestion | Cycle skipped, resumes automatically | Gap in daily events | No |
| OpenSky 429 rate limit | Cycle skipped, verify backoff in code | Some enrichment NULLs | No |
| OpenSky connection reset | Cycle skipped | Gap in daily events | No |
| Unknown aircraft FK violation | Row silently skipped | That aircraft's events | No |
| Kafka broker down | Write to PostgreSQL only | Kafka events only | No |
| PostgreSQL down during ingestion | Cycle skipped, retries next cycle | Gap in daily events | No |
| Machine crashes mid-ingestion | WAL rollback, clean state | Last 1-2 seconds | No |
| DAG task fails mid-run | Downstream tasks cancelled, email sent, daily tables preserved | Depends on which task | Yes — re-trigger DAG |
| DAG runs twice same date | WHERE NOT EXISTS skips duplicates | None | No |
| Machine off at 23:59 | DAG does not fire, watchdog sends alert email | Daily rollup missed | Yes — manual trigger |
| Historical loader crashes | Checkpoint file, resumes from last file | None | No — just re-run |
| WAL pressure during bulk load | synchronous_commit=off, reduced workers | None | No — just re-run |
