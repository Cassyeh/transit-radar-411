"""
airflow/dags/eod_rollup_dag.py

PURPOSE
-------
The End-of-Day rollup DAG. Runs every night at 23:59:00 UTC.

This DAG is the equivalent of the bank's nightly batch process —
the moment the teller closes the window, counts the day's
transactions, files them in the vault and prepares a clean
ledger for tomorrow.

1. validate_daily_counts
        ↓
2. rollup_flight_events ──┐
                         ├──→ 3. reconcile_counts
2. rollup_territory_crossings ─┘
                                ↓
4. update_inflight_crossings
                                ↓
5. truncate_daily_tables
                                ↓
6. send_completion_email

WHAT IT DOES
------------
Every night at 23:59:00 it runs six tasks in order:

    Task 1: validate_daily_counts
        Counts rows in daily_flight_events and
        daily_territory_crossings. Sends a warning email if
        counts are suspiciously low. Never blocks the rollup —
        validation is informational only.

    Task 2: rollup_flight_events
        Copies all rows from daily_flight_events into
        hist_flight_events. Uses INSERT ... SELECT with
        ON CONFLICT DO NOTHING so it is safe to re-run.
        If this task fails: sends error email, stops the DAG,
        does NOT truncate — daily data is preserved.

    Task 3: rollup_territory_crossings
        Copies all rows from daily_territory_crossings into
        hist_territory_crossings. Runs after task 2 completes.
        Same failure behaviour — stop and preserve.

    Task 4: update_inflight_crossings
        Some flights were still airborne at 23:59:00.
        Their territory crossings were copied with
        flight_status = IN_PROGRESS and exited_at = NULL.
        This task marks them in hist_territory_crossings
        so the ingestion service knows to update them
        when those flights land tomorrow.
        Runs after task 3.

    Task 5: truncate_daily_tables
        TRUNCATE daily_flight_events CASCADE
        TRUNCATE daily_territory_crossings CASCADE
        Only runs if tasks 2 AND 3 both succeeded.
        Clears both daily tables for tomorrow.

    Task 6: send_completion_email
        Sends a summary email with row counts, timing,
        and any warnings detected during the run.
        Always runs — even if earlier tasks failed — so
        you always get notified of what happened.

FAILURE BEHAVIOUR
-----------------
If tasks 2 or 3 fail:
    - Airflow marks those tasks as FAILED (red in UI)
    - Task 5 (truncate) is SKIPPED automatically because
      its upstream dependencies failed
    - Task 6 (email) still runs with failure details
    - Daily tables are fully preserved for investigation
    - Re-running the DAG is safe — no duplicates created

SCHEDULE
--------
Runs at 23:59:00 UTC every day.
Cron expression: 59 23 * * *

HOW TO VIEW IN AIRFLOW UI
-------------------------
Open http://localhost:8080 in your browser.
Default credentials: airflow / airflow
Find "eod_rollup_dag" in the DAGs list.

HOW TO TRIGGER MANUALLY
-----------------------
In the Airflow UI click the play button next to the DAG.
Or via CLI:
    docker exec -it airflow airflow dags trigger eod_rollup_dag
"""

import os
import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pendulum

import sys
sys.path.append('/opt/airflow')
# Add project root to path for utils imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.emails_utils import send_email

import psycopg2
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB"),
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

# Minimum expected rows per day — used in validation task
# Adjust these thresholds as your pipeline matures
MIN_EXPECTED_EVENTS    = 10    # Warn if fewer than 10 flight events today
MIN_EXPECTED_CROSSINGS = 5     # Warn if fewer than 5 territory crossings today


# ─────────────────────────────────────────────────────────────
# REUSABLE: DATABASE CONNECTION
# ─────────────────────────────────────────────────────────────
def get_connection():
    """
    Creates a PostgreSQL connection for use inside Airflow tasks.
    Note: inside Docker, the host is 'postgres' (the container name)
    not 'localhost'. The DB_CONFIG above defaults to 'postgres'.
    """
    return psycopg2.connect(**DB_CONFIG)


# ─────────────────────────────────────────────────────────────
# REUSABLE: EMAIL SENDER
# ─────────────────────────────────────────────────────────────
def send_dag_email(subject: str, body: str) -> None:
    """
    Sends an email notification from within an Airflow task.
    Imports send_email from utils — same function used everywhere.
    Never raises — email failure should not crash a DAG task.
    """
    try:
        import sys
        # Add project root so utils is importable inside Airflow
        airflow_home = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        project_root = os.path.dirname(airflow_home)
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
        from utils.emails_utils import send_email
        send_email(subject=subject, body=body)
    except Exception as e:
        log.warning(f"Email notification failed: {e}")


# ─────────────────────────────────────────────────────────────
# TASK 1: VALIDATE DAILY COUNTS
# ─────────────────────────────────────────────────────────────
def validate_daily_counts(**context) -> dict:
    """
    Counts rows in daily_flight_events and
    daily_territory_crossings for today.

    Sends a warning email if counts are below minimum thresholds.
    Never raises an exception — this task is informational only
    and should never block the rollup from proceeding.

    Pushes counts to XCom so downstream tasks can reference them
    in the completion email.
    """
    conn   = get_connection()
    cursor = conn.cursor()
    today  = datetime.now(timezone.utc).date()

    # Count today's flight events
    cursor.execute("""
        SELECT COUNT(*) FROM daily_flight_events
        WHERE event_date = %s
    """, (today,))
    event_count = cursor.fetchone()[0]

    # Count today's territory crossings
    cursor.execute("""
        SELECT COUNT(*) FROM daily_territory_crossings
        WHERE entered_date = %s
    """, (today,))
    crossing_count = cursor.fetchone()[0]

    # Count events by type for the summary
    cursor.execute("""
        SELECT event_type, COUNT(*)
        FROM daily_flight_events
        WHERE event_date = %s
        GROUP BY event_type
        ORDER BY event_type
    """, (today,))
    event_breakdown = dict(cursor.fetchall())

    cursor.close()
    conn.close()

    log.info(f"Daily counts for {today}:")
    log.info(f"  Flight events      : {event_count:,}")
    log.info(f"  Territory crossings: {crossing_count:,}")
    log.info(f"  Event breakdown    : {event_breakdown}")

    warnings = []

    if event_count < MIN_EXPECTED_EVENTS:
        warnings.append(
            f"Flight events ({event_count}) below minimum "
            f"threshold ({MIN_EXPECTED_EVENTS})."
        )
    if crossing_count < MIN_EXPECTED_CROSSINGS:
        warnings.append(
            f"Territory crossings ({crossing_count}) below minimum "
            f"threshold ({MIN_EXPECTED_CROSSINGS})."
        )

    if warnings:
        warning_text = "\n".join(f"  - {w}" for w in warnings)
        send_dag_email(
            subject=f"Transit Radar 411 — EOD WARNING for {today}",
            body=(
                f"The EOD validation detected low counts for {today}.\n\n"
                f"Warnings:\n{warning_text}\n\n"
                f"  Flight events      : {event_count:,}\n"
                f"  Territory crossings: {crossing_count:,}\n"
                f"  Event breakdown    : {event_breakdown}\n\n"
                f"The rollup will proceed despite these warnings.\n"
                f"Check opensky_ingestor.py logs if counts seem wrong."
            )
        )
    else:
        send_email(subject="Test_email_dag", body="Test Email Dag")
        send_dag_email(
            subject=f"Transit Radar 411 — EOD Rollup Starting for {today}",
            body=(
                f"The EOD rollup has begun for {today}.\n\n"
                f"Validation passed with the following counts:\n\n"
                f"  Flight events      : {event_count:,}\n"
                f"  Territory crossings: {crossing_count:,}\n"
                f"  Event breakdown    : {event_breakdown}\n\n"
                f"You will receive a completion email once the rollup finishes."
            )
        )

    # Push counts to XCom for the completion email task
    context["ti"].xcom_push(key="event_count",    value=event_count)
    context["ti"].xcom_push(key="crossing_count", value=crossing_count)
    context["ti"].xcom_push(key="event_breakdown", value=str(event_breakdown))
    context["ti"].xcom_push(key="warnings",       value=warnings)

    return {
        "event_count":    event_count,
        "crossing_count": crossing_count,
        "warnings":       warnings
    }


# ─────────────────────────────────────────────────────────────
# TASK 2: ROLLUP FLIGHT EVENTS
# ─────────────────────────────────────────────────────────────
def rollup_flight_events(**context) -> int:
    """
    Copies all rows from daily_flight_events into
    hist_flight_events.

    Uses INSERT ... SELECT so the entire operation happens
    inside PostgreSQL — no data leaves the database server.
    This is faster and safer than reading into Python and
    re-inserting.

    ON CONFLICT DO NOTHING means:
    If a row already exists in hist_flight_events (e.g. from a
    previous failed+retried run) it is skipped silently.
    This makes the operation fully idempotent — safe to re-run.

    Also sets batch_date and inserted_at on every copied row
    so you can always identify which EOD batch a row came from.

    If this task fails for any reason it raises the exception
    so Airflow marks it FAILED and stops the DAG. Task 5
    (truncate) will be skipped automatically. Daily data is
    fully preserved.
    """
    conn   = get_connection()
    cursor = conn.cursor()
    today  = datetime.now(timezone.utc).date()
    now    = datetime.now(timezone.utc)

    try:
        cursor.execute("""
            INSERT INTO hist_flight_events (
                icao24, callsign, event_type, event_timestamp,
                event_date, latitude, longitude, altitude_ft,
                velocity_kmh, heading_deg, vertical_rate_fpm,
                on_ground, origin_iata, destination_iata,
                source, inserted_at, batch_date, created_at
            )
            SELECT
                d.icao24, d.callsign, d.event_type, d.event_timestamp,
                d.event_date, d.latitude, d.longitude, d.altitude_ft,
                d.velocity_kmh, d.heading_deg, d.vertical_rate_fpm,
                d.on_ground, d.origin_iata, d.destination_iata,
                d.source, %s, %s, d.created_at
            FROM daily_flight_events d
            WHERE NOT EXISTS (
                SELECT 1 FROM hist_flight_events h
                WHERE h.icao24 = d.icao24
                AND h.event_timestamp = d.event_timestamp
                AND h.event_type = d.event_type
            )
        """, (now, today))

        rows_inserted = cursor.rowcount
        conn.commit()

        log.info(f"rollup_flight_events: {rows_inserted:,} rows copied to hist.")
        context["ti"].xcom_push(key="events_rolled_up", value=rows_inserted)
        return rows_inserted

    except Exception as e:
        conn.rollback()
        send_dag_email(
            subject=f"Transit Radar 411 — EOD FAILED: rollup_flight_events",
            body=(
                f"The EOD rollup failed at rollup_flight_events.\n\n"
                f"  Date  : {today}\n"
                f"  Error : {e}\n\n"
                f"Daily tables have NOT been truncated.\n"
                f"Data is safe. Investigate and re-trigger the DAG."
            )
        )
        raise   # Re-raise so Airflow marks this task FAILED

    finally:
        cursor.close()
        conn.close()


# ─────────────────────────────────────────────────────────────
# TASK 3: ROLLUP TERRITORY CROSSINGS
# ─────────────────────────────────────────────────────────────
def rollup_territory_crossings(**context) -> int:
    """
    Copies all rows from daily_territory_crossings into
    hist_territory_crossings.

    Same pattern as rollup_flight_events — INSERT ... SELECT,
    ON CONFLICT DO NOTHING, fully idempotent.

    Flights still airborne at 23:59:00 will have:
        flight_status = IN_PROGRESS
        exited_at     = NULL
    These are copied as-is. Task 4 handles them.

    Fails loudly and stops the DAG if anything goes wrong.
    Daily data is preserved.
    """
    conn   = get_connection()
    cursor = conn.cursor()
    today  = datetime.now(timezone.utc).date()
    now    = datetime.now(timezone.utc)

    try:
        cursor.execute("""
            INSERT INTO hist_territory_crossings (
                flight_event_id, icao24, callsign, territory_type,
                territory_id, territory_name, crossing_role, flight_status,
                entered_at, entered_date, exited_at, exited_date,
                duration_minutes, source, inserted_at, batch_date, updated_at
            )
            SELECT
                h.event_id, dtc.icao24, dtc.callsign, dtc.territory_type,
                dtc.territory_id, dtc.territory_name, dtc.crossing_role, dtc.flight_status,
                dtc.entered_at, dtc.entered_date, dtc.exited_at, dtc.exited_date,
                dtc.duration_minutes, 'opensky', %s, %s, %s
            FROM daily_territory_crossings dtc
            JOIN daily_flight_events dfe ON dfe.event_id = dtc.flight_event_id
            JOIN hist_flight_events h 
                ON h.icao24 = dfe.icao24
                AND h.event_timestamp = dfe.event_timestamp
                AND h.event_type = dfe.event_type
            WHERE NOT EXISTS (
                SELECT 1 FROM hist_territory_crossings htc
                WHERE htc.icao24 = dtc.icao24
                AND htc.entered_at = dtc.entered_at
                AND htc.territory_id = dtc.territory_id
            )
        """, (now, today, now))

        rows_inserted = cursor.rowcount

        cursor.execute("""
            SELECT COUNT(*) FROM daily_territory_crossings dtc
            WHERE NOT EXISTS (
                SELECT 1 FROM daily_flight_events dfe 
                WHERE dfe.event_id = dtc.flight_event_id
            )
        """)
        orphaned = cursor.fetchone()[0]
        if orphaned > 0:
            logging.warning(f"rollup_territory_crossings: {orphaned} crossings have no matching flight event in daily_flight_events table — they will be skipped")

        conn.commit()

        log.info(f"rollup_territory_crossings: {rows_inserted:,} rows copied.")
        context["ti"].xcom_push(key="crossings_rolled_up", value=rows_inserted)
        return rows_inserted

    except Exception as e:
        conn.rollback()
        send_dag_email(
            subject=f"Transit Radar 411 — EOD FAILED: rollup_territory_crossings",
            body=(
                f"The EOD rollup failed at rollup_territory_crossings.\n\n"
                f"  Date  : {today}\n"
                f"  Error : {e}\n\n"
                f"Note: flight events may have already been rolled up.\n"
                f"Daily tables have NOT been truncated.\n"
                f"Investigate and re-trigger the DAG."
            )
        )
        raise

    finally:
        cursor.close()
        conn.close()

def reconcile_counts(**context) -> None:
    """
    Verifies that the number of rows rolled into historical tables
    matches the number of rows in daily tables for this batch.

    This is a financial-grade integrity check.
    """
    conn = get_connection()
    cursor = conn.cursor()

    today = datetime.now(timezone.utc).date()
    ti = context["ti"]

    events_rolled = ti.xcom_pull(
        task_ids="rollup_flight_events",
        key="events_rolled_up"
    ) or 0

    crossings_rolled = ti.xcom_pull(
        task_ids="rollup_territory_crossings",
        key="crossings_rolled_up"
    ) or 0

    cursor.execute("SELECT COUNT(*) FROM daily_flight_events")
    daily_events = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM daily_territory_crossings")
    daily_crossings = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    if events_rolled < daily_events:
        raise AirflowFailException(
            f"Reconciliation failed: events mismatch "
            f"(rolled={events_rolled}, daily={daily_events})"
        )

    if crossings_rolled < daily_crossings:
        raise AirflowFailException(
            f"Reconciliation failed: crossings mismatch "
            f"(rolled={crossings_rolled}, daily={daily_crossings})"
        )

    log.info("Reconciliation successful — counts match.")

# ─────────────────────────────────────────────────────────────
# TASK 4: UPDATE IN-FLIGHT CROSSINGS
# ─────────────────────────────────────────────────────────────
def update_inflight_crossings(**context) -> int:
    """
    Identifies territory crossings that were still IN_PROGRESS
    at midnight — flights that had not yet landed when the DAG ran.

    These crossings were copied to hist_territory_crossings with:
        flight_status = IN_PROGRESS
        exited_at     = NULL

    This task adds a note in the logs so they are visible.
    The ingestion service will update these rows directly in
    hist_territory_crossings when those flights land tomorrow.

    We do not close them here because we do not know the exit
    time — the flight has not landed yet. We simply count them
    and report them in the completion email.
    """
    conn   = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT COUNT(*)
            FROM hist_territory_crossings
            WHERE flight_status = 'IN_PROGRESS'
            AND exited_at IS NULL
            AND batch_date = CURRENT_DATE
        """)
        inflight_count = cursor.fetchone()[0]

        log.info(
            f"update_inflight_crossings: "
            f"{inflight_count:,} crossings still IN_PROGRESS. "
            f"Will be updated by ingestor when flights land."
        )

        context["ti"].xcom_push(
            key="inflight_count", value=inflight_count
        )
        return inflight_count

    finally:
        cursor.close()
        conn.close()


# ─────────────────────────────────────────────────────────────
# TASK 5: TRUNCATE DAILY TABLES
# ─────────────────────────────────────────────────────────────
def truncate_daily_tables(**context) -> None:
    """
    Clears both daily tables so they are ready for tomorrow.

    ONLY runs if tasks 2 (rollup_flight_events) AND 3
    (rollup_territory_crossings) both completed successfully.
    Airflow enforces this automatically through task dependencies.

    Uses TRUNCATE ... CASCADE which also clears any dependent
    rows in related tables automatically.

    Why TRUNCATE and not DELETE?
    TRUNCATE is much faster than DELETE for large tables because
    it does not scan rows one by one — it drops the entire table
    storage and recreates it empty. For a daily table with tens
    of thousands of rows this is effectively instant.

    After truncating, resets the SERIAL sequences back to 1
    so event_id and crossing_id start fresh each day.
    """
    conn   = get_connection()
    cursor = conn.cursor()
    today  = datetime.now(timezone.utc).date()

    try:
        # Truncate both daily tables
        cursor.execute("TRUNCATE TABLE daily_flight_events CASCADE")
        cursor.execute("TRUNCATE TABLE daily_territory_crossings CASCADE")

        # Reset sequences so IDs start from 1 tomorrow
        cursor.execute(
            "ALTER SEQUENCE daily_flight_events_event_id_seq RESTART WITH 1"
        )
        cursor.execute(
            "ALTER SEQUENCE daily_territory_crossings_crossing_id_seq RESTART WITH 1"
        )

        conn.commit()
        log.info(
            f"truncate_daily_tables: both daily tables cleared for {today}."
        )

    except Exception as e:
        conn.rollback()
        send_dag_email(
            subject=f"Transit Radar 411 — EOD FAILED: truncate_daily_tables",
            body=(
                f"Truncation failed after successful rollup.\n\n"
                f"  Date  : {today}\n"
                f"  Error : {e}\n\n"
                f"Historical tables contain today's data.\n"
                f"Daily tables may still have data — manual cleanup needed."
            )
        )
        raise

    finally:
        cursor.close()
        conn.close()


# ─────────────────────────────────────────────────────────────
# TASK 6: SEND COMPLETION EMAIL
# ─────────────────────────────────────────────────────────────
def send_completion_email(**context) -> None:
    """
    Sends the final EOD summary email.

    Always runs regardless of whether earlier tasks succeeded
    or failed — configured via trigger_rule='all_done'.
    This ensures you always receive a notification about
    what happened during the EOD run.

    Pulls counts from XCom — the shared memory that Airflow
    tasks use to pass values to each other.
    """
    ti    = context["ti"]
    today = datetime.now(timezone.utc).date()

    # Pull values from XCom — default to 0 if task failed
    event_count      = ti.xcom_pull(task_ids="validate_daily_counts",
                                    key="event_count")          or 0
    crossing_count   = ti.xcom_pull(task_ids="validate_daily_counts",
                                    key="crossing_count")       or 0
    event_breakdown  = ti.xcom_pull(task_ids="validate_daily_counts",
                                    key="event_breakdown")      or "{}"
    warnings         = ti.xcom_pull(task_ids="validate_daily_counts",
                                    key="warnings")             or []
    events_rolled    = ti.xcom_pull(task_ids="rollup_flight_events",
                                    key="events_rolled_up")     or "FAILED"
    crossings_rolled = ti.xcom_pull(task_ids="rollup_territory_crossings",
                                    key="crossings_rolled_up")  or "FAILED"
    inflight_count   = ti.xcom_pull(task_ids="update_inflight_crossings",
                                    key="inflight_count")       or 0

    # Determine overall status
    if events_rolled == "FAILED" or crossings_rolled == "FAILED":
        status  = "FAILED"
        subject = f"Transit Radar 411 — EOD FAILED for {today}"
    elif warnings:
        status  = "COMPLETED WITH WARNINGS"
        subject = f"Transit Radar 411 — EOD WARNING for {today}"
    else:
        status  = "COMPLETED SUCCESSFULLY"
        subject = f"Transit Radar 411 — EOD complete for {today}"

    warning_text = (
        "\nWarnings:\n" + "\n".join(f"  - {w}" for w in warnings) + "\n"
        if warnings else ""
    )

    body = (
        f"End-of-Day rollup status: {status}\n\n"
        f"  Date                    : {today}\n"
        f"  Flight events today     : {event_count:,}\n"
        f"  Event breakdown         : {event_breakdown}\n"
        f"  Territory crossings     : {crossing_count:,}\n"
        f"  Events rolled to hist   : {events_rolled}\n"
        f"  Crossings rolled to hist: {crossings_rolled}\n"
        f"  In-flight at midnight   : {inflight_count:,}\n"
        f"{warning_text}"
        f"\nDaily tables have been cleared for tomorrow."
        if status == "COMPLETED SUCCESSFULLY"
        else f"\nDaily tables have NOT been cleared — investigate before re-running."
    )

    send_dag_email(subject=subject, body=body)
    log.info(f"Completion email sent: {subject}")


# ─────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────

# Default arguments applied to every task in the DAG
default_args = {
    "owner":            "transit_radar_411",
    "depends_on_past":  False,
    "email_on_failure": False,   # We send our own emails — no Airflow emails
    "email_on_retry":   False,
    "retries":          0,       # No automatic retries — fail fast and notify
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
}

with DAG(
    dag_id="eod_rollup_dag",
    default_args=default_args,
    description="End-of-day rollup: daily tables → historical tables",
    schedule_interval="59 23 * * *",     # Every night at 23:59:00 UTC
    # start_date=days_ago(1),
    catchup=False,                        # Do not backfill missed runs
    max_active_runs=1,                    # Only one run at a time
    tags=["eod", "rollup", "pipeline"],
    start_date=pendulum.datetime(2026, 4, 22, tz="Africa/Lagos"),   #timezone should be WAT not Airflows default UTC
) as dag:

    # ── Task 1: Validate ────────────────────────────────────
    t1_validate = PythonOperator(
        task_id="validate_daily_counts",
        python_callable=validate_daily_counts,
    )

    # ── Task 2: Rollup flight events ────────────────────────
    t2_rollup_events = PythonOperator(
        task_id="rollup_flight_events",
        python_callable=rollup_flight_events,
    )

    # ── Task 3: Rollup territory crossings ──────────────────
    t3_rollup_crossings = PythonOperator(
        task_id="rollup_territory_crossings",
        python_callable=rollup_territory_crossings,
    )

    t_reconcile = PythonOperator(
        task_id="reconcile_counts",
        python_callable=reconcile_counts,
    )

    # ── Task 4: Update in-flight crossings ──────────────────
    t4_update_inflight = PythonOperator(
        task_id="update_inflight_crossings",
        python_callable=update_inflight_crossings,
    )

    # ── Task 5: Truncate daily tables ───────────────────────
    t5_truncate = PythonOperator(
        task_id="truncate_daily_tables",
        python_callable=truncate_daily_tables,
    )

    # ── Task 6: Send completion email ───────────────────────
    # trigger_rule=all_done means this task runs regardless
    # of whether earlier tasks succeeded or failed.
    # You always get an email — success or failure.
    t6_email = PythonOperator(
        task_id="send_completion_email",
        python_callable=send_completion_email,
        trigger_rule="all_done",
    )

    # ── Task dependencies ────────────────────────────────────
    # This defines the order tasks run in:
    #
    #   t1_validate
    #       ↓
    #   t2_rollup_events   
    #       ↓
    #   t3_rollup_crossings
    #       ↓              
    #   t_reconcile
    #       ↓
    #   t4_update_inflight
    #       ↓
    #   t5_truncate
    #       ↓
    #   t6_email
    #
    # t2 and t3 run in sequence after t1 completes.
    # t4 waits for both t2 and t3.
    # t5 waits for t4.
    # t6 always runs last.

    t1_validate >> t2_rollup_events >> t3_rollup_crossings >> t_reconcile
    t_reconcile >> t4_update_inflight
    t4_update_inflight >> t5_truncate
    t5_truncate >> t6_email