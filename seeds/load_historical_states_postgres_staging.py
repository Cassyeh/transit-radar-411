"""
seeds/load_historical_states_postgres_staging.py

WHAT THIS SCRIPT DOES
---------------------
Loads historical flight data from the OpenSky COVID-19 flight
dataset into hist_flight_events using two major optimisations:

1. Decompress the .gz file into a temp CSV.
   Use COPY to load raw CSV into a PostgreSQL staging table

2. MPP (Massively Parallel Processing) — processes multiple
   files simultaneously using Python multiprocessing.
   Each worker gets its own database connection.
   Number of workers = CPU cores - 1.

3. CHECKPOINT SYSTEM — after each file is fully inserted,
   its filename is written to seeds/historical/.checkpoint.
   On restart the script reads the checkpoint and skips
   already-loaded files entirely — no re-reading, no re-inserting.
   This makes crash recovery instant.

SOURCE DATA
-----------
Monthly .csv.gz files downloaded by seeds/download_data.py
into seeds/historical/. Each file covers one calendar month.
Each row in the file = one complete flight.

WHAT EACH FLIGHT ROW PRODUCES
------------------------------
Two rows in hist_flight_events:
    DEPARTED event — from firstseen, latitude_1, longitude_1
    LANDED event   — from lastseen,  latitude_2, longitude_2

AFTER THIS SCRIPT
-----------------
Run: python seeds/load_historical_territories.py

HOW TO RUN
----------
    docker compose up -d
    python seeds/load_historical_states_postgres_staging.py

CRASH RECOVERY
--------------
If the script crashes mid-run, simply re-run it.
Already completed files are listed in:
    seeds/historical/.checkpoint
and will be skipped automatically.
"""

import os
import sys
import logging
import csv
import tempfile
import multiprocessing as mp
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from dotenv import load_dotenv
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.emails_utils import send_email

load_dotenv()

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB"),
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

SEEDS_DIR        = os.path.dirname(os.path.abspath(__file__))
HISTORICAL_DIR   = os.path.join(SEEDS_DIR, "historical")
CHECKPOINT_FILE  = os.path.join(HISTORICAL_DIR, ".checkpoint")

SOURCE           = "opensky_historical"

# Bulk insert page size — number of rows per SQL statement
# 5000 is a good balance between speed and memory
PAGE_SIZE        = 5_000

# Number of parallel workers
# cpu_count() - 1 leaves one core free for PostgreSQL and OS
#NUM_WORKERS      = min(6, mp.cpu_count() - 1)
#
NUM_WORKERS = 1


# ─────────────────────────────────────────────────────────────
# CHECKPOINT SYSTEM
# ─────────────────────────────────────────────────────────────
def load_checkpoint() -> set:
    """
    Reads the checkpoint file and returns a set of filenames
    that have already been fully loaded.

    The checkpoint file is a plain text file with one filename
    per line. It lives in seeds/historical/.checkpoint

    Returns an empty set if no checkpoint file exists yet
    — meaning no files have been loaded before.
    """
    if not os.path.exists(CHECKPOINT_FILE):
        return set()

    with open(CHECKPOINT_FILE, "r") as f:
        completed = {line.strip() for line in f if line.strip()}

    log.info(f"  Checkpoint: {len(completed)} files already loaded.")
    return completed


def mark_checkpoint(filename: str) -> None:
    """
    Appends a filename to the checkpoint file after it has been
    fully and successfully loaded into hist_flight_events.

    Uses append mode ('a') so existing entries are never
    overwritten — only new entries are added.

    Called by each worker after a successful file load.
    Thread-safe for multiprocessing because each worker
    writes a complete line atomically.
    """
    with open(CHECKPOINT_FILE, "a") as f:
        f.write(filename + "\n")


# ─────────────────────────────────────────────────────────────
# DATABASE CONNECTION
# ─────────────────────────────────────────────────────────────
def get_connection_2():
    """
    Creates a PostgreSQL connection.
    Each multiprocessing worker calls this independently —
    connections cannot be shared between processes.
    """
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        raise RuntimeError(f"Cannot connect to PostgreSQL: {e}")

def get_connection(retries: int = 5, delay: int = 10):
    """
    Creates a PostgreSQL connection with retry logic.
    Retries up to `retries` times with `delay` seconds between
    attempts. This handles temporary connection failures when
    PostgreSQL is under heavy bulk load.
    """
    import time
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return psycopg2.connect(**DB_CONFIG)
        except Exception as e:
            last_error = e
            if attempt < retries:
                log.warning(
                    f"  Connection attempt {attempt}/{retries} failed. "
                    f"Retrying in {delay}s..."
                )
                time.sleep(delay)
    raise RuntimeError(f"Cannot connect to PostgreSQL after {retries} attempts: {last_error}")


# ─────────────────────────────────────────────────────────────
# AIRPORT ICAO → IATA LOOKUP
# ─────────────────────────────────────────────────────────────
def build_airport_lookup(conn) -> dict:
    """
    Builds ICAO -> IATA airport code mapping from dim_airport.
    Each worker builds its own copy — cannot share between processes.
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT airport_icao_code, airport_iata_code
        FROM dim_airport
        WHERE airport_icao_code IS NOT NULL
        AND   airport_iata_code IS NOT NULL
    """)
    rows   = cursor.fetchall()
    cursor.close()
    return {row[0]: row[1] for row in rows}


# ─────────────────────────────────────────────────────────────
# VALUE CLEANERS
# ─────────────────────────────────────────────────────────────
def clean_str(val) -> str | None:
    s = str(val).strip() if val is not None else ""
    return s if s and s.lower() not in ("nan", "none", "") else None


def to_float(val) -> float | None:
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        return None


def to_timestamp(val) -> datetime | None:
    """
    Converts a value to a UTC datetime.
    Handles both Unix timestamps (1577836800) and
    datetime strings (2020-01-01 14:23:00).
    """
    if val is None or str(val).strip().lower() in ("nan", "none", ""):
        return None
    raw = str(val).strip()
    # Try Unix timestamp first
    try:
        return datetime.fromtimestamp(float(raw), tz=timezone.utc)
    except (ValueError, OverflowError):
        pass
    # Try datetime string formats
    for fmt in [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S%z",
    ]:
        try:
            dt = datetime.strptime(raw, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


# ─────────────────────────────────────────────────────────────
# MAP ONE FLIGHT ROW TO EVENT TUPLES
# ─────────────────────────────────────────────────────────────
def map_flight_to_events(
    row: dict,
    airport_lookup: dict,
    now: datetime,
    batch_date
) -> list[tuple] | None:
    """
    Maps one COVID dataset row to two event tuples —
    DEPARTED and LANDED — ready for bulk insert.

    Returns None if required fields are missing.
    """
    icao24   = clean_str(row.get("icao24"))
    callsign = clean_str(row.get("callsign"))

    if not icao24:
        return None

    firstseen = to_timestamp(row.get("firstseen"))
    lastseen  = to_timestamp(row.get("lastseen"))

    if not firstseen or not lastseen:
        return None

    # Convert ICAO airport codes to IATA
    origin_icao      = clean_str(row.get("origin"))
    destination_icao = clean_str(row.get("destination"))
    origin_iata      = airport_lookup.get(origin_icao)      if origin_icao      else None
    destination_iata = airport_lookup.get(destination_icao) if destination_icao else None

    # Parse coordinates and convert altitude metres → feet
    lat1    = to_float(row.get("latitude_1"))
    lon1    = to_float(row.get("longitude_1"))
    alt1    = to_float(row.get("altitude_1"))
    lat2    = to_float(row.get("latitude_2"))
    lon2    = to_float(row.get("longitude_2"))
    alt2    = to_float(row.get("altitude_2"))
    alt1_ft = round(alt1 * 3.28084, 1) if alt1 else None
    alt2_ft = round(alt2 * 3.28084, 1) if alt2 else None

    departed = (
        icao24, callsign, "DEPARTED",
        firstseen, firstseen.date(),
        lat1, lon1, alt1_ft,
        origin_iata, destination_iata,
        SOURCE, now, batch_date, now
    )

    landed = (
        icao24, callsign, "LANDED",
        lastseen, lastseen.date(),
        lat2, lon2, alt2_ft,
        origin_iata, destination_iata,
        SOURCE, now, batch_date, now
    )

    return [departed, landed]


# ─────────────────────────────────────────────────────────────
# STEP 1 OF 3: DECOMPRESS .GZ TO TEMP CSV
# ─────────────────────────────────────────────────────────────
def decompress_to_temp_csv(filepath: str) -> str:
    """
    Decompresses a .csv.gz file to a temporary CSV file on disk.

    Returns the path to the temp CSV file.
    The caller is responsible for deleting it after use.

    Why decompress to disk instead of reading into memory?
    PostgreSQL COPY reads from a file path or STDIN — it cannot
    read from a gzip stream directly. We write to disk first
    so COPY can read it at full speed via STDIN.
    """
    import gzip
    import shutil
    import tempfile

    tmp = tempfile.NamedTemporaryFile(
        suffix="_raw.csv", delete=False, mode="wb"
    )
    tmp_path = tmp.name

    with gzip.open(filepath, "rb") as gz:
        shutil.copyfileobj(gz, tmp)
    tmp.close()

    return tmp_path


# ─────────────────────────────────────────────────────────────
# STEP 2 OF 3: COPY RAW CSV INTO STAGING TABLE
# ─────────────────────────────────────────────────────────────
def copy_raw_to_staging(tmp_csv_path: str, conn) -> None:
    """
    Loads the raw decompressed CSV into a temporary PostgreSQL
    staging table using COPY — the fastest possible ingestion method.

    The staging table accepts all columns as TEXT so COPY runs
    at full speed with zero type validation overhead. Type
    conversion and transformation happen in the next step.

    TEMPORARY means the staging table auto-drops when the
    database connection closes — no manual cleanup needed.

    ON COMMIT DROP means the table is also dropped if the
    transaction commits or rolls back — belt and braces.
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TEMP TABLE IF NOT EXISTS staging_flights (
                callsign     TEXT,
                number       TEXT,
                icao24       TEXT,
                registration TEXT,
                typecode     TEXT,
                origin       TEXT,
                destination  TEXT,
                firstseen    TEXT,
                lastseen     TEXT,
                day          TEXT,
                latitude_1   TEXT,
                longitude_1  TEXT,
                altitude_1   TEXT,
                latitude_2   TEXT,
                longitude_2  TEXT,
                altitude_2   TEXT
            ) ON COMMIT DROP
        """)

        with open(tmp_csv_path, "r", encoding="utf-8", errors="replace") as f:
            #print(tmp_csv_path)
            cursor.copy_expert("""
                COPY staging_flights FROM STDIN
                WITH (FORMAT CSV, HEADER TRUE, NULL '')
            """, f)
        
        cursor.execute("SELECT COUNT(*) FROM staging_flights")
        file_rows_count = cursor.fetchone()[0]
        print(f"{tmp_csv_path} has {file_rows_count} amount of rows")
        #conn.commit()
        conn_check = get_connection()
        cursor     = conn_check.cursor()
    finally:
        cursor.close()


# ─────────────────────────────────────────────────────────────
# STEP 3 OF 3: TRANSFORM IN PYTHON AND COPY OUT TO HIST TABLE
# ─────────────────────────────────────────────────────────────
def transform_and_copy_out(
    conn,
    airport_lookup: dict,
    now: datetime,
    batch_date
) -> tuple[int, int]:
    """
    Reads from the staging table into Python, applies the
    existing map_flight_to_events() transformation, writes
    the clean result to a second temp CSV, then COPYs that
    clean CSV directly into hist_flight_events.

    Why COPY out instead of execute_values?
    ----------------------------------------
    execute_values still uses SQL parameter binding which
    has overhead per row — quoting, escaping, parsing.
    COPY FROM STDIN bypasses all of that and writes directly
    to PostgreSQL storage. For 100,000+ rows this is
    3-5x faster than execute_values.

    Why write a clean CSV instead of streaming directly?
    -----------------------------------------------------
    COPY expects to read a complete file. Writing to a temp
    CSV first lets us build all rows in Python memory before
    sending — simpler and more reliable than streaming.

    Returns (events_inserted, rows_skipped) as a tuple.
    """
    cursor   = conn.cursor()
    tmp_clean_path = None

    try:
        # Read all rows from staging into Python
        cursor.execute("""
            SELECT
                callsign, number, icao24, registration,
                typecode, origin, destination,
                firstseen, lastseen, day,
                latitude_1, longitude_1, altitude_1,
                latitude_2, longitude_2, altitude_2
            FROM staging_flights
            WHERE icao24 IS NOT NULL
            AND  TRIM(icao24) != ''
        """)

        columns      = [desc[0] for desc in cursor.description]
        staging_rows = cursor.fetchall()

        # Apply Python transformation and write to clean temp CSV
        tmp_clean = tempfile.NamedTemporaryFile(
            suffix="_clean.csv", delete=False,
            mode="w", encoding="utf-8", newline=""
        )
        tmp_clean_path = tmp_clean.name
        writer         = csv.writer(tmp_clean, quoting=csv.QUOTE_MINIMAL)

        total_events = 0
        rows_skipped = 0

        for staging_row in staging_rows:
            row_dict = dict(zip(columns, staging_row))

            events = map_flight_to_events(
                row=row_dict,
                airport_lookup=airport_lookup,
                now=now,
                batch_date=batch_date
            )

            if events:
                for event in events:
                    # Convert Python objects to strings for COPY
                    # datetime → ISO string, None → empty string
                    clean_row = [
                        v.isoformat()
                        if isinstance(v, (datetime, type(now.date())))
                        else ('' if v is None else str(v))
                        for v in event
                    ]
                    writer.writerow(clean_row)
                    total_events += 1
            else:
                rows_skipped += 1

        tmp_clean.close()

        # COPY clean CSV directly into hist_flight_events
        # No execute_values, no parameter binding — pure COPY speed
        with open(tmp_clean_path, "r", encoding="utf-8") as f:
            cursor.copy_expert("""
                COPY hist_flight_events (
                    icao24, callsign, event_type, event_timestamp,
                    event_date, latitude, longitude, altitude_ft,
                    origin_iata, destination_iata, source,
                    inserted_at, batch_date, created_at
                )
                FROM STDIN
                WITH (FORMAT CSV, NULL '')
            """, f)

        conn.commit()
        return total_events, rows_skipped

    finally:
        cursor.close()
        if tmp_clean_path and os.path.exists(tmp_clean_path):
            os.remove(tmp_clean_path)


# ─────────────────────────────────────────────────────────────
# ORCHESTRATOR: COPY AND TRANSFORM
# ─────────────────────────────────────────────────────────────
def copy_and_transform(
    filepath: str,
    conn,
    airport_lookup: dict,
    now: datetime,
    batch_date
) -> tuple[int, int]:
    """
    Orchestrates the three-step COPY pipeline for one file:

        Step 1: decompress_to_temp_csv()
                .gz → temp CSV on disk

        Step 2: copy_raw_to_staging()
                COPY temp CSV → PostgreSQL staging table
                Fastest possible ingestion — zero type overhead

        Step 3: transform_and_copy_out()
                SELECT staging → Python transform → clean CSV
                COPY clean CSV → hist_flight_events
                Fastest possible output — no parameter binding

    Each step is a separate function with one responsibility.
    If any step fails the exception propagates up to
    process_file() which handles logging and error reporting.

    Returns (events_inserted, rows_skipped) as a tuple.
    """
    tmp_raw_path = None
    try:
        tmp_raw_path = decompress_to_temp_csv(filepath)
        copy_raw_to_staging(tmp_raw_path, conn)
        events_inserted, skipped = transform_and_copy_out(
            conn, airport_lookup, now, batch_date
        )
        return events_inserted, skipped

    finally:
        # Always clean up raw temp CSV even if an error occurred
        if tmp_raw_path and os.path.exists(tmp_raw_path):
            os.remove(tmp_raw_path)


# ─────────────────────────────────────────────────────────────
# PROCESS ONE FILE — runs inside each worker process
# ─────────────────────────────────────────────────────────────
def process_file(args: tuple) -> dict:
    """
    Entry point for each multiprocessing worker.
    Each worker calls this function independently with its
    own database connection — connections cannot be shared
    between processes.

    Orchestrates the full pipeline for one monthly file:
        1. Get database connection
        2. Build airport lookup
        3. Run copy_and_transform() — the three-step COPY pipeline
        4. Mark file complete in checkpoint

    Parameters passed as a single tuple (multiprocessing requirement):
        filepath     — full path to the .csv.gz file
        file_number  — position in the overall sequence
        total_files  — total number of files being processed

    Returns a summary dict aggregated by main().
    """
    filepath, file_number, total_files = args
    filename = os.path.basename(filepath)

    # Each worker creates its own connection and airport lookup
    try:
        conn           = get_connection()
        airport_lookup = build_airport_lookup(conn)
    except Exception as e:
        return {
            "filename":        filename,
            "error":           str(e),
            "events_inserted": 0,
            "rows_skipped":    0
        }

    log.info(f"  Worker started: [{file_number}/{total_files}] {filename}")

    now        = datetime.now(timezone.utc)
    batch_date = now.date()

    try:
        events_inserted, rows_skipped = copy_and_transform(
            filepath=filepath,
            conn=conn,
            airport_lookup=airport_lookup,
            now=now,
            batch_date=batch_date
        )
    except Exception as e:
        conn.close()
        return {
            "filename":        filename,
            "error":           f"Pipeline failed: {e}",
            "events_inserted": 0,
            "rows_skipped":    0
        }

    conn.close()

    # Mark this file complete in the checkpoint so it is
    # skipped on any future re-run of the script
    mark_checkpoint(filename)

    log.info(
        f"  Worker done: [{file_number}/{total_files}] {filename} | "
        f"inserted: {events_inserted:,} | skipped: {rows_skipped:,}"
    )

    return {
        "filename":        filename,
        "events_inserted": events_inserted,
        "rows_skipped":    rows_skipped
    }


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("  Transit Radar 411 — Historical States Loader (MPP)")
    log.info("=" * 60)
    log.info(f"  Parallel workers : {NUM_WORKERS}")
    log.info(f"  Bulk insert size : {PAGE_SIZE:,} rows per statement")

    # Find all downloaded files
    if not os.path.exists(HISTORICAL_DIR):
        log.error(f"  Directory not found: {HISTORICAL_DIR}")
        log.error(f"  Run seeds/download_data.py first.")
        sys.exit(1)

    all_files = sorted([
        os.path.join(HISTORICAL_DIR, f)
        for f in os.listdir(HISTORICAL_DIR)
        if f.endswith(".csv.gz")
    ])

    if not all_files:
        log.error(f"  No .csv.gz files found in {HISTORICAL_DIR}")
        sys.exit(1)

    # Load checkpoint — skip already completed files
    completed = load_checkpoint()
    files_to_process = [
        f for f in all_files
        if os.path.basename(f) not in completed
    ]

    total_all    = len(all_files)
    total_skip   = len(completed)
    total_todo   = len(files_to_process)

    log.info(f"  Total files found    : {total_all}")
    log.info(f"  Already completed    : {total_skip} (from checkpoint)")
    log.info(f"  Files to process now : {total_todo}")

    if total_todo == 0:
        log.info("  All files already loaded. Nothing to do.")
        log.info("  Delete .checkpoint to reload from scratch.")
        sys.exit(0)

    # Build args list for multiprocessing
    # file_number continues from where checkpoint left off
    args = [
        (filepath, total_skip + i + 1, total_all)
        for i, filepath in enumerate(files_to_process)
    ]

    send_email(
        subject="Transit Radar 411 — Historical states loading started (MPP)",
        body=(
            f"Starting parallel load of {total_todo} monthly files.\n\n"
            f"  Total files          : {total_all}\n"
            f"  Already loaded       : {total_skip}\n"
            f"  Processing now       : {total_todo}\n"
            f"  Parallel workers     : {NUM_WORKERS}\n"
            f"  Bulk insert page size: {PAGE_SIZE:,}\n\n"
            f"Checkpoint file: seeds/historical/.checkpoint\n"
            f"Re-run safely after crash — completed files are skipped."
        )
    )

    start_time = time.time()

    # Run all files in parallel using a worker pool
    # pool.map distributes files across workers automatically
    # Each worker processes one file at a time
    # When a worker finishes it picks up the next file
    with mp.Pool(processes=NUM_WORKERS) as pool:
        for results in pool.imap_unordered(process_file, args):
            print(results)

    duration_minutes = (time.time() - start_time) / 60

    # Safe aggregation — handles both dict results and unexpected strings
    valid_results         = [r for r in results if isinstance(r, dict)]
    bad_results           = [r for r in results if not isinstance(r, dict)]

    total_events_inserted = sum(r.get("events_inserted", 0) for r in valid_results)
    total_rows_skipped    = sum(r.get("rows_skipped",    0) for r in valid_results)
    failed_files          = [r for r in valid_results if "error" in r]

    # Log any workers that returned unexpected output
    for bad in bad_results:
        log.error(f"  Worker returned unexpected result: {bad}")
        failed_files.append({"filename": "unknown", "error": str(bad)})

    log.info("\n" + "=" * 60)
    log.info("  Summary")
    log.info("=" * 60)
    log.info(f"  Files processed  : {total_todo - len(failed_files)}")
    log.info(f"  Files failed     : {len(failed_files)}")
    log.info(f"  Events inserted  : {total_events_inserted:,}")
    log.info(f"  Rows skipped     : {total_rows_skipped:,}")
    log.info(f"  Duration         : {duration_minutes:.1f} minutes")

    if failed_files:
        for f in failed_files:
            log.error(f"  FAILED: {f['filename']} — {f.get('error')}")

    status = "All files loaded successfully." if not failed_files else \
             f"WARNING: {len(failed_files)} file(s) failed."

    send_email(
        subject="Transit Radar 411 — Historical states loading complete",
        body=(
            f"{status}\n\n"
            f"  Files processed  : {total_todo - len(failed_files)}\n"
            f"  Files failed     : {len(failed_files)}\n"
            f"  Events inserted  : {total_events_inserted:,}\n"
            f"  Rows skipped     : {total_rows_skipped:,}\n"
            f"  Duration         : {duration_minutes:.1f} minutes\n\n"
            + (
                "Failed files:\n" +
                "\n".join(f"  - {f['filename']}: {f.get('error')}"
                          for f in failed_files) + "\n\n"
                if failed_files else ""
            ) +
            "Next step: python seeds/load_historical_territories.py"
        )
    )

    conn_check = get_connection()
    cursor     = conn_check.cursor()
    cursor.execute("SELECT COUNT(*) FROM hist_flight_events WHERE source = %s",
                   (SOURCE,))
    final_count = cursor.fetchone()[0]
    cursor.close()
    conn_check.close()

    log.info(f"  hist_flight_events total rows: {final_count:,}")
    log.info("\nDone. Next: python seeds/load_historical_territories.py")


if __name__ == "__main__":
    # Required on Windows for multiprocessing
    # Without this, each worker spawns infinite child processes
    mp.freeze_support()
    main()