"""
seeds/load_aircraft.py

WHAT THIS SCRIPT DOES
---------------------
Loads the dim_aircraft table with aircraft data from the OpenSky
Network aircraft database. This is the "customer master file" of
the entire pipeline — every live flight event that arrives from
OpenSky will be validated against this table via the icao24 code.

The script runs once before the pipeline starts. It does not need
to be run again unless database is wiped.

WHAT IT READS
-------------
1. OpenSky aircraftDatabase.csv — downloaded at runtime
   URL: https://opensky-network.org/datasets/metadata/aircraftDatabase.csv
   Contains ~500,000 aircraft with manufacturer, model, airline etc.
   Downloaded in chunks of 10,000 rows to avoid memory issues.

2. seeds/aircraft_reg-prefixes.csv — already on machine
   Maps registration prefix to ISO2 country code.
   Example: "5N" -> "NG"

3. seeds/countries_code.csv — already on machine
   Maps ISO2 country code to full country name.
   Example: "NG" -> "Nigeria"

WHAT IT FILLS
-------------
dim_aircraft — one row per aircraft

HOW TO RUN
----------
Make sure  Docker containers are running:
    docker compose up -d

Then from  project root (transit-radar-411/):
    python seeds/load_aircraft.py

Expected runtime: 3-8 minutes depending on  internet speed.
"""

import os
import sys
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from io import StringIO
from dotenv import load_dotenv

import logging
import os

# Get current file name without extension
current_file = os.path.splitext(os.path.basename(__file__))[0]

# Set log filename based on current file
log_filename = f"{current_file}.log"

# Configure logging
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ─────────────────────────────────────────────────────────────
# LOAD ENVIRONMENT VARIABLES
# Reads  .env file so we do not hardcode passwords.
# The .env file must exist in  project root folder.
# ─────────────────────────────────────────────────────────────
load_dotenv()

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

# Database connection — values come from  .env file
DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB"),
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

# OpenSky aircraft database URL
OPENSKY_AIRCRAFT_URL = (
    "https://opensky-network.org/datasets/metadata/aircraftDatabase.csv"
)

# Local seed files — relative to the seeds folder
SEEDS_DIR           = os.path.dirname(__file__)
REG_PREFIXES_FILE   = os.path.join(SEEDS_DIR, "aircraft_reg-prefixes.csv")
COUNTRIES_FILE      = os.path.join(SEEDS_DIR, "countries_code.csv")

# How many rows to process and insert in each batch
CHUNK_SIZE = 10_000


# ─────────────────────────────────────────────────────────────
# DATABASE CONNECTION
# ─────────────────────────────────────────────────────────────

def get_connection():
    """
    Creates and returns a PostgreSQL connection.
    Exits with an error message if the connection fails.
    This usually means Docker is not running or the .env
    file has incorrect credentials.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logging.info("  Connected to PostgreSQL successfully.")
        return conn
    except Exception as e:
        logging.info(f"  ERROR: Could not connect to PostgreSQL.")
        logging.info(f"  Details: {e}")
        logging.info(f"  Make sure docker compose up -d is running.")
        sys.exit(1)


# ─────────────────────────────────────────────────────────────
# STEP 1 — BUILD LOOKUP DICTIONARIES FROM LOCAL FILES
# ─────────────────────────────────────────────────────────────

def build_country_lookups():
    """
    Reads the two local CSV files and builds two dictionaries.

    prefix_to_iso2: maps registration prefix to ISO2 code
        Example: {"5N": "NG", "G": "GB", "A6": "AE"}

    iso2_to_name: maps ISO2 code to full country name
        Example: {"NG": "Nigeria", "GB": "United Kingdom"}

    These are used together to derive country_iso2 and
    country_of_reg for each aircraft from its registration.

    Why dictionaries and not CSV searches?
    Because we do this lookup 500,000 times. A dictionary
    lookup is almost instant. Searching a CSV file each time
    would take hours.
    """
    logging.info("\n[Step 1/4] Building country lookup dictionaries...")

    # Validate local files exist before doing anything else
    if not os.path.exists(REG_PREFIXES_FILE):
        logging.info(f"  ERROR: {REG_PREFIXES_FILE} not found.")
        logging.info(f"  Download it from:")
        logging.info(f"  https://raw.githubusercontent.com/vradarserver/standing-data")
        logging.info(f"  /main/registration-prefixes/schema-01/reg-prefixes.csv")
        sys.exit(1)

    if not os.path.exists(COUNTRIES_FILE):
        logging.info(f"  ERROR: {COUNTRIES_FILE} not found.")
        logging.info(f"  Download it from:")
        logging.info(f"  https://raw.githubusercontent.com/vradarserver/standing-data")
        logging.info(f"  /main/countries/schema-01/countries.csv")
        sys.exit(1)

    # Read registration prefix to ISO2 mapping
    # Columns in file: Prefix, CountryISO2
    reg_df = pd.read_csv(REG_PREFIXES_FILE)
    reg_df.columns = [c.strip() for c in reg_df.columns]
    prefix_to_iso2 = dict(zip(
        reg_df["Prefix"].astype(str).str.strip(),
        reg_df["CountryISO2"].astype(str).str.strip()
    ))
    logging.info(f"  Loaded {len(prefix_to_iso2):,} registration prefixes.")

    # Read ISO2 to country name mapping
    # Columns in file: ISO, Name
    countries_df = pd.read_csv(COUNTRIES_FILE)
    countries_df.columns = [c.strip() for c in countries_df.columns]
    iso2_to_name = dict(zip(
        countries_df["ISO"].astype(str).str.strip(),
        countries_df["Name"].astype(str).str.strip()
    ))
    logging.info(f"  Loaded {len(iso2_to_name):,} country names.")

    return prefix_to_iso2, iso2_to_name


def get_country_from_registration(registration, prefix_to_iso2, iso2_to_name):
    """
    Given a registration like "5N-JJJ", extracts the prefix "5N",
    looks it up in prefix_to_iso2 to get "NG", then looks up "NG"
    in iso2_to_name to get "Nigeria".

    Returns (country_iso2, country_of_reg) as a tuple.
    Returns (None, None) if registration is missing or prefix
    not found in the lookup tables.

    How prefixes work:
      "5N-JJJ"  -> prefix = "5N"  (split on hyphen)
      "GABCD"   -> try "GAB", "GA", "G" (no hyphen, try lengths)
      "N12345"  -> try "N12", "N1", "N" (US format, no hyphen)
    """
    if not registration or pd.isna(registration):
        return None, None

    registration = str(registration).strip()

    if "-" in registration:
        prefix = registration.split("-")[0].strip()
    else:
        prefix = None
        for length in [3, 2, 1]:
            candidate = registration[:length]
            if candidate in prefix_to_iso2:
                prefix = candidate
                break

    if not prefix or prefix not in prefix_to_iso2:
        return None, None

    country_iso2 = prefix_to_iso2[prefix]

    # ── TEMPORARY DEBUG ──────────────────────────────────────
    #logging.info(f"  DEBUG: prefix={prefix} country_iso2={country_iso2} type={type(country_iso2)} len={len(str(country_iso2))}")
    # ─────────────────────────────────────────────────────────

    country_of_reg = iso2_to_name.get(country_iso2)

    return country_iso2, country_of_reg


# ─────────────────────────────────────────────────────────────
# STEP 2 — DOWNLOAD OPENSKY AIRCRAFT DATABASE
# ─────────────────────────────────────────────────────────────

def download_opensky_aircraft():
    """
    Downloads the OpenSky aircraft database CSV as a stream.

    stream=True means Python downloads the file progressively
    line by line rather than loading the entire 500MB into
    memory at once. This is essential for large files.

    Returns the response object — we iterate over it in main().
    """
    logging.info(f"\n[Step 2/4] Connecting to OpenSky aircraft database...")
    logging.info(f"  URL: {OPENSKY_AIRCRAFT_URL}")
    logging.info(f"  This file is large. Processing will begin shortly...")

    try:
        response = requests.get(
            OPENSKY_AIRCRAFT_URL,
            stream=True,
            timeout=120
        )
        response.raise_for_status()
        logging.info(f"  Connection established. Processing in chunks of {CHUNK_SIZE:,}...")
        return response
    except requests.exceptions.Timeout:
        logging.info("  ERROR: Connection timed out after 2 minutes.")
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        logging.info(f"  ERROR: Download failed: {e}")
        sys.exit(1)


# ─────────────────────────────────────────────────────────────
# STEP 3 — MAP ONE OPENSKY ROW TO dim_aircraft COLUMNS
# ─────────────────────────────────────────────────────────────

def map_row(row, prefix_to_iso2, iso2_to_name):
    """
    Translates one OpenSky CSV row into a tuple that matches
    the exact column order of our INSERT statement.

    This is the translator between OpenSky's naming convention
    and our dim_aircraft naming convention.

    OpenSky name          Our dim_aircraft column
    ─────────────────     ───────────────────────
    icao24                icao24
    registration          registration
    manufacturername      manufacturername
    model                 model
    typecode              type_code
    icaoaircrafttype      icao_aircraft_type
    operator              airline_name
    operatorcallsign      operatorcallsign
    operatoricao          airline_icao
    operatoriata          airline_iata
    (derived)             country_iso2
    (derived)             country_of_reg
    firstflightdate       first_flight_date
    engines               engines
    categoryDescription   weight_category
    status                is_active

    Returns None if the row has no icao24 — we cannot insert
    a row without the primary business key.
    """
    # ── FIX 1: treat "nan" as missing ────────────────────────
    if not icao24 or icao24 == "nan":
        return None
    
    icao24 = str(row.get("icao24", "")).strip().lower()
    if not icao24:
        return None

    # Get registration and derive country from prefix
    registration = str(row.get("registration", "")).strip() or None
    country_iso2, country_of_reg = get_country_from_registration(
        registration, prefix_to_iso2, iso2_to_name
    )

    # Convert first_flight_date string to a Python date object
    # OpenSky stores it as "8/15/2017" — PostgreSQL needs a date
    first_flight_date = None
    raw_date = str(row.get("firstflightdate", "")).strip()
    if raw_date and raw_date not in ("", "nan", "None"):
        for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y"]:
            try:
                first_flight_date = datetime.strptime(raw_date, fmt).date()
                break
            except ValueError:
                continue

    # Helper to clean string values — converts "nan" to None
    def clean(val):
        s = str(row.get(val, "")).strip()
        return s if s and s.lower() not in ("nan", "none") else None

    return (
        icao24,
        registration,
        clean("manufacturer"),
        clean("model"),
        clean("typecode"),
        clean("icaoaircrafttype"),
        clean("operator"),
        clean("operatorcallsign"),
        clean("operatoricao"),
        clean("operatoriata"),
        country_iso2,
        country_of_reg,
        first_flight_date,
        clean("engines"),
        clean("categoryDescription"),
        clean("status"),
        True
    )


# ─────────────────────────────────────────────────────────────
# STEP 4 — BULK INSERT A CHUNK INTO dim_aircraft
# ─────────────────────────────────────────────────────────────

def load_chunk(rows, conn, chunk_number):
    """
    Bulk-inserts a list of mapped aircraft rows into dim_aircraft.

    execute_values sends all rows in a single SQL statement.
    This is much faster than inserting one row at a time.

    ON CONFLICT (icao24) DO NOTHING means:
    If an aircraft with this icao24 already exists in the table,
    skip it silently without an error. This makes the script
    safe to run multiple times without creating duplicates.
    """
    if not rows:
        return 0
    
    # ── TEMPORARY DEBUG ──────────────────────────────────────
    if chunk_number == 1:
        logging.info(f"  DEBUG first row: {rows[0]}")

    cursor = conn.cursor()
    try:
        execute_values(
            cursor,
            """
            INSERT INTO dim_aircraft (
                icao24,
                registration,
                manufacturername,
                model,
                type_code,
                icao_aircraft_type,
                airline_name,
                operatorcallsign,
                airline_icao,
                airline_iata,
                country_iso2,
                country_of_reg,
                first_flight_date,
                engines,
                weight_category,
                is_active
            ) VALUES %s
            ON CONFLICT (icao24) DO NOTHING
            """,
            rows
        )
        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        logging.info(f"  ERROR in chunk {chunk_number}: {e}")
        logging.info(f"  DEBUG row: {rows[0]}")
        return 0
    finally:
        cursor.close()


# ─────────────────────────────────────────────────────────────
# VERIFICATION
# ─────────────────────────────────────────────────────────────

def verify(conn):
    """
    Queries dim_aircraft after loading to confirm the row count.
    This is the post-load reconciliation check — confirms the
    data actually made it into the database.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM dim_aircraft")
    count = cursor.fetchone()[0]
    cursor.close()
    logging.info(f"  dim_aircraft total rows: {count:,}")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    logging.info("=" * 60)
    logging.info("  Transit Radar 411 — Aircraft Seed Loader")
    logging.info("=" * 60)

    # Step 1 — Build lookup dictionaries
    prefix_to_iso2, iso2_to_name = build_country_lookups()

    # Connect to PostgreSQL
    conn = get_connection()

    # Step 2 — Connect to OpenSky download stream
    response = download_opensky_aircraft()

    # Step 3 and 4 — Read stream, map columns, insert in chunks
    logging.info(f"\n[Step 3/4] Processing and inserting...")

    chunk_number   = 0
    total_inserted = 0
    total_skipped  = 0
    line_buffer    = []
    header         = None

    for raw_line in response.iter_lines():
        line = raw_line.decode("utf-8", errors="replace")

        # First line is always the header
        if header is None:
            header = line
            continue

        line_buffer.append(line)

        # When we have a full chunk, process it
        if len(line_buffer) >= CHUNK_SIZE:
            chunk_number += 1

            # Parse lines as a mini CSV with the header prepended
            chunk_csv = header + "\n" + "\n".join(line_buffer)
            df = pd.read_csv(
                StringIO(chunk_csv),
                low_memory=False,
                dtype=str
            )
            df.columns = [c.lower().strip() for c in df.columns]

            # Map each row and collect valid ones
            rows = []
            for _, row in df.iterrows():
                mapped = map_row(row, prefix_to_iso2, iso2_to_name)
                if mapped:
                    rows.append(mapped)
                else:
                    total_skipped += 1

            # Bulk insert this chunk
            inserted = load_chunk(rows, conn, chunk_number)
            total_inserted += inserted

            logging.info(
                f"  Chunk {chunk_number:>4} | "
                f"inserted {inserted:>6,} | "
                f"total {total_inserted:>8,}"
            )

            # Clear buffer for next chunk
            line_buffer = []

    # Process remaining lines that did not fill a full chunk
    if line_buffer:
        chunk_number += 1
        chunk_csv = header + "\n" + "\n".join(line_buffer)
        df = pd.read_csv(StringIO(chunk_csv), low_memory=False, dtype=str)
        df.columns = [c.lower().strip() for c in df.columns]

        rows = []
        for _, row in df.iterrows():
            mapped = map_row(row, prefix_to_iso2, iso2_to_name)
            if mapped:
                rows.append(mapped)
            else:
                total_skipped += 1

        inserted = load_chunk(rows, conn, chunk_number)
        total_inserted += inserted
        logging.info(
            f"  Chunk {chunk_number:>4} | "
            f"inserted {inserted:>6,} | "
            f"total {total_inserted:>8,}"
        )

    # Step 4 — Verify
    logging.info(f"\n[Step 4/4] Verifying...")
    logging.info(f"  Total rows inserted : {total_inserted:,}")
    logging.info(f"  Total rows skipped  : {total_skipped:,} (missing icao24)")
    verify(conn)

    conn.close()
    logging.info("\nload_aircraft.py complete. dim_aircraft is ready.")
    logging.info("Next: run seeds/load_airports.py")


if __name__ == "__main__":
    main()
