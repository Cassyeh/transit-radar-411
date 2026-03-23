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
OPENSKY_AIRCRAFT_FILE = os.path.join(SEEDS_DIR,"aircraftDatabase.csv")

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
        print("  Connected to PostgreSQL successfully.")
        return conn
    except Exception as e:
        print(f"  ERROR: Could not connect to PostgreSQL.")
        print(f"  Details: {e}")
        print(f"  Make sure docker compose up -d is running.")
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
    print("\n[Step 1/4] Building country lookup dictionaries...")

    # Validate local files exist before doing anything else
    if not os.path.exists(REG_PREFIXES_FILE):
        print(f"  ERROR: {REG_PREFIXES_FILE} not found.")
        print(f"  Download it from:")
        print(f"  https://raw.githubusercontent.com/vradarserver/standing-data")
        print(f"  /main/registration-prefixes/schema-01/reg-prefixes.csv")
        sys.exit(1)

    if not os.path.exists(COUNTRIES_FILE):
        print(f"  ERROR: {COUNTRIES_FILE} not found.")
        print(f"  Download it from:")
        print(f"  https://raw.githubusercontent.com/vradarserver/standing-data")
        print(f"  /main/countries/schema-01/countries.csv")
        sys.exit(1)

    # Read registration prefix to ISO2 mapping
    # Columns in file: Prefix, CountryISO2
    # Read registration prefix to ISO2 mapping
    reg_df = pd.read_csv(REG_PREFIXES_FILE, keep_default_na=False) #Pandas to treat 'NA' as string not NaN
    reg_df.columns = [c.strip() for c in reg_df.columns]
    prefix_to_iso2 = dict(zip(
        reg_df["Prefix"].astype(str).str.strip(),
        reg_df["CountryISO2"].astype(str).str.strip()
    ))
    #print(prefix_to_iso2)
    print(f"  Loaded {len(prefix_to_iso2):,} registration prefixes.")

    # Read ISO2 to country name mapping
    # Columns in file: ISO, Name
    countries_df = pd.read_csv(COUNTRIES_FILE, keep_default_na=False) #Pandas to treat 'NA' as string not NaN
    countries_df.columns = [c.strip() for c in countries_df.columns]
    iso2_to_name = dict(zip(
        countries_df["ISO"].astype(str).str.strip(),
        countries_df["Name"].astype(str).str.strip()
    ))
    print(f"  Loaded {len(iso2_to_name):,} country names.")

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
    # if registration == 'V5-ANF':
    #     print("Country_iso2 is: ", country_iso2)
    #     print(prefix)
    # Safety check — if the lookup returned 'nan' treat it as None
    if not country_iso2 or str(country_iso2).lower() in ("nan", "none", ""):
        country_iso2 = None

    # Safety check — iso2 must be exactly 2 characters
    if len(str(country_iso2).strip()) > 2:
        country_iso2 = None

    # ── TEMPORARY DEBUG ──────────────────────────────────────
    #print(f"  DEBUG: prefix={prefix} country_iso2={country_iso2} type={type(country_iso2)} len={len(str(country_iso2))}")
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
    print(f"\n[Step 2/4] Connecting to OpenSky aircraft database...")
    print(f"  URL: {OPENSKY_AIRCRAFT_URL}")
    print(f"  This file is large. Processing will begin shortly...")

    try:
        response = requests.get(
            OPENSKY_AIRCRAFT_URL,
            stream=True,
            timeout=120
        )
        response.raise_for_status()
        print(f"  Connection established. Processing in chunks of {CHUNK_SIZE:,}...")
        return response
    except requests.exceptions.Timeout:
        print("  ERROR: Connection timed out after 2 minutes.")
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        print(f"  ERROR: Download failed: {e}")
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
    # Get icao24 first — if missing or nan, skip this row entirely
    raw_icao24 = row.get("icao24", "")
    if pd.isna(raw_icao24):
        return None
    
    icao24 = str(raw_icao24).strip().lower()
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
        if (val == "icaoaircrafttype"):
            # for aircrafttype > 10 chars
            text = str(row.get(val, "")).strip()
            if len(text) > 10 and "BOEING ICAO Manufacturer Code" in text:
                if "(" in text and ")" in text:
                    extracted = text.split("(")[-1].split(")")[0]
                    s = extracted
                    return s if s and s.lower() not in ("nan", "none") else None
        if (val == "operatoriata"):
            # for operatoriata > 10 chars
            text = str(row.get(val, "")).strip()
            if len(text) > 10 and "SWA	 Airline IATA" in text:
                s = "WN"
                print(s)
                return s if s and s.lower() not in ("nan", "none") else None
        s = str(row.get(val, "")).strip()
        return s if s and s.lower() not in ("nan", "none") else None

    return (
        icao24,
        registration,
        clean("manufacturername"),
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
    # if chunk_number == 1:
    #     print(f"  DEBUG first row: {rows[0]}")
        # row = rows[0]
        # for i, val in enumerate(row, 1):
        #     print(f"  Position {i:>2}: {repr(val)}")
    
    for i, row in enumerate(rows):
        val = row[10]  # position 11 = country_iso2 (0-indexed = 10)
        if val and len(str(val)) > 2:
            print(f"  VIOLATION at row {i}: country_iso2={repr(val)} full row={row}")
            break

    for i, row in enumerate(rows):
        val = row[5]  # position 6 = icao_aircraft_type (0-indexed = 5)
        if val and len(str(val)) > 10:
            print(f"  VIOLATION at row {i}: icao_aircraft_type={repr(val)} full row={row}")
            break

    for i, row in enumerate(rows):
        val = row[9]  # position 10 = airline_iata (0-indexed = 9)
        if val and len(str(val)) > 10:
            print(f"  VIOLATION at row {i}: airline_iata={repr(val)} full row={row}")
            break

    cursor = conn.cursor()
    print("Connected to DB successfully")
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
        print(f"  ERROR in chunk {chunk_number}: {e}")
        print(f"  DEBUG row: {rows[0]}")
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
    print(f"  dim_aircraft total rows: {count:,}")
    return(count)


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Transit Radar 411 — Aircraft Seed Loader")
    print("=" * 60)

    # Step 1 — Build lookup dictionaries
    prefix_to_iso2, iso2_to_name = build_country_lookups()

    # Connect to PostgreSQL
    conn = get_connection()

    # Step 2 — Download entire CSV at once into pandas
    print(f"\n[Step 2/4] Downloading OpenSky aircraft database...")
    #print(f"  URL: {OPENSKY_AIRCRAFT_URL}")
    print(f"  Downloading entire file into memory...")

    df = pd.read_csv(OPENSKY_AIRCRAFT_FILE)
    df.columns = [c.lower().strip() for c in df.columns]
    print(f"  Downloaded {len(df):,} rows.")
    print(f"  Unique icao24 aircraft codes: {df['icao24'].nunique():,}")

    #Step 3 — Map all rows at once
    print(f"\n[Step 3/4] Mapping all rows...")
    rows = []
    skipped = 0
    for _, row in df.iterrows():
        mapped = map_row(row, prefix_to_iso2, iso2_to_name)
        if mapped:
            rows.append(mapped)
        else:
            skipped += 1

    print(f"  Mapped {len(rows):,} rows. Skipped {skipped:,}.")

    # Step 4 — Insert all at once
    print(f"\n[Step 4/4] Inserting all rows...")
    inserted = load_chunk(rows, conn, 1)
    print(f"  Inserted {inserted:,} rows.")

    # Verify
    total_rows_in_table = verify(conn)
    conn.close()
    print(f"  Total rows skipped  : {len(df) - total_rows_in_table:,} (missing icao24)")
    print("\nload_aircraft.py complete. dim_aircraft is ready.")
    print("Next: run seeds/load_airports.py")

if __name__ == "__main__":
    main()
