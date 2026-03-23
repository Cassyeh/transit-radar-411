"""
seeds/load_airports.py

WHAT THIS SCRIPT DOES
---------------------
Loads the dim_airport table with airport data from OurAirports —
the most actively maintained free global airport database.

Every flight event references an origin and destination airport
by its IATA code. This table provides the full context behind
that code — name, location, country, region and whether it
handles commercial flights.

The latitude and longitude columns are critical — they are used
by the geospatial territory detection logic to determine which
country or territory an airport sits inside.

WHAT IT READS
-------------
1. OurAirports airports.csv — downloaded at runtime
   URL: https://davidmegginson.github.io/ourairports-data/airports.csv
   Contains every airport in the world with coordinates,
   IATA/ICAO codes, type, and country information.

2. seeds/regions.csv — already on machine
   Maps ISO region code to full region name.
   Example: "NG-LA" -> "Lagos"

WHAT IT FILLS
-------------
dim_airport — one row per airport

UNIQUE KEY
----------
ident is the unique key — every OurAirports airport has one.
airport_iata_code is allowed to be NULL for airports that
do not have an IATA code (military, private, small strips).

HOW TO RUN
----------
Make sure Docker containers are running:
    docker compose up -d

Then from project root (transit-radar-411/):
    python seeds/load_airports.py

Expected runtime: under 1 minute.
"""

import os
import sys
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from io import StringIO
from dotenv import load_dotenv

load_dotenv()

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

AIRPORTS_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
SEEDS_DIR    = os.path.dirname(__file__)
REGIONS_FILE = os.path.join(SEEDS_DIR, "regions.csv")


# ─────────────────────────────────────────────────────────────
# DATABASE CONNECTION
# ─────────────────────────────────────────────────────────────
def get_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("  Connected to PostgreSQL successfully.")
        return conn
    except Exception as e:
        print(f"  ERROR: Could not connect to PostgreSQL: {e}")
        print(f"  Make sure docker compose up -d is running.")
        sys.exit(1)


# ─────────────────────────────────────────────────────────────
# STEP 1 — BUILD REGION LOOKUP FROM LOCAL FILE
# ─────────────────────────────────────────────────────────────
def build_region_lookup():
    """
    Reads regions.csv and builds a dictionary mapping
    ISO region code to full region name.

    Example:
        {"NG-LA": "Lagos", "SO-GA": "Galguduud",
         "FR-IDF": "Ile-de-France"}

    keep_default_na=False prevents Pandas from converting
    values like "NA" (Namibia) to NaN.
    """
    print("\n[Step 1/4] Building region lookup dictionary...")

    if not os.path.exists(REGIONS_FILE):
        print(f"  ERROR: {REGIONS_FILE} not found.")
        print(f"  Download from:")
        print(f"  https://davidmegginson.github.io/ourairports-data/regions.csv")
        sys.exit(1)

    df = pd.read_csv(REGIONS_FILE, keep_default_na=False)
    df.columns = [c.strip().lower() for c in df.columns]

    # Columns in regions.csv: id, code, local_code, name,
    # continent, iso_country, wikipedia_link, keywords
    # We only need: code -> name
    region_lookup = dict(zip(
        df["code"].astype(str).str.strip(),
        df["name"].astype(str).str.strip()
    ))

    print(f"  Loaded {len(region_lookup):,} region names.")
    return region_lookup


# ─────────────────────────────────────────────────────────────
# STEP 2 — DOWNLOAD AIRPORTS CSV AT RUNTIME
# ─────────────────────────────────────────────────────────────
def download_airports():
    """
    Downloads OurAirports airports.csv into a pandas DataFrame.
    The file is small enough (~10MB) to load entirely at once.
    """
    print(f"\n[Step 2/4] Downloading OurAirports airports database...")
    print(f"  URL: {AIRPORTS_URL}")

    try:
        response = requests.get(AIRPORTS_URL, timeout=60)
        response.raise_for_status()
        df = pd.read_csv(
            StringIO(response.text),
            low_memory=False,
            keep_default_na=False,
            dtype=str
        )
        df.columns = [c.strip().lower() for c in df.columns]
        print(f"  Downloaded {len(df):,} airport rows.")
        return df
    except requests.exceptions.Timeout:
        print("  ERROR: Download timed out.")
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        print(f"  ERROR: Download failed: {e}")
        sys.exit(1)


# ─────────────────────────────────────────────────────────────
# STEP 3 — MAP ONE OURAIRPORTS ROW TO dim_airport COLUMNS
# ─────────────────────────────────────────────────────────────
def map_row(row, region_lookup):
    """
    Translates one OurAirports row into a tuple matching
    the exact column order of our INSERT statement.

    We don't skip airports with no IATA code — every
    airport with a valid ident gets inserted. This is because
    every airport has a valid unique identifier
    regardless of whether it has an IATA code.

    Airports with no IATA code will have NULL in
    airport_iata_code and cannot be used as foreign keys
    from flight event tables — but they are still valid
    reference data for geospatial queries.

    OurAirports column    Our dim_airport column
    ──────────────────    ──────────────────────
    ident                 ident              (unique key)
    iata_code             airport_iata_code  (NULL allowed)
    icao_code             airport_icao_code
    name                  airport_name
    type                  airport_type
    scheduled_service     is_commercial      (yes->TRUE, no->FALSE)
    municipality          municipality
    iso_country           iso_country
    continent             continent
    iso_region            iso_region
    (lookup from regions) region_name
    latitude_deg          latitude_deg
    longitude_deg         longitude_deg
    """
    def clean(val):
        s = str(row.get(val, "")).strip()
        return s if s and s.lower() not in ("nan", "none", "") else None

    def to_float(val):
        try:
            return float(str(row.get(val, "")).strip())
        except (ValueError, TypeError):
            return None

    # ident is required — skip only if somehow missing
    ident = clean("ident")
    if not ident:
        return None

    # IATA code — NULL is now allowed
    iata_code = clean("iata_code")

    # Convert scheduled_service yes/no to boolean
    scheduled     = str(row.get("scheduled_service", "")).strip().lower()
    is_commercial = True if scheduled == "yes" else False

    # Resolve full region name from iso_region code
    # using the regions.csv lookup dictionary
    iso_region  = clean("iso_region")
    region_name = region_lookup.get(iso_region) if iso_region else None

    return (
        ident,                     # 1  ident
        iata_code,                 # 2  airport_iata_code — NULL allowed
        clean("icao_code"),        # 3  airport_icao_code
        clean("name"),             # 4  airport_name
        clean("type"),             # 5  airport_type
        is_commercial,             # 6  is_commercial
        clean("municipality"),     # 7  municipality
        clean("iso_country"),      # 8  iso_country
        clean("continent"),        # 9  continent
        iso_region,                # 10 iso_region
        region_name,               # 11 region_name
        to_float("latitude_deg"),  # 12 latitude_deg
        to_float("longitude_deg"), # 13 longitude_deg
    )


# ─────────────────────────────────────────────────────────────
# STEP 4 — BULK INSERT INTO dim_airport
# ─────────────────────────────────────────────────────────────
def load_airports(rows, conn):
    """
    Bulk-inserts all mapped airport rows into dim_airport.
    ON CONFLICT (ident) DO NOTHING skips duplicates safely.
    ident is used as the conflict target because every airport
    has one — unlike airport_iata_code which can be NULL.
    """
    if not rows:
        print("  No rows to insert.")
        return 0
    
    for i, row in enumerate(rows):
        val = row[0]  # position 1 = ident (0-indexed = 0)
        if val and len(str(val)) > 10:
            print(f"  VIOLATION at row {i}: ident > 10 ={repr(val)} full row={row}")
            break
    
    for i, row in enumerate(rows):
        val = row[1]  # position 2 = airport_iata_code (0-indexed = 1)
        if val and len(str(val)) > 3:
            print(f"  VIOLATION at row {i}: airport_iata_code > 3 ={repr(val)} full row={row}")
            break
    
    for i, row in enumerate(rows):
        val = row[2]  # position 3 = ident (0-indexed = 2)
        if val and len(str(val)) > 4:
            print(f"  VIOLATION at row {i}: airport_icao_code > 4 ={repr(val)} full row={row}")
            break
    
    for i, row in enumerate(rows):
        val = row[4]  # position 5 = airport_type (0-indexed = 4)
        if val and len(str(val)) > 20:
            print(f"  VIOLATION at row {i}: airport_type > 20 ={repr(val)} full row={row}")
            break
    
    for i, row in enumerate(rows):
        val = row[7]  # position 8 = iso_country (0-indexed = 7)
        if val and len(str(val)) > 2:
            print(f"  VIOLATION at row {i}: iso_country > 2 ={repr(val)} full row={row}")
            break
    
    for i, row in enumerate(rows):
        val = row[8]  # position 9 = continent (0-indexed = 8)
        if val and len(str(val)) > 2:
            print(f"  VIOLATION at row {i}: continent > 2 ={repr(val)} full row={row}")
            break
    
    for i, row in enumerate(rows):
        val = row[9]  # position 10 = iso_region (0-indexed = 9)
        if val and len(str(val)) > 10:
            print(f"  VIOLATION at row {i}: iso_region > 10 ={repr(val)} full row={row}")
            break

    cursor = conn.cursor()
    try:
        execute_values(
            cursor,
            """
            INSERT INTO dim_airport (
                ident,
                airport_iata_code,
                airport_icao_code,
                airport_name,
                airport_type,
                is_commercial,
                municipality,
                iso_country,
                continent,
                iso_region,
                region_name,
                latitude_deg,
                longitude_deg
            ) VALUES %s
            ON CONFLICT (ident) DO NOTHING
            """,
            rows
        )
        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        print(f"  ERROR inserting airports: {e}")
        return 0
    finally:
        cursor.close()


# ─────────────────────────────────────────────────────────────
# VERIFICATION
# ─────────────────────────────────────────────────────────────
def verify(conn):
    """
    After loading, print row counts and a sample of African
    commercial airports to confirm data quality.
    """
    cursor = conn.cursor()

    # Total row count
    cursor.execute("SELECT COUNT(*) FROM dim_airport")
    total = cursor.fetchone()[0]
    print(f"  dim_airport total rows      : {total:,}")

    # Rows with IATA code
    cursor.execute("SELECT COUNT(*) FROM dim_airport WHERE airport_iata_code IS NOT NULL")
    with_iata = cursor.fetchone()[0]
    print(f"  Rows with IATA code         : {with_iata:,}")

    # Commercial airports
    cursor.execute("SELECT COUNT(*) FROM dim_airport WHERE is_commercial = TRUE")
    commercial = cursor.fetchone()[0]
    print(f"  Commercial airports         : {commercial:,}")

    cursor.close()

    # Sample of African airports
    cursor = conn.cursor()
    cursor.execute("""
        SELECT airport_iata_code, airport_name, municipality,
               iso_country, region_name, is_commercial
        FROM dim_airport
        WHERE iso_country IN ('NG', 'GH', 'KE', 'ZA', 'ET')
        AND is_commercial = TRUE
        ORDER BY iso_country, airport_name
        LIMIT 10
    """)
    rows = cursor.fetchall()
    cursor.close()

    print(f"\n  Sample African commercial airports:")
    print(f"  {'IATA':<6} {'Name':<35} {'City':<20} {'Country':<8} {'Region'}")
    print(f"  {'-'*6} {'-'*35} {'-'*20} {'-'*8} {'-'*20}")
    for row in rows:
        print(
            f"  {str(row[0]):<6} {str(row[1]):<35} "
            f"{str(row[2]):<20} {str(row[3]):<8} {str(row[4])}"
        )


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  Transit Radar 411 — Airport Seed Loader")
    print("=" * 60)

    # Step 1 — Build region lookup from local file
    region_lookup = build_region_lookup()

    # Connect to PostgreSQL
    conn = get_connection()

    # Step 2 — Download airports CSV at runtime
    df = download_airports()

    # Step 3 — Map all rows
    print(f"\n[Step 3/4] Mapping all rows...")
    rows    = []
    skipped = 0

    for _, row in df.iterrows():
        mapped = map_row(row, region_lookup)
        if mapped:
            rows.append(mapped)
        else:
            skipped += 1

    print(f"  Mapped   {len(rows):,} rows.")
    print(f"  Skipped  {skipped:,} rows (no ident).")

    # Step 4 — Bulk insert
    print(f"\n[Step 4/4] Inserting into dim_airport...")
    inserted = load_airports(rows, conn)
    print(f"  Inserted {inserted:,} rows.")

    # Verify
    verify(conn)

    conn.close()
    print("\nload_airports.py complete. dim_airport is ready.")
    print("Next: run seeds/load_geodata.py")


if __name__ == "__main__":
    main()