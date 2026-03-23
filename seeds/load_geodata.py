"""
seeds/load_geodata.py

WHAT THIS SCRIPT DOES
---------------------
Loads the dim_country table with country boundary data from
the Natural Earth dataset. This is the most technically
interesting seed script because it uses GeoPandas to read
Shapefile data and PostGIS to store boundary polygons.

The boundary column is the heart of the territory detection
logic. When an aircraft position ping arrives, PostGIS checks
the coordinates against every country boundary using
ST_Contains() to determine which country the aircraft is
flying over.

WHAT IT READS
-------------
seeds/ne_110m_admin_0_countries.shp — already on machine
    All four Shapefile components in the same folder:
    .shp, .shx, .dbf, .prj

WHAT IT FILLS
-------------
dim_country — one row per country with PostGIS boundary polygon

HOW IT WORKS
------------
GeoPandas reads the Shapefile and gives us a GeoDataFrame —
a pandas DataFrame where one column contains geometry objects
(POLYGON or MULTIPOLYGON). We extract the columns we need,
convert the geometry to WKT (Well Known Text) format, and
insert into PostgreSQL using ST_GeomFromText() which converts
WKT back into a PostGIS geometry object stored in the
boundary column.

WKT is simply the text representation of a geometry:
    POLYGON ((3.39 6.45, 3.40 6.46, ...))
    MULTIPOLYGON (((3.39 6.45, ...)), ((4.0 7.0, ...)))

HOW TO RUN
----------
Make sure Docker containers are running:
    docker compose up -d

Then from project root (transit-radar-411/):
    python seeds/load_geodata.py

Expected runtime: under 1 minute.
"""

import os
import sys
import psycopg2
from psycopg2.extras import execute_values
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

SEEDS_DIR  = os.path.dirname(__file__)
SHAPEFILE  = os.path.join(SEEDS_DIR, "ne_110m_admin_0_countries.shp")


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
# STEP 1 — READ NATURAL EARTH SHAPEFILE
# ─────────────────────────────────────────────────────────────
def read_shapefile():
    """
    Reads the Natural Earth countries Shapefile using GeoPandas.

    GeoPandas reads all four Shapefile components together
    (.shp, .shx, .dbf, .prj) and returns a GeoDataFrame —
    a DataFrame where one column contains geometry objects.

    We select only the 9 columns we need and rename them
    to match our dim_country column names.
    """
    print("\n[Step 1/3] Reading Natural Earth Shapefile...")

    if not os.path.exists(SHAPEFILE):
        print(f"  ERROR: {SHAPEFILE} not found.")
        print(f"  Make sure all four Shapefile files are in your seeds folder:")
        print(f"  .shp, .shx, .dbf, .prj")
        sys.exit(1)

    try:
        import geopandas as gpd
    except ImportError:
        print("  ERROR: geopandas is not installed.")
        print("  Run: pip install geopandas")
        sys.exit(1)

    gdf = gpd.read_file(SHAPEFILE)
    print(f"  Read {len(gdf):,} country rows.")
    print(f"  Total columns in file: {len(gdf.columns)}")

    # Select only the columns we need and rename them
    # Natural Earth uses uppercase column names
    columns_map = {
        "ISO_A2":    "iso2",
        "ISO_A3":    "iso3",
        "NAME":      "country_name",
        "FORMAL_EN": "formal_name",
        "CONTINENT": "continent",
        "SUBREGION": "subregion",
        "LABEL_X":   "centroid_lon",
        "LABEL_Y":   "centroid_lat",
        "POP_EST":   "population",
        "geometry":  "geometry"
    }

    gdf = gdf[list(columns_map.keys())].rename(columns=columns_map)
    print(f"  Selected {len(gdf.columns)} columns.")

    return gdf


# ─────────────────────────────────────────────────────────────
# STEP 2 — MAP ONE ROW TO dim_country COLUMNS
# ─────────────────────────────────────────────────────────────
def map_row(row):
    """
    Translates one Natural Earth GeoDataFrame row into a tuple
    matching the exact column order of our INSERT statement.

    Key decisions:
    1. iso2 = '-99' means no official ISO code — store as NULL
    2. iso3 = '-99' means no official ISO code — store as NULL
    3. formal_name uses FORMAL_EN with country_name as fallback
       when FORMAL_EN is NULL or empty
    4. geometry is converted to WKT string for insertion
       via ST_GeomFromText() in the SQL statement
    """
    import pandas as pd

    def clean_str(val):
        s = str(val).strip() if val is not None else ""
        return s if s and s.lower() not in ("nan", "none", "") else None

    def clean_iso(val):
        """
        Cleans ISO codes — converts -99 to NULL.
        Natural Earth uses -99 for territories with no
        official ISO code e.g. Kosovo, Western Sahara.
        """
        if (clean_str(val) == "CN-TW"):
            # for iso2 > 2 chars
            s = "TW"
            print(s)
            return s if s and s.lower() not in ("nan", "none") else None
        s = clean_str(val)
        if s == "-99" or s == "-9":
            return None
        # Must be exactly 2 chars for iso2, 3 chars for iso3
        return s

    # Derive country_name — use FORMAL_EN with NAME as fallback
    formal_name   = clean_str(row.get("formal_name"))
    country_name = clean_str(row.get("country_name"))
    formal_name  = formal_name if formal_name else country_name

    # Skip rows with no country name at all
    if not country_name:
        return None

    # Convert geometry to WKT (Well Known Text) string
    # WKT is the text representation of a geometry:
    # POLYGON ((lon lat, lon lat, ...))
    # MULTIPOLYGON (((lon lat, ...)), ((lon lat, ...)))
    # PostGIS ST_GeomFromText() converts this back to a
    # proper geometry object when we insert it.
    geometry = row.get("geometry")
    if geometry is None or (hasattr(geometry, 'is_empty') and geometry.is_empty):
        wkt = None
    else:
        wkt = geometry.wkt

    # Convert population — Natural Earth stores as float
    # e.g. 218541212.0 — we store as integer
    try:
        population = int(float(row.get("population", 0)))
        if population <= 0:
            population = None
    except (ValueError, TypeError):
        population = None

    return (
        clean_iso(row.get("iso2")),         # 1  iso2
        clean_iso(row.get("iso3")),         # 2  iso3
        country_name,                       # 3  country_name
        clean_str(row.get("formal_en")),    # 4  formal_name
        clean_str(row.get("continent")),    # 5  continent
        clean_str(row.get("subregion")),    # 6  subregion
        row.get("centroid_lon"),            # 7  centroid_lon
        row.get("centroid_lat"),            # 8  centroid_lat
        population,                         # 9  population
        wkt,                                # 10  boundary (WKT string)
    )


# ─────────────────────────────────────────────────────────────
# STEP 3 — INSERT INTO dim_country
# ─────────────────────────────────────────────────────────────
def load_countries(rows, conn):
    """
    Inserts all country rows into dim_country.

    The boundary column is special — we cannot use execute_values
    directly with geometry objects. Instead we use a template
    that calls ST_GeomFromText() on the WKT string and
    ST_SetSRID() to assign coordinate system 4326 (WGS84 —
    the same system used by GPS and OpenSky).

    ST_GeomFromText(wkt, 4326) converts:
        "POLYGON ((3.39 6.45, ...))"
    into a PostGIS geometry object stored in the boundary column.
    """
    if not rows:
        print("  No rows to insert.")
        return 0
    
    for i, row in enumerate(rows):
        val = row[0]  # position 1 = iso2 (0-indexed = 0)
        if val and len(str(val)) > 2:
            print(f"  VIOLATION at row {i}: iso2 > 2 ={repr(val)} full row={row}")
            break

    cursor = conn.cursor()
    try:
        # We use execute_values with a custom template
        # %s placeholders for regular columns
        # ST_SetSRID(ST_GeomFromText(%s), 4326) for the geometry
        execute_values(
            cursor,
            """
            INSERT INTO dim_country (
                iso2,
                iso3,
                country_name,
                formal_name,
                continent,
                subregion,
                centroid_lon,
                centroid_lat,
                population,
                boundary
            ) VALUES %s
            ON CONFLICT DO NOTHING
            """,
            rows,
            template="""(
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                ST_SetSRID(ST_GeomFromText(%s), 4326)
            )"""
        )
        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        print(f"  ERROR inserting countries: {e}")
        return 0
    finally:
        cursor.close()



# ─────────────────────────────────────────────────────────────
# VERIFICATION
# ─────────────────────────────────────────────────────────────
def verify(conn):
    """
    After loading, confirm row counts and test the ST_Contains
    function with a known coordinate — Lagos, Nigeria
    (longitude 3.39, latitude 6.45).
    """
    cursor = conn.cursor()

    # Total rows
    cursor.execute("SELECT COUNT(*) FROM dim_country")
    total = cursor.fetchone()[0]
    print(f"  dim_country total rows: {total:,}")

    # Rows with boundary polygon
    cursor.execute("SELECT COUNT(*) FROM dim_country WHERE boundary IS NOT NULL")
    with_boundary = cursor.fetchone()[0]
    print(f"  Rows with boundary    : {with_boundary:,}")

    # Rows with no iso2 (disputed territories)
    cursor.execute("SELECT COUNT(*) FROM dim_country WHERE iso2 IS NULL")
    no_iso2 = cursor.fetchone()[0]
    print(f"  Rows with no iso2     : {no_iso2:,} (disputed territories)")

    cursor.close()

    # Test ST_Contains with Lagos coordinates
    # This confirms PostGIS territory detection is working
    print(f"\n  Testing ST_Contains with Lagos coordinates (3.39, 6.45)...")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT country_name, iso2, continent
        FROM dim_country
        WHERE ST_Contains(
            boundary,
            ST_SetSRID(ST_Point(3.39, 6.45), 4326)
        )
    """)
    result = cursor.fetchone()
    cursor.close()

    if result:
        print(f"  Result: {result[0]} ({result[1]}) — {result[2]}")
        print(f"  ST_Contains is working correctly.")
    else:
        print(f"  WARNING: No country found for Lagos coordinates.")
        print(f"  This may indicate a geometry issue.")

    # Show a sample of African countries
    cursor = conn.cursor()
    cursor.execute("""
        SELECT iso2, iso3, country_name, continent, subregion
        FROM dim_country
        WHERE continent = 'Africa'
        ORDER BY country_name
        LIMIT 10
    """)
    rows = cursor.fetchall()
    cursor.close()

    print(f"\n  Sample African countries:")
    print(f"  {'ISO2':<6} {'ISO3':<6} {'Name':<40} {'Subregion'}")
    print(f"  {'-'*6} {'-'*6} {'-'*40} {'-'*25}")
    for row in rows:
        print(f"  {str(row[0]):<6} {str(row[1]):<6} {str(row[2]):<40} {str(row[4])}")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  Transit Radar 411 — Geodata Seed Loader")
    print("=" * 60)

    # Step 1 — Read Shapefile
    gdf = read_shapefile()

    # Connect to PostgreSQL
    conn = get_connection()

    # Step 2 — Map all rows
    print(f"\n[Step 2/3] Mapping all rows...")
    rows    = []
    skipped = 0

    for _, row in gdf.iterrows():
        mapped = map_row(row)
        if mapped:
            rows.append(mapped)
        else:
            skipped += 1

    print(f"  Mapped   {len(rows):,} rows.")
    print(f"  Skipped  {skipped:,} rows (no country name).")

    # Step 3 — Insert into dim_country
    print(f"\n[Step 3/3] Inserting into dim_country...")
    inserted = load_countries(rows, conn)
    print(f"  Inserted {inserted:,} rows.")

    # Verify
    verify(conn)

    conn.close()
    print("\nload_geodata.py complete. dim_country is ready.")
    print("Next: run seeds/load_historical_states.py")


if __name__ == "__main__":
    main()