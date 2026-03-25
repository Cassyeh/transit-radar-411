"""
seeds/load_geodata_states.py

WHAT THIS SCRIPT DOES
---------------------
Loads the dim_state table with state/province boundary data
from the Natural Earth dataset. Uses GeoPandas to read
the Shapefile and PostGIS to store boundary polygons.

This is similar to load_geodata.py for countries but
for admin-1 states/provinces.

WHAT IT READS
-------------
seeds/ne_10m_admin_1_states_provinces.shp — already on machine
All four Shapefile components (.shp, .shx, .dbf, .prj) must be present.

WHAT IT FILLS
-------------
dim_state — one row per state/province with PostGIS geometry
and centroids.

HOW IT WORKS
------------
GeoPandas reads the Shapefile and gives a GeoDataFrame.
We filter by featurecla, map columns to our dim_state schema,
convert geometry to WKT for PostGIS, compute centroids,
and insert into PostgreSQL.
"""

import os
import sys
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from iso3_mapping import iso3_mapping

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
SHAPEFILE  = os.path.join(SEEDS_DIR, "ne_10m_admin_1_states_provinces.shp")


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
        sys.exit(1)


# ─────────────────────────────────────────────────────────────
# STEP 1 — READ SHAPEFILE
# ─────────────────────────────────────────────────────────────
def read_shapefile():
    import geopandas as gpd

    print("\n[Step 1/3] Reading Natural Earth admin-1 states/provinces Shapefile...")

    if not os.path.exists(SHAPEFILE):
        print(f"  ERROR: {SHAPEFILE} not found.")
        print(f"  Make sure all four Shapefile files are in your seeds folder:")
        print(f"  .shp, .shx, .dbf, .prj")
        sys.exit(1)

    gdf = gpd.read_file(SHAPEFILE)
    print(f"  Read {len(gdf):,} rows from shapefile.")
    print(f"  Total columns in file: {len(gdf.columns)}")

    # Filter for featurecla = "Admin-1 states provinces"
    gdf = gdf[gdf['featurecla'] == "Admin-1 states provinces"]
    print(f"  Filtered to {len(gdf):,} admin-1 states/provinces.")

    return gdf


# ─────────────────────────────────────────────────────────────
# STEP 2 — MAP ROWS TO dim_state
# ─────────────────────────────────────────────────────────────
def map_row(row, country_iso3_list):
    """
    Translates one Natural Earth GeoDataFrame row into a tuple
    matching the exact column order for dim_state insertion.

    Rules:
    1. adm1_code must exist, otherwise skip the row
    2. state_name uses 'name' or falls back to 'admin'
    3. state_type uses 'type_en'
    4. country_iso3 is mapped via iso3_mapping
       or set to None if mapping results in unknown
    5. Geometry must exist and not be empty
    """
    import pandas as pd

    def clean_str(val):
        s = str(val).strip() if val is not None else ""
        return s if s and s.lower() not in ("nan", "none", "") else None

    adm1_code   = clean_str(row.get("adm1_code"))
    iso_3166_2  = clean_str(row.get("iso_3166_2"))
    state_name  = clean_str(row.get("name")) or clean_str(row.get("admin"))
    state_type  = clean_str(row.get("type_en"))
    country_iso3 = clean_str(row.get("adm0_a3"))
    country_iso2 = clean_str(row.get("iso_a2"))
    country_name = clean_str(row.get("admin"))
    centroid_lon = row.get("longitude")
    centroid_lat = row.get("latitude")
    geometry = row.get("geometry")

    if not adm1_code or not state_name or geometry is None or geometry.is_empty:
        return None

    centroid = geometry.centroid

    if country_iso3:
        country_iso3 = iso3_mapping.get(country_iso3, country_iso3)
        # Only keep mapped code if it exists in dim_country
        if country_iso3 not in country_iso3_list:
            country_iso3 = None
    else:
        country_iso3 = None

    return (
        adm1_code,
        iso_3166_2,
        state_name,
        state_type,
        country_iso3,
        country_iso2,
        country_name,
        centroid_lon,
        centroid_lat,
        geometry.wkt
    )


# STEP 2.5 — FIND ISO3 MISMATCHES
# ─────────────────────────────────────────────────────────────
def find_missing_iso3(gdf, conn):
    """
    Returns a set of all unique country_iso3 codes in the states
    GeoDataFrame that do not exist in dim_country.iso3.
    """
    cursor = conn.cursor()

    # Get all existing iso3 in dim_country
    cursor.execute("SELECT iso3 FROM dim_country")
    existing_iso3 = {row[0] for row in cursor.fetchall() if row[0]}

    # Get all iso3 from shapefile (adm0_a3 column)
    shapefile_iso3 = set(gdf['adm0_a3'].dropna().unique())

    # Find which ones are missing
    missing = shapefile_iso3 - existing_iso3

    cursor.close()
    return missing

# ─────────────────────────────────────────────────────────────
# STEP 3 — INSERT INTO dim_state
# ─────────────────────────────────────────────────────────────
def load_states(rows, conn):
    if not rows:
        print("  No rows to insert.")
        return 0

    cursor = conn.cursor()
    try:
        execute_values(
            cursor,
            """
            INSERT INTO dim_state (
                adm1_code,
                iso_3166_2,
                state_name,
                state_type,
                country_iso3,
                country_iso2,
                country_name,
                centroid_lon,
                centroid_lat,
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
        print(f"  ERROR inserting states: {e}")
        return 0
    finally:
        cursor.close()


# ─────────────────────────────────────────────────────────────
# VERIFICATION
# ─────────────────────────────────────────────────────────────
def verify(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM dim_state")
    total = cursor.fetchone()[0]
    print(f"  dim_state total rows: {total:,}")

    cursor.execute("SELECT COUNT(*) FROM dim_state WHERE boundary IS NOT NULL")
    with_boundary = cursor.fetchone()[0]
    print(f"  Rows with boundary: {with_boundary:,}")

    cursor.close()

    # Test ST_Contains with Lagos coordinates
    # This confirms PostGIS territory detection is working
    print(f"\n  Testing ST_Contains with Lagos coordinates (3.50, 6.45)...")
    cursor = conn.cursor()
    cursor.execute("""
    SELECT *
    FROM dim_state
        WHERE ST_Intersects(
        boundary,
        ST_Buffer(ST_SetSRID(ST_Point(3.50, 6.45), 4326), 0.002)
    )
    """)
    result = cursor.fetchall()
    cursor.close()

    if result:
        print(f"  Result: {result}")
        print(f"  ST_Contains is working correctly.")
    else:
        print(f"  WARNING: No state found for Lagos coordinates.")
        print(f"  This may indicate a geometry issue.")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    print("="*60)
    print("  Transit Radar 411 — Geodata States Loader")
    print("="*60)

    gdf = read_shapefile()
    conn = get_connection()

    # Get all iso3 codes from dim_country
    cursor = conn.cursor()
    cursor.execute("SELECT iso3 FROM dim_country")
    dim_country_iso3 = [row[0] for row in cursor.fetchall() if row[0]]
    cursor.close()

    print(f"\n[Step 2/3] Mapping all rows...")
    rows = []
    skipped = 0
    for _, row in gdf.iterrows():
        mapped = map_row(row, dim_country_iso3)
        if mapped:
            rows.append(mapped)
        else:
            skipped += 1

    print(f"  Mapped   {len(rows):,} rows.")
    print(f"  Skipped  {skipped:,} rows (invalid or missing geometry).")

    # Extract all iso3 codes from the tuples
    tuple_iso3 = [row[4] for row in rows if row[4]]  # 3rd index is country_iso3

    # Check which iso3 in the tuples doesn't exist in dim_country
    missing_iso3 = set(tuple_iso3) - set(dim_country_iso3)
    print("\nISO3 in tuples but missing in dim_country:")
    print(sorted(missing_iso3))

    print(f"\n[Step 3/3] Inserting into dim_state...")
    #inserted = load_states(rows, conn)
    #print(f"  Inserted {inserted:,} rows.")

    verify(conn)
    conn.close()
    print("\nload_geodata_states.py complete. dim_state is ready.")


if __name__ == "__main__":
    main()