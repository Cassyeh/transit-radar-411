"""
seeds/load_historical_territories.py

WHAT THIS SCRIPT DOES
---------------------
Reads DEPARTED and LANDED event pairs from hist_flight_events
and detects which countries each flight passed over, writing
the results into hist_territory_crossings.

This script runs AFTER load_historical_states.py completes.

YEAR FILTER
-----------
To avoid loading all years at once and running out of memory,
you can specify which year to process using the --year argument.

    python seeds/load_historical_territories.py --year 2020
    python seeds/load_historical_territories.py --year 2021
    python seeds/load_historical_territories.py --year 2022

If no year is specified, all years are processed in sequence.

HOW TERRITORY DETECTION WORKS
------------------------------
For each flight we have two coordinates:
    (latitude_1, longitude_1) = position at departure
    (latitude_2, longitude_2) = position at arrival

We sample ROUTE_SAMPLE_POINTS evenly spaced points along the
straight line between them. For each point we query PostGIS
to find which country it falls inside. Consecutive identical
countries are deduplicated into one crossing row.

ROUTE_SAMPLE_POINTS = 10
-------------------------
10 points gives a good balance between accuracy and speed.
More points = more PostGIS queries per flight = slower.
10 points catches most territory crossings for typical
intercontinental routes without excessive memory pressure.

TIMESTAMP ESTIMATION
--------------------
Entry and exit timestamps are estimated proportionally —
how far along the route each territory crossing occurs
multiplied by the total flight duration.

HOW TO RUN
----------
    docker compose up -d

    # Process one year at a time (recommended)
    python seeds/load_historical_territories.py --year 2019
    python seeds/load_historical_territories.py --year 2020
    python seeds/load_historical_territories.py --year 2021
    python seeds/load_historical_territories.py --year 2022

    # Or process all years at once (requires more memory)
    python seeds/load_historical_territories.py
"""

import os
import sys
import logging
import argparse
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

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

# Number of points sampled along each flight route.
# 10 points means we check one position every ~10% of the route.
# Reduced from 20 to lower memory pressure and PostGIS query load.
# Each point = one ST_Contains query against dim_country.
ROUTE_SAMPLE_POINTS = 10

# Number of flights to fetch and process per database batch.
# Keeping this at 1000 means we never hold more than
# 1000 flights × 10 points = 10,000 PostGIS queries in memory
# at one time.
BATCH_SIZE = 1_000

# Send a progress email every this many flights processed
EMAIL_EVERY_N = 50_000

SOURCE = "opensky_historical"


# ─────────────────────────────────────────────────────────────
# ARGUMENT PARSER — year filter
# ─────────────────────────────────────────────────────────────
def parse_args():
    """
    Parses command line arguments.

    --year YYYY  : process only flights from this year
                   e.g. --year 2020
                   If not provided, all years are processed.

    Usage examples:
        python seeds/load_historical_territories.py --year 2020
        python seeds/load_historical_territories.py
    """
    parser = argparse.ArgumentParser(
        description="Load historical territory crossings into hist_territory_crossings."
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Year to process (e.g. 2020). If omitted, all years are processed."
    )
    return parser.parse_args()


# ─────────────────────────────────────────────────────────────
# DATABASE CONNECTION
# ─────────────────────────────────────────────────────────────
def get_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        log.info("  Connected to PostgreSQL successfully.")
        return conn
    except Exception as e:
        log.error(f"  ERROR: {e}")
        send_email(
            subject="Transit Radar 411 — Territory loader connection failed",
            body=f"Error: {e}"
        )
        sys.exit(1)


# ─────────────────────────────────────────────────────────────
# REUSABLE: SAMPLE POINTS ALONG A ROUTE
# ─────────────────────────────────────────────────────────────
def sample_route_points(
    lat1: float, lon1: float,
    lat2: float, lon2: float,
    n: int = ROUTE_SAMPLE_POINTS
) -> list[tuple]:
    """
    Generates n evenly spaced coordinate points along the
    straight line between (lat1, lon1) and (lat2, lon2).

    Returns a list of (latitude, longitude, fraction) tuples.
    fraction goes from 0.0 at departure to 1.0 at arrival.

    Example with n=10 for Lagos → London:
        (6.45,  3.39, 0.0)  ← Lagos (departure)
        (11.00, 1.95, 0.1)  ← northern Nigeria
        (15.55, 0.51, 0.2)  ← Niger
        ...
        (51.50,-0.12, 1.0)  ← London (arrival)

    This is linear interpolation — not a true great circle arc
    but accurate enough for 110m-scale country boundaries.

    Reusable by any script needing route point sampling.
    """
    return [
        (
            lat1 + (i / n) * (lat2 - lat1),  # latitude at this fraction
            lon1 + (i / n) * (lon2 - lon1),  # longitude at this fraction
            i / n                              # fraction along route
        )
        for i in range(n + 1)
    ]


# ─────────────────────────────────────────────────────────────
# REUSABLE: DETECT COUNTRIES ALONG A ROUTE
# ─────────────────────────────────────────────────────────────
def detect_countries_along_route(
    conn,
    lat1: float, lon1: float,
    lat2: float, lon2: float
) -> list[dict]:
    """
    Samples points along a flight route and identifies which
    countries each point falls inside using PostGIS ST_Contains.

    For each sampled point queries dim_country:
        SELECT country_id, country_name, iso2
        FROM dim_country
        WHERE ST_Contains(boundary, ST_SetSRID(ST_Point(lon, lat), 4326))

    Points over open ocean or uninhabited airspace return None
    and are skipped — no territory crossing recorded for them.

    Consecutive identical countries are deduplicated — if three
    consecutive points are all inside Nigeria, only one Nigeria
    crossing is recorded with the full entry-to-exit span.

    Returns a deduplicated list of country dicts with
    entry_fraction and exit_fraction for timestamp estimation.

    Example output for Lagos → London:
        [
            {"country_id": 1, "country_name": "Nigeria",
             "iso2": "NG", "entry_fraction": 0.0, "exit_fraction": 0.2},
            {"country_id": 2, "country_name": "Niger",
             "iso2": "NE", "entry_fraction": 0.2, "exit_fraction": 0.4},
            {"country_id": 3, "country_name": "Algeria",
             "iso2": "DZ", "entry_fraction": 0.4, "exit_fraction": 0.6},
            {"country_id": 4, "country_name": "France",
             "iso2": "FR", "entry_fraction": 0.8, "exit_fraction": 0.9},
            {"country_id": 5, "country_name": "United Kingdom",
             "iso2": "GB", "entry_fraction": 0.9, "exit_fraction": 1.0}
        ]

    Note: Mediterranean points returned None so Algeria exits
    at 0.6 and France starts at 0.8 — the gap is open ocean.

    Reusable by any script needing country detection along
    a flight path.
    """
    cursor = conn.cursor()
    points = sample_route_points(lat1, lon1, lat2, lon2)

    # Query PostGIS for each sample point
    point_countries = []
    for lat, lon, fraction in points:
        cursor.execute("""
            SELECT country_id, country_name, iso2
            FROM dim_country
            WHERE ST_Contains(
                boundary,
                ST_SetSRID(ST_Point(%s, %s), 4326)
            )
            LIMIT 1
        """, (lon, lat))   # NOTE: ST_Point takes longitude first, then latitude
        result = cursor.fetchone()
        if result:
            point_countries.append({
                "country_id":   result[0],
                "country_name": result[1],
                "iso2":         result[2],
                "fraction":     fraction
            })

    cursor.close()

    if not point_countries:
        return []

    # Deduplicate consecutive identical countries
    # and compute entry/exit fractions for each crossing
    deduplicated = []
    current      = point_countries[0]
    entry_frac   = current["fraction"]

    for i in range(1, len(point_countries)):
        nxt = point_countries[i]
        if nxt["country_id"] != current["country_id"]:
            # Country changed — close current and open new
            deduplicated.append({
                **current,
                "entry_fraction": entry_frac,
                "exit_fraction":  nxt["fraction"]
            })
            current    = nxt
            entry_frac = nxt["fraction"]

    # Add the final country — exit fraction is always 1.0
    deduplicated.append({
        **current,
        "entry_fraction": entry_frac,
        "exit_fraction":  1.0
    })

    return deduplicated


# ─────────────────────────────────────────────────────────────
# BUILD CROSSING ROWS FOR ONE FLIGHT
# ─────────────────────────────────────────────────────────────
def build_crossing_rows(
    countries:         list[dict],
    departed_event_id: int,
    landed_event_id:   int,
    icao24:            str,
    callsign:          str | None,
    firstseen:         datetime,
    lastseen:          datetime,
    now:               datetime,
    batch_date
) -> list[tuple]:
    """
    Converts a list of country dicts (from detect_countries_along_route)
    into database row tuples ready for bulk insert into
    hist_territory_crossings.

    For each country in the list:

    CROSSING ROLE assignment:
        START — first country in the list (where flight departed from)
        END   — last country in the list  (where flight landed)
        ROUTE — every country in between

    TIMESTAMP ESTIMATION:
        entered_at = firstseen + (entry_fraction × total_duration)
        exited_at  = firstseen + (exit_fraction  × total_duration)
        duration_minutes = (exit_fraction - entry_fraction) × total_duration / 60

    All historical flights are COMPLETED — they have both
    a departure and an arrival so flight_status is always
    set to COMPLETED here.

    Returns a list of tuples in the exact column order
    expected by the INSERT statement in insert_crossings().
    """
    if not countries:
        return []

    total_seconds = (lastseen - firstseen).total_seconds()
    rows          = []
    total         = len(countries)

    for idx, country in enumerate(countries):

        # Assign crossing role based on position in list
        if idx == 0:
            crossing_role   = "START"
            flight_event_id = departed_event_id
        elif idx == total - 1:
            crossing_role   = "END"
            flight_event_id = landed_event_id
        else:
            crossing_role   = "ROUTE"
            flight_event_id = departed_event_id

        # Estimate timestamps proportionally
        if total_seconds > 0:
            entered_at = firstseen + timedelta(
                seconds=country["entry_fraction"] * total_seconds
            )
            exited_at = firstseen + timedelta(
                seconds=country["exit_fraction"] * total_seconds
            )
            duration_minutes = round(
                (country["exit_fraction"] - country["entry_fraction"])
                * total_seconds / 60, 2
            )
        else:
            entered_at       = firstseen
            exited_at        = lastseen
            duration_minutes = None

        rows.append((
            flight_event_id,          # 1  flight_event_id
            icao24,                   # 2  icao24
            callsign,                 # 3  callsign
            "COUNTRY",                # 4  territory_type
            country["country_id"],    # 5  territory_id
            country["country_name"],  # 6  territory_name
            crossing_role,            # 7  crossing_role
            "COMPLETED",              # 8  flight_status
            entered_at,               # 9  entered_at
            entered_at.date(),        # 10 entered_date
            exited_at,                # 11 exited_at
            exited_at.date(),         # 12 exited_date
            duration_minutes,         # 13 duration_minutes
            SOURCE,                   # 14 source
            now,                      # 15 inserted_at
            batch_date,               # 16 batch_date
            now,                      # 17 updated_at
            now,                      # 18 created_at
        ))

    return rows


# ─────────────────────────────────────────────────────────────
# BULK INSERT TERRITORY CROSSINGS
# ─────────────────────────────────────────────────────────────
def insert_crossings(rows: list[tuple], conn) -> int:
    """
    Bulk-inserts territory crossing rows into
    hist_territory_crossings using execute_values.

    ON CONFLICT DO NOTHING makes this idempotent — running
    the script twice for the same year will not create
    duplicate rows.

    Returns the number of rows inserted.
    """
    if not rows:
        return 0

    cursor = conn.cursor()
    try:
        execute_values(
            cursor,
            """
            INSERT INTO hist_territory_crossings (
                flight_event_id, icao24, callsign,
                territory_type, territory_id, territory_name,
                crossing_role, flight_status,
                entered_at, entered_date,
                exited_at, exited_date,
                duration_minutes, source,
                inserted_at, batch_date, updated_at, created_at
            ) VALUES %s
            ON CONFLICT DO NOTHING
            """,
            rows
        )
        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        log.error(f"  insert_crossings error: {e}")
        return 0
    finally:
        cursor.close()


# ─────────────────────────────────────────────────────────────
# COUNT FLIGHTS FOR A GIVEN YEAR (or all years)
# ─────────────────────────────────────────────────────────────
def count_flights(conn, year: int | None) -> int:
    """
    Counts how many DEPARTED/LANDED pairs exist in
    hist_flight_events for the given year.

    If year is None, counts across all years.
    Used at startup to show progress percentage.
    """
    cursor = conn.cursor()

    if year:
        cursor.execute("""
            SELECT COUNT(*)
            FROM hist_flight_events d
            JOIN hist_flight_events l
                ON  l.icao24     = d.icao24
                AND l.event_type = 'LANDED'
                AND l.source     = d.source
                AND l.event_date = d.event_date
            WHERE d.event_type    = 'DEPARTED'
            AND   d.source        = %s
            AND   d.latitude      IS NOT NULL
            AND   d.longitude     IS NOT NULL
            AND   l.latitude      IS NOT NULL
            AND   l.longitude     IS NOT NULL
            AND   EXTRACT(YEAR FROM d.event_date) = %s
        """, (SOURCE, year))
    else:
        cursor.execute("""
            SELECT COUNT(*)
            FROM hist_flight_events d
            JOIN hist_flight_events l
                ON  l.icao24     = d.icao24
                AND l.event_type = 'LANDED'
                AND l.source     = d.source
                AND l.event_date = d.event_date
            WHERE d.event_type = 'DEPARTED'
            AND   d.source     = %s
            AND   d.latitude   IS NOT NULL
            AND   d.longitude  IS NOT NULL
            AND   l.latitude   IS NOT NULL
            AND   l.longitude  IS NOT NULL
        """, (SOURCE,))

    total = cursor.fetchone()[0]
    cursor.close()
    return total


# ─────────────────────────────────────────────────────────────
# FETCH ONE BATCH OF FLIGHT PAIRS
# ─────────────────────────────────────────────────────────────
def fetch_flight_batch(
    conn,
    year: int | None,
    limit: int,
    offset: int
) -> list[tuple]:
    """
    Fetches a batch of DEPARTED/LANDED event pairs from
    hist_flight_events — one pair per flight.

    If year is provided, only returns flights where the
    DEPARTED event falls within that calendar year.

    JOIN between DEPARTED and LANDED uses icao24 + event_date
    + source to match the two events for the same flight.

    Returns a list of tuples — one tuple per flight containing:
        departed_event_id, icao24, callsign,
        firstseen, lat1, lon1,
        landed_event_id, lastseen,
        lat2, lon2
    """
    cursor = conn.cursor()

    if year:
        cursor.execute("""
            SELECT
                d.event_id,
                d.icao24,
                d.callsign,
                d.event_timestamp,
                d.latitude,
                d.longitude,
                l.event_id,
                l.event_timestamp,
                l.latitude,
                l.longitude
            FROM hist_flight_events d
            JOIN hist_flight_events l
                ON  l.icao24     = d.icao24
                AND l.event_type = 'LANDED'
                AND l.source     = d.source
                AND l.event_date = d.event_date
            WHERE d.event_type    = 'DEPARTED'
            AND   d.source        = %s
            AND   d.latitude      IS NOT NULL
            AND   d.longitude     IS NOT NULL
            AND   l.latitude      IS NOT NULL
            AND   l.longitude     IS NOT NULL
            AND   EXTRACT(YEAR FROM d.event_date) = %s
            ORDER BY d.event_id
            LIMIT %s OFFSET %s
        """, (SOURCE, year, limit, offset))
    else:
        cursor.execute("""
            SELECT
                d.event_id,
                d.icao24,
                d.callsign,
                d.event_timestamp,
                d.latitude,
                d.longitude,
                l.event_id,
                l.event_timestamp,
                l.latitude,
                l.longitude
            FROM hist_flight_events d
            JOIN hist_flight_events l
                ON  l.icao24     = d.icao24
                AND l.event_type = 'LANDED'
                AND l.source     = d.source
                AND l.event_date = d.event_date
            WHERE d.event_type = 'DEPARTED'
            AND   d.source     = %s
            AND   d.latitude   IS NOT NULL
            AND   d.longitude  IS NOT NULL
            AND   l.latitude   IS NOT NULL
            AND   l.longitude  IS NOT NULL
            ORDER BY d.event_id
            LIMIT %s OFFSET %s
        """, (SOURCE, limit, offset))

    batch = cursor.fetchall()
    cursor.close()
    return batch


# ─────────────────────────────────────────────────────────────
# PROCESS ALL FLIGHTS FOR A GIVEN YEAR (or all years)
# ─────────────────────────────────────────────────────────────
def process_year(conn, year: int | None) -> dict:
    """
    Processes all flights for the given year (or all years
    if year is None) and inserts their territory crossings
    into hist_territory_crossings.

    Fetches flights in batches of BATCH_SIZE to keep memory
    usage flat. For each batch:
        1. Detect countries along each flight route
        2. Build crossing row tuples
        3. Bulk insert into hist_territory_crossings

    Sends a progress email every EMAIL_EVERY_N flights.

    Returns a summary dict with total counts.
    """
    year_label     = str(year) if year else "all years"
    total_flights  = count_flights(conn, year)

    log.info(f"  Flights to process ({year_label}): {total_flights:,}")

    if total_flights == 0:
        log.warning(f"  No flights found for {year_label}.")
        log.warning(f"  Make sure load_historical_states.py ran first.")
        return {"processed": 0, "crossings": 0, "skipped": 0}

    now        = datetime.now(timezone.utc)
    batch_date = now.date()

    total_processed = 0
    total_crossings = 0
    total_skipped   = 0
    offset          = 0

    while True:
        # Fetch next batch of flight pairs
        batch = fetch_flight_batch(conn, year, BATCH_SIZE, offset)

        if not batch:
            break   # No more flights — we are done

        crossing_rows = []

        for row in batch:
            (
                dep_id, icao24, callsign,
                firstseen, lat1, lon1,
                land_id, lastseen,
                lat2, lon2
            ) = row

            try:
                countries = detect_countries_along_route(
                    conn,
                    float(lat1), float(lon1),
                    float(lat2), float(lon2)
                )

                if not countries:
                    # Flight path entirely over ocean or
                    # no matching country boundaries found
                    total_skipped += 1
                    continue

                rows = build_crossing_rows(
                    countries=countries,
                    departed_event_id=dep_id,
                    landed_event_id=land_id,
                    icao24=icao24,
                    callsign=callsign,
                    firstseen=firstseen,
                    lastseen=lastseen,
                    now=now,
                    batch_date=batch_date
                )

                crossing_rows.extend(rows)
                total_processed += 1

            except Exception as e:
                log.warning(f"  Skipping {icao24}: {e}")
                total_skipped += 1

        # Bulk insert all crossing rows for this batch
        inserted        = insert_crossings(crossing_rows, conn)
        total_crossings += inserted
        offset          += BATCH_SIZE

        log.info(
            f"  [{year_label}] "
            f"processed: {total_processed:,}/{total_flights:,} | "
            f"crossings: {total_crossings:,} | "
            f"skipped: {total_skipped:,}"
        )

        # Send progress email every EMAIL_EVERY_N flights
        if total_processed % EMAIL_EVERY_N == 0 and total_processed > 0:
            send_email(
                subject=(
                    f"Transit Radar 411 — Territory progress "
                    f"({year_label}): {total_processed:,}/{total_flights:,}"
                ),
                body=(
                    f"Territory detection progress for {year_label}.\n\n"
                    f"  Flights processed  : {total_processed:,}\n"
                    f"  Flights remaining  : {total_flights - total_processed:,}\n"
                    f"  Crossings inserted : {total_crossings:,}\n"
                    f"  Flights skipped    : {total_skipped:,}"
                )
            )

    return {
        "processed": total_processed,
        "crossings": total_crossings,
        "skipped":   total_skipped
    }


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    args = parse_args()
    year = args.year

    year_label = str(year) if year else "all years"

    log.info("=" * 60)
    log.info("  Transit Radar 411 — Historical Territory Loader")
    log.info("=" * 60)
    log.info(f"  Year filter        : {year_label}")
    log.info(f"  Sample points      : {ROUTE_SAMPLE_POINTS} per flight route")
    log.info(f"  Batch size         : {BATCH_SIZE:,} flights per batch")

    conn = get_connection()

    send_email(
        subject=f"Transit Radar 411 — Territory loading started ({year_label})",
        body=(
            f"Starting territory detection for {year_label}.\n\n"
            f"  Sample points per route : {ROUTE_SAMPLE_POINTS}\n"
            f"  Batch size              : {BATCH_SIZE:,}\n\n"
            f"Progress email every {EMAIL_EVERY_N:,} flights.\n\n"
            f"Run one year at a time to manage memory:\n"
            f"    python seeds/load_historical_territories.py --year 2019\n"
            f"    python seeds/load_historical_territories.py --year 2020\n"
            f"    python seeds/load_historical_territories.py --year 2021\n"
            f"    python seeds/load_historical_territories.py --year 2022"
        )
    )

    summary = process_year(conn, year)

    log.info("\n" + "=" * 60)
    log.info("  Summary")
    log.info("=" * 60)
    log.info(f"  Year               : {year_label}")
    log.info(f"  Flights processed  : {summary['processed']:,}")
    log.info(f"  Flights skipped    : {summary['skipped']:,}")
    log.info(f"  Crossings inserted : {summary['crossings']:,}")

    send_email(
        subject=f"Transit Radar 411 — Territory loading complete ({year_label})",
        body=(
            f"Historical territory detection complete for {year_label}.\n\n"
            f"  Flights processed  : {summary['processed']:,}\n"
            f"  Flights skipped    : {summary['skipped']:,}\n"
            f"  Crossings inserted : {summary['crossings']:,}\n\n"
            + (
                f"Run the next year:\n"
                f"    python seeds/load_historical_territories.py --year {year + 1}\n"
                if year and year < 2022 else
                f"All years complete.\n"
                f"Next step: python seeds/load_bts_delays.py"
            )
        )
    )

    conn.close()

    if year and year < 2022:
        log.info(f"\nDone. Next year:")
        log.info(f"    python seeds/load_historical_territories.py --year {year + 1}")
    else:
        log.info("\nDone. Next: python seeds/load_bts_delays.py")


if __name__ == "__main__":
    main()