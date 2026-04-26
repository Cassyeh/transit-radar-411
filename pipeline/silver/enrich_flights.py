"""
pipeline/silver/enrich_flights.py

PURPOSE
-------
The Silver layer enrichment job. Reads daily_flight_events
where origin_iata or destination_iata is NULL and fills them
in using the OpenSky flights endpoint.

This transforms Bronze raw data into Silver enriched data —
the same medallion architecture pattern used in enterprise
data platforms.

BRONZE → SILVER TRANSFORMATION
--------------------------------
Bronze (raw, written by ingestor):
    icao24 = 'aa8c39'
    event_type = 'DEPARTED'
    origin_iata = NULL          ← empty
    destination_iata = NULL     ← empty

Silver (enriched, written by this script):
    icao24 = 'aa8c39'
    event_type = 'DEPARTED'
    origin_iata = 'LOS'         ← filled in
    destination_iata = 'LHR'   ← filled in (if flight landed)

HOW IT WORKS
------------
1. Query daily_flight_events for all unique icao24 values
   where origin_iata OR destination_iata is NULL today

2. For each unique icao24 make ONE call to the OpenSky
   flights endpoint — one call covers all events for that
   aircraft today regardless of how many events exist

3. The endpoint returns estDepartureAirport (ICAO 4-letter)
   and estArrivalAirport (ICAO 4-letter, NULL if still flying)

4. Convert ICAO codes to IATA codes via dim_airport lookup

5. UPDATE all events for that icao24 today with the
   resolved origin_iata and destination_iata

CREDIT CONSERVATION
-------------------
OpenSky gives 4,000 API credits per day. This script makes
one call per unique icao24 with unenriched events. In a
typical day with 100-300 unique aircraft in your bounding
box, that is 100-300 credits — well within the daily limit.

A 1-second sleep between calls prevents rate limit errors.

WHEN IT RUNS
------------
Triggered by Airflow twice per day:
    12:00 UTC — catches morning departures
    23:50 UTC — catches afternoon flights before EOD rollup

Can also be run manually:
    python pipeline/silver/enrich_flights.py

HOW TO RUN
----------
    docker compose up -d
    python pipeline/silver/enrich_flights.py
"""

import os
import sys
import time
import logging
import requests
import psycopg2
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
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

OPENSKY_CLIENT_ID     = os.getenv("OPENSKY_CLIENT_ID")
OPENSKY_CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET")

OPENSKY_TOKEN_URL  = (
    "https://auth.opensky-network.org/auth/realms/"
    "opensky-network/protocol/openid-connect/token"
)
OPENSKY_FLIGHTS_URL = "https://opensky-network.org/api/flights/aircraft"

# Sleep between API calls to conserve credits
API_SLEEP_SECONDS = 3


# ─────────────────────────────────────────────────────────────
# DATABASE CONNECTION
# ─────────────────────────────────────────────────────────────
def get_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        log.info("  Connected to PostgreSQL successfully.")
        return conn
    except Exception as e:
        log.error(f"  ERROR: Could not connect to PostgreSQL: {e}")
        send_email(
            subject="Transit Radar 411 — Silver layer connection failed",
            body=f"enrich_flights.py could not connect.\n\nError: {e}"
        )
        sys.exit(1)


# ─────────────────────────────────────────────────────────────
# REUSABLE: AIRPORT ICAO → IATA LOOKUP
# ─────────────────────────────────────────────────────────────
def build_airport_lookup(conn) -> dict:
    """
    Builds ICAO -> IATA airport code mapping from dim_airport.
    Same function used across all scripts — reusable pattern.

    Example: {"DNMM": "LOS", "EGLL": "LHR", "OMDB": "DXB"}
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
    lookup = {row[0]: row[1] for row in rows}
    log.info(f"  Airport lookup: {len(lookup):,} ICAO -> IATA mappings.")
    return lookup


# ─────────────────────────────────────────────────────────────
# OPENSKY AUTHENTICATION
# ─────────────────────────────────────────────────────────────
def get_opensky_token() -> str | None:
    """
    Gets a fresh OAuth2 Bearer token from OpenSky.
    Tokens expire after 30 minutes — this script gets one
    fresh token at startup and uses it for all API calls.

    Returns the token string or None if authentication fails.
    The script continues without enrichment if auth fails
    rather than crashing — enrichment is important but not
    critical for the pipeline to function.
    """
    if not OPENSKY_CLIENT_ID or not OPENSKY_CLIENT_SECRET:
        log.warning(
            "  OPENSKY_CLIENT_ID or OPENSKY_CLIENT_SECRET not set in .env."
            " Skipping enrichment."
        )
        return None

    try:
        response = requests.post(
            OPENSKY_TOKEN_URL,
            data={
                "grant_type":    "client_credentials",
                "client_id":     OPENSKY_CLIENT_ID,
                "client_secret": OPENSKY_CLIENT_SECRET
            },
            timeout=30
        )
        response.raise_for_status()
        token = response.json().get("access_token")
        log.info("  OpenSky token obtained successfully.")
        return token
    except Exception as e:
        log.error(f"  OpenSky authentication failed: {e}")
        return None


# ─────────────────────────────────────────────────────────────
# FETCH UNENRICHED AIRCRAFT
# ─────────────────────────────────────────────────────────────
def fetch_unenriched_aircraft(conn, today) -> list[str]:
    """
    Returns a list of unique icao24 values that have at least
    one DEPARTED or LANDED event today where origin_iata OR
    destination_iata is NULL.
 
    WHY FILTER TO DEPARTED AND LANDED ONLY?
    ----------------------------------------
    CLIMBING, CRUISING and DESCENDING events are mid-flight —
    they do not need origin or destination airport codes.
    Only DEPARTED and LANDED events meaningfully use these
    columns. Without this filter the count can reach 1,000+
    aircraft which would exhaust the 4,000 daily OpenSky
    credit limit in a single enrichment run.
 
    With this filter the count typically drops to 100-300
    aircraft — the actual number of flights that departed
    or landed in your bounding box today.
 
    One API call per icao24 covers all events for that
    aircraft for the day regardless of how many events exist.
 
    Only looks at today's events — daily_flight_events is
    cleared every night by the Airflow EOD DAG.
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT icao24
        FROM daily_flight_events
        WHERE event_date = %s
        AND   event_type IN ('DEPARTED', 'LANDED')
        AND (
            origin_iata      IS NULL
            OR destination_iata IS NULL
        )
        ORDER BY icao24
    """, (today,))
    rows   = cursor.fetchall()
    cursor.close()
    result = [row[0] for row in rows]
    log.info(f"  Aircraft needing enrichment: {len(result):,}")
    log.info(f"  (Filtered to DEPARTED/LANDED events only — conserves API credits)")
    return result


# ─────────────────────────────────────────────────────────────
# CALL OPENSKY FLIGHTS ENDPOINT
# ─────────────────────────────────────────────────────────────
def fetch_flight_data(
    icao24: str,
    token:  str,
    today:  object
) -> dict | None:
    """
    Calls the OpenSky flights/aircraft endpoint for one icao24.

    Returns the most recent flight entry for that aircraft
    today, or None if no data is available.

    The endpoint returns a list of flights — we take the last
    one (most recent) since an aircraft could technically make
    multiple flights in one day.

    Response fields we use:
        estDepartureAirport — ICAO code e.g. 'DNMM'
        estArrivalAirport   — ICAO code e.g. 'EGLL', NULL if flying

    The time window is today 00:00:00 UTC to now UTC — covers
    all flights that started today.
    """
    # Build time window: today 00:00 UTC to now UTC
    start_of_day = int(
        datetime(today.year, today.month, today.day,
                 tzinfo=timezone.utc).timestamp()
    )
    now_unix = int(datetime.now(timezone.utc).timestamp())

    for attempt in range(1, 4):
        try:
            response = requests.get(
                OPENSKY_FLIGHTS_URL,
                params={
                    "icao24": icao24,
                    "begin":  start_of_day,
                    "end":    now_unix
                },
                headers={"Authorization": f"Bearer {token}"},
                timeout=30
            )

            # 404 means no flight data for this aircraft today
            # This is normal — not every aircraft has flight records
            if response.status_code == 404:
                if response.status_code == 404:
                    return None

                if response.status_code == 429:
                    wait = 60 * attempt  # 60s, 120s, 180s
                    log.warning(
                        f"  Rate limit hit for {icao24}. "
                        f"Waiting {wait}s before retry {attempt}/3..."
                    )
                    time.sleep(wait)
                    continue

                response.raise_for_status()
                flights = response.json()
                # Return the most recent flight entry
                return flights[-1] if flights else None
        
        except requests.exceptions.Timeout:
            log.warning(f"  Timeout fetching {icao24}")
            return None
        except requests.exceptions.RequestException as e:
            log.warning(f"  API error for {icao24}: {e}")
            return None

    log.warning(f"  Giving up on {icao24} after 3 rate limit retries.")
    return None



# ─────────────────────────────────────────────────────────────
# UPDATE DAILY FLIGHT EVENTS
# ─────────────────────────────────────────────────────────────
def update_flight_events(
    conn,
    icao24:          str,
    origin_iata:     str | None,
    destination_iata: str | None,
    today:           object
) -> int:
    """
    Updates all events for this icao24 today with the
    resolved origin_iata and destination_iata.

    Only updates columns that have a non-NULL value to set —
    we never overwrite an existing value with NULL.

    Returns the number of rows updated.
    """
    if not origin_iata and not destination_iata:
        return 0

    cursor = conn.cursor()
    try:
        if origin_iata and destination_iata:
            # Both available — update both columns
            cursor.execute("""
                UPDATE daily_flight_events
                SET origin_iata      = %s,
                    destination_iata = %s
                WHERE icao24     = %s
                AND   event_date = %s
                AND (
                    origin_iata      IS NULL
                    OR destination_iata IS NULL
                )
            """, (origin_iata, destination_iata, icao24, today))

        elif origin_iata:
            # Only departure airport known — flight still airborne
            cursor.execute("""
                UPDATE daily_flight_events
                SET origin_iata = %s
                WHERE icao24     = %s
                AND   event_date = %s
                AND   origin_iata IS NULL
            """, (origin_iata, icao24, today))

        elif destination_iata:
            # Only arrival airport known — unusual but handle it
            cursor.execute("""
                UPDATE daily_flight_events
                SET destination_iata = %s
                WHERE icao24          = %s
                AND   event_date      = %s
                AND   destination_iata IS NULL
            """, (destination_iata, icao24, today))

        rows_updated = cursor.rowcount
        conn.commit()
        return rows_updated

    except Exception as e:
        conn.rollback()
        log.error(f"  update_flight_events error for {icao24}: {e}")
        return 0
    finally:
        cursor.close()


# ─────────────────────────────────────────────────────────────
# ENRICH ONE AIRCRAFT
# ─────────────────────────────────────────────────────────────
def enrich_aircraft(
    icao24:         str,
    token:          str,
    conn,
    airport_lookup: dict,
    today:          object
) -> dict:
    """
    Orchestrates the full enrichment for one aircraft:
        1. Call OpenSky flights endpoint
        2. Extract estDepartureAirport and estArrivalAirport
        3. Convert ICAO codes to IATA via airport_lookup
        4. Update daily_flight_events

    Returns a summary dict for reporting.
    """
    flight_data = fetch_flight_data(icao24, token, today)

    if not flight_data:
        return {
            "icao24":   icao24,
            "status":   "no_data",
            "updated":  0
        }

    # Extract ICAO airport codes from response
    dep_icao = flight_data.get("estDepartureAirport")
    arr_icao = flight_data.get("estArrivalAirport")

    # Convert ICAO to IATA using dim_airport lookup
    origin_iata      = airport_lookup.get(dep_icao) if dep_icao else None
    destination_iata = airport_lookup.get(arr_icao) if arr_icao else None

    # Update the database
    rows_updated = update_flight_events(
        conn=conn,
        icao24=icao24,
        origin_iata=origin_iata,
        destination_iata=destination_iata,
        today=today
    )

    log.info(
        f"  {icao24} | "
        f"dep={dep_icao or 'unknown':>4} → {origin_iata or 'N/A':<4} | "
        f"arr={arr_icao or 'unknown':>4} → {destination_iata or 'N/A':<4} | "
        f"rows updated: {rows_updated}"
    )

    return {
        "icao24":          icao24,
        "status":          "enriched",
        "origin_iata":     origin_iata,
        "destination_iata": destination_iata,
        "updated":         rows_updated
    }


# ─────────────────────────────────────────────────────────────
# VERIFY ENRICHMENT RESULTS
# ─────────────────────────────────────────────────────────────
def verify_enrichment(conn, today) -> dict:
    """
    After enrichment runs, queries daily_flight_events to show
    how many events now have origin_iata and destination_iata
    filled in vs still NULL.

    Returns counts for the completion email and logs.
    """
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            COUNT(*)                                           AS total,
            COUNT(origin_iata)                                AS has_origin,
            COUNT(destination_iata)                           AS has_destination,
            COUNT(*) - COUNT(origin_iata)                     AS missing_origin,
            COUNT(*) - COUNT(destination_iata)                AS missing_destination
        FROM daily_flight_events
        WHERE event_date = %s
    """, (today,))

    row    = cursor.fetchone()
    cursor.close()

    return {
        "total":               row[0],
        "has_origin":          row[1],
        "has_destination":     row[2],
        "missing_origin":      row[3],
        "missing_destination": row[4]
    }


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("  Transit Radar 411 — Silver Layer Enrichment")
    log.info("=" * 60)

    today = datetime.now(timezone.utc).date()
    log.info(f"  Enriching events for: {today}")

    # Connect to database and build airport lookup
    conn           = get_connection()
    airport_lookup = build_airport_lookup(conn)

    # Get OpenSky OAuth2 token
    token = get_opensky_token()
    if not token:
        log.error("  Cannot enrich without OpenSky token. Exiting.")
        conn.close()
        sys.exit(1)

    # Find aircraft that need enrichment
    unenriched = fetch_unenriched_aircraft(conn, today)

    if not unenriched:
        log.info("  All events already enriched. Nothing to do.")
        conn.close()
        return

    log.info(f"\n  Starting enrichment for {len(unenriched)} aircraft...")

    # Enrich each aircraft one at a time
    # One API call per aircraft — conserves OpenSky credits
    total_enriched       = 0
    total_no_data        = 0
    total_rows_updated   = 0

    for i, icao24 in enumerate(unenriched, start=1):
        log.info(f"  [{i}/{len(unenriched)}] {icao24}")

        result = enrich_aircraft(
            icao24=icao24,
            token=token,
            conn=conn,
            airport_lookup=airport_lookup,
            today=today
        )

        if result["status"] == "enriched":
            total_enriched     += 1
            total_rows_updated += result["updated"]
        else:
            total_no_data += 1

        # Sleep between API calls to respect rate limits
        # and conserve daily credit allowance
        time.sleep(API_SLEEP_SECONDS)

    # Verify results
    verification = verify_enrichment(conn, today)

    log.info("\n" + "=" * 60)
    log.info("  Enrichment Summary")
    log.info("=" * 60)
    log.info(f"  Aircraft processed   : {len(unenriched):,}")
    log.info(f"  Aircraft enriched    : {total_enriched:,}")
    log.info(f"  Aircraft no data     : {total_no_data:,}")
    log.info(f"  Rows updated         : {total_rows_updated:,}")
    log.info(f"  Events with origin   : {verification['has_origin']:,} / {verification['total']:,}")
    log.info(f"  Events with dest     : {verification['has_destination']:,} / {verification['total']:,}")
    log.info(f"  Still missing origin : {verification['missing_origin']:,}")
    log.info(f"  Still missing dest   : {verification['missing_destination']:,}")

    conn.close()
    log.info("\nSilver enrichment complete.")


if __name__ == "__main__":
    main()