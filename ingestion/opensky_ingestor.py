"""
ingestion/opensky_ingestor.py

WHAT THIS FILE IS
-----------------
The real-time ingestion service — the beating heart of the
Transit Radar 411 pipeline. It runs continuously, waking up
every 60 seconds to poll the OpenSky Network API for live
aircraft positions.

ESSENCE
-------
Think of this as the bank teller who sits at the counter all
day. Every 60 seconds a new customer (position ping) walks in.
The teller checks: has anything meaningful changed for this
customer since last time? If yes, record a transaction
(flight event) and update which country they are in
(territory crossing). If nothing changed, do nothing.

WHAT IT WRITES TO
-----------------
1. daily_flight_events      — one row per meaningful state change
                              (DEPARTED, CLIMBING, CRUISING,
                               DESCENDING, LANDED)

2. daily_territory_crossings — one row per country boundary crossed
                               Updated when a new country is entered,
                               closed when the aircraft exits.

3. Kafka topic: flight_events — same event published simultaneously
                                for downstream real-time consumers.

WHY KAFKA AS WELL AS POSTGRES?
-------------------------------
PostgreSQL stores the event so it is queryable and the Airflow
EOD DAG can roll it up at midnight.

Kafka publishes the event so any future downstream service can
react in real-time without hitting the database — a live dashboard,
a delay prediction trigger, a notification service.

Same event, two destinations. This is the dual-write pattern.

STATE CHANGE DETECTION
----------------------
The ingestion service keeps an in-memory dictionary called
aircraft_memory. Key = icao24, Value = last known state.

Every 60 seconds it compares the new state to the stored state:

    on_ground False -> True       = LANDED
    on_ground True  -> False      = DEPARTED
    vertical_rate > 200 fpm       = CLIMBING
    vertical_rate < -200 fpm      = DESCENDING
    vertical_rate between -200/200
    AND altitude > 10,000ft       = CRUISING

If none of these conditions are met, nothing is written.

TERRITORY CROSSING DETECTION
-----------------------------
After every ping the service checks which country the aircraft
is currently over using PostGIS ST_Contains against dim_country.
If the result differs from the last recorded country for that
aircraft, a new row is opened in daily_territory_crossings and
the previous row is closed (exited_at and duration_minutes filled).

HOW TO RUN
----------
    docker compose up -d
    python ingestion/opensky_ingestor.py

Press Ctrl+C to stop gracefully.
"""

import os
import sys
import json
import time
import logging
import psycopg2
import requests
from datetime import datetime, timezone
from dataclasses import dataclass, asdict, field
from typing import Optional
from dotenv import load_dotenv

# Add project root to path for utils imports
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

OPENSKY_API_URL  = "https://opensky-network.org/api/states/all"
KAFKA_BROKER     = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC      = "flight_events"
POLL_INTERVAL_S  = 60       # Seconds between OpenSky API polls

# Bounding box — Africa and Middle East
# Format: min_lat, max_lat, min_lon, max_lon
BOUNDING_BOX = {
    "lamin": -35.0,   # Southern tip of Africa
    "lamax":  37.0,   # North Africa / Mediterranean
    "lomin": -17.0,   # West African Atlantic coast
    "lomax":  60.0,   # Arabian Peninsula east coast
}

# State detection thresholds
CLIMBING_THRESHOLD_FPM   =  200   # vertical_rate above this = CLIMBING
DESCENDING_THRESHOLD_FPM = -200   # vertical_rate below this = DESCENDING
CRUISE_ALT_FT            = 10_000 # altitude above this = potentially CRUISING

# Source identifier for database rows
SOURCE = "opensky"


# ─────────────────────────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────────────────────────
@dataclass
class AircraftState:
    """
    Represents the complete state of one aircraft at one moment.
    Built from one row of the OpenSky API response.

    This is immutable — a snapshot in time. We compare two
    AircraftState objects to detect what changed.
    """
    icao24:            str
    callsign:          Optional[str]
    longitude:         Optional[float]
    latitude:          Optional[float]
    altitude_ft:       Optional[float]
    on_ground:         bool
    velocity_kmh:      Optional[float]
    heading_deg:       Optional[float]
    vertical_rate_fpm: Optional[float]
    timestamp:         datetime


@dataclass
class TerritoryMemory:
    """
    Tracks which territory an aircraft is currently flying over
    and the database row ID of the open territory crossing.

    Stored in territory_memory dict keyed by icao24.
    Used to detect when an aircraft crosses into a new territory
    and to close the previous crossing row.

    territory_type is either 'COUNTRY' or 'STATE':
    - 'STATE'   — a state boundary was found in dim_state
                  for the current position. State takes priority.
    - 'COUNTRY' — no state boundary found but a country boundary
                  was found in dim_country. Country is the fallback.

    territory_id and territory_name hold the ID and name of
    whichever territory type was detected — state or country.
    """
    country_id:     int         # Always the country — for change detection
    country_name:   str         # Always the country name
    territory_type: str         # 'STATE' or 'COUNTRY'
    territory_id:   int         # state_id or country_id depending on type
    territory_name: str         # state_name or country_name
    crossing_id:    int         # The crossing_id of the open crossing row
    entered_at:     datetime


# ─────────────────────────────────────────────────────────────
# REUSABLE: DATABASE CONNECTION
# ─────────────────────────────────────────────────────────────
def get_connection():
    """
    Creates and returns a PostgreSQL connection.
    Reusable — identical to the function in seed scripts.
    Exits with error email if connection fails.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        log.info("  Connected to PostgreSQL.")
        return conn
    except Exception as e:
        log.error(f"  Cannot connect to PostgreSQL: {e}")
        send_email(
            subject="Transit Radar 411 — Ingestor DB connection failed",
            body=f"opensky_ingestor.py could not connect to PostgreSQL.\n\nError: {e}"
        )
        sys.exit(1)


# ─────────────────────────────────────────────────────────────
# REUSABLE: AIRPORT ICAO → IATA LOOKUP
# ─────────────────────────────────────────────────────────────
def build_airport_lookup(conn) -> dict:
    """
    Builds ICAO -> IATA airport code mapping from dim_airport.
    Reusable — identical to the function in seed scripts.
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT airport_icao_code, airport_iata_code
        FROM dim_airport
        WHERE airport_icao_code IS NOT NULL
        AND airport_iata_code IS NOT NULL
    """)
    rows   = cursor.fetchall()
    cursor.close()
    lookup = {row[0]: row[1] for row in rows}
    log.info(f"  Airport lookup: {len(lookup):,} ICAO -> IATA mappings loaded.")
    return lookup


# ─────────────────────────────────────────────────────────────
# REUSABLE: FETCH OPENSKY STATES
# ─────────────────────────────────────────────────────────────
def fetch_opensky_states() -> list[AircraftState]:
    """
    Calls the OpenSky Network REST API and returns a list of
    AircraftState objects for all aircraft in the bounding box.

    OpenSky returns a JSON object with a 'states' array.
    Each element is a fixed-position list of 17 values.
    We unpack by position index — documented at:
    https://openskynetwork.github.io/opensky-api/rest.html

    Position mapping:
        0  = icao24
        1  = callsign
        5  = longitude (degrees)
        6  = latitude  (degrees)
        7  = baro_altitude (metres)
        8  = on_ground (boolean)
        9  = velocity (metres/second)
        10 = true_track (degrees)
        11 = vertical_rate (metres/second)

    All units are converted to imperial/km for consistency:
        metres      -> feet    (× 3.28084)
        m/s speed   -> km/h    (× 3.6)
        m/s vrate   -> ft/min  (× 196.85)

    Returns empty list on any API error — the main loop
    will simply skip this cycle and try again in 60 seconds.

    Reusable — any script that needs live OpenSky data can
    call this function.
    """
    try:
        response = requests.get(
            OPENSKY_API_URL,
            params=BOUNDING_BOX,
            timeout=30
        )
        response.raise_for_status()
        data   = response.json()
        states = data.get("states") or []

    except requests.exceptions.Timeout:
        log.warning("  OpenSky API timed out. Skipping cycle.")
        return []
    except requests.exceptions.RequestException as e:
        log.warning(f"  OpenSky API error: {e}. Skipping cycle.")
        return []

    now    = datetime.now(timezone.utc)
    result = []

    for s in states:
        try:
            alt_m    = s[7]
            vel_ms   = s[9]
            vrate_ms = s[11]

            result.append(AircraftState(
                icao24=s[0],
                callsign=(s[1] or "").strip() or None,
                longitude=s[5],
                latitude=s[6],
                altitude_ft=round(alt_m * 3.28084, 1)   if alt_m    is not None else None,
                on_ground=bool(s[8]),
                velocity_kmh=round(vel_ms * 3.6, 1)     if vel_ms   is not None else None,
                heading_deg=s[10],
                vertical_rate_fpm=round(vrate_ms * 196.85, 1) if vrate_ms is not None else None,
                timestamp=now
            ))
        except (IndexError, TypeError):
            continue

    log.info(f"  OpenSky: {len(result):,} aircraft in bounding box.")
    return result


# ─────────────────────────────────────────────────────────────
# REUSABLE: STATE CHANGE DETECTION
# ─────────────────────────────────────────────────────────────
def detect_event(
    prev: Optional[AircraftState],
    curr: AircraftState
) -> Optional[str]:
    """
    Compares two consecutive states for the same aircraft and
    returns an event type if a meaningful transition occurred.
    Returns None if nothing significant changed.

    This is the core business logic of the entire pipeline.
    Every decision about what constitutes a meaningful event
    lives here.

    State machine:
        on_ground False -> True              = LANDED
        on_ground True  -> False             = DEPARTED
        airborne + vrate > 200 fpm           = CLIMBING
        airborne + vrate < -200 fpm          = DESCENDING
        airborne + vrate between -200 and 200
                 + altitude > 10,000ft       = CRUISING

    Why 200 fpm as the threshold?
    Aircraft in level flight have minor vertical fluctuations.
    200 fpm filters out normal turbulence and autopilot
    corrections while still catching genuine climbs and descents.

    Reusable — this exact logic can be unit tested independently
    and used by any component that processes aircraft states.
    """
    if prev is None:
        # First time we see this aircraft — no comparison possible
        return None

    # LANDED: was airborne, now on the ground
    if not prev.on_ground and curr.on_ground:
        return "LANDED"

    # DEPARTED: was on the ground, now airborne
    if prev.on_ground and not curr.on_ground:
        return "DEPARTED"

    # For airborne transitions
    if not curr.on_ground and not prev.on_ground:
        vrate = curr.vertical_rate_fpm or 0
        alt   = curr.altitude_ft or 0

        if vrate > CLIMBING_THRESHOLD_FPM:
            return "CLIMBING"

        if vrate < DESCENDING_THRESHOLD_FPM:
            return "DESCENDING"

        if abs(vrate) <= CLIMBING_THRESHOLD_FPM and alt > CRUISE_ALT_FT:
            # Only fire CRUISING once — when transitioning from
            # a climb into level flight
            prev_vrate = prev.vertical_rate_fpm or 0
            if abs(prev_vrate) > CLIMBING_THRESHOLD_FPM:
                return "CRUISING"

    return None


# ─────────────────────────────────────────────────────────────
# REUSABLE: TERRITORY DETECTION (COUNTRY + STATE)
# ─────────────────────────────────────────────────────────────
def detect_territory(
    conn,
    latitude: float,
    longitude: float
) -> Optional[tuple]:
    """
    Detects which territory an aircraft is currently over
    by querying dim_country and dim_state using PostGIS
    ST_Contains.

    STATE TAKES PRIORITY OVER COUNTRY.

    Detection logic on every ping:
        Step 1 — Query dim_country
                 If no country found → return None (open ocean)

        Step 2 — Query dim_state using the same coordinates
                 If a state is found → return STATE result
                 If no state found   → return COUNTRY result

    Returns a tuple of:
        (territory_type, territory_id, territory_name,
         country_id, country_name, iso2)

    territory_type is 'STATE' or 'COUNTRY'.
    country_id and country_name are always the parent country —
    used by TerritoryMemory to detect country-level changes
    even when tracking at state level.

    Returns None if the coordinate is over open ocean or
    uninhabited airspace with no named territory.

    NOTE: ST_Point takes (longitude, latitude) — x before y.
    This is a common PostGIS gotcha — opposite of how humans
    usually say coordinates.

    Reusable — load_historical_territories.py uses this same
    logic for processing historical flight paths.
    """
    try:
        cursor = conn.cursor()

        # Step 1 — Query dim_country
        cursor.execute("""
            SELECT country_id, country_name, iso2
            FROM dim_country
            WHERE ST_Contains(
                boundary,
                ST_SetSRID(ST_Point(%s, %s), 4326)
            )
            LIMIT 1
        """, (longitude, latitude))
        country_result = cursor.fetchone()

        if not country_result:
            cursor.close()
            return None     # Open ocean or uninhabited airspace

        country_id, country_name, iso2 = country_result

        # Step 2 — Query dim_state for the same coordinates
        # State takes priority over country when found
        cursor.execute("""
            SELECT state_id, state_name, country_iso2
            FROM dim_state
            WHERE ST_Contains(
                boundary,
                ST_SetSRID(ST_Point(%s, %s), 4326)
            )
            LIMIT 1
        """, (longitude, latitude))
        state_result = cursor.fetchone()
        cursor.close()

        if state_result:
            state_id, state_name, _ = state_result
            # State found — use STATE as territory type
            return (
                "STATE",        # territory_type
                state_id,       # territory_id
                state_name,     # territory_name
                country_id,     # parent country_id
                country_name,   # parent country_name
                iso2            # parent country iso2
            )
        else:
            # No state found — fall back to COUNTRY
            return (
                "COUNTRY",      # territory_type
                country_id,     # territory_id
                country_name,   # territory_name
                country_id,     # same as territory_id for country
                country_name,   # same as territory_name for country
                iso2
            )

    except Exception as e:
        log.warning(f"  Territory detection error: {e}")
        return None


# ─────────────────────────────────────────────────────────────
# REUSABLE: WRITE FLIGHT EVENT
# ─────────────────────────────────────────────────────────────
def write_flight_event(
    state:       AircraftState,
    event_type:  str,
    conn,
    origin_iata:      Optional[str] = None,
    destination_iata: Optional[str] = None
) -> Optional[int]:
    """
    Inserts one flight event into daily_flight_events.
    Returns the auto-generated event_id on success.
    Returns None if the insert fails or is a duplicate.

    The event_id is needed to link territory crossings back
    to this specific flight event.

    ON CONFLICT DO NOTHING prevents duplicate events if the
    same state change is somehow detected twice.

    Reusable — any component that needs to write a flight event
    can call this function.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO daily_flight_events (
                icao24, callsign, event_type, event_timestamp,
                event_date, latitude, longitude, altitude_ft,
                velocity_kmh, heading_deg, vertical_rate_fpm,
                on_ground, origin_iata, destination_iata, source
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT DO NOTHING
            RETURNING event_id
            """,
            (
                state.icao24,
                state.callsign,
                event_type,
                state.timestamp,
                state.timestamp.date(),
                state.latitude,
                state.longitude,
                state.altitude_ft,
                state.velocity_kmh,
                state.heading_deg,
                state.vertical_rate_fpm,
                state.on_ground,
                origin_iata,
                destination_iata,
                SOURCE
            )
        )
        result = cursor.fetchone()
        conn.commit()
        return result[0] if result else None
    except Exception as e:
        conn.rollback()
        if "fk_daily_events_aircraft" in str(e):
            # Aircraft not in dim_aircraft — skip silently
            # This is normal for military/experimental aircraft
            log.debug(f"  Skipping unknown aircraft: {state.icao24}")
        else:
            log.error(f"  write_flight_event error: {e}")
            log.error(f"  write_flight_event error: {e}")
        return None
    finally:
        cursor.close()


# ─────────────────────────────────────────────────────────────
# REUSABLE: WRITE TERRITORY CROSSING
# ─────────────────────────────────────────────────────────────
def open_territory_crossing(
    conn,
    flight_event_id: int,
    state:           AircraftState,
    territory_type:  str,
    territory_id:    int,
    territory_name:  str,
    crossing_role:   str
) -> Optional[int]:
    """
    Opens a new territory crossing row in daily_territory_crossings.
    Returns the crossing_id of the new row.

    Called when an aircraft enters a new territory — either a
    new country or a new state within a country.

    territory_type is 'STATE' or 'COUNTRY' — passed in from
    detect_territory() so the correct type is stored.

    crossing_role is one of:
        START  — first territory after DEPARTED event
        ROUTE  — territory between START and END
        END    — territory where LANDED event was detected
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO daily_territory_crossings (
                flight_event_id, icao24, callsign,
                territory_type, territory_id, territory_name,
                crossing_role, flight_status,
                entered_at, entered_date
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s,
                %s, 'IN_PROGRESS',
                %s, %s
            )
            RETURNING crossing_id
            """,
            (
                flight_event_id,
                state.icao24,
                state.callsign,
                territory_type,
                territory_id,
                territory_name,
                crossing_role,
                state.timestamp,
                state.timestamp.date()
            )
        )
        result = cursor.fetchone()
        conn.commit()
        return result[0] if result else None
    except Exception as e:
        conn.rollback()
        log.error(f"  open_territory_crossing error: {e}")
        return None
    finally:
        cursor.close()


def close_territory_crossing(
    conn,
    crossing_id: int,
    exited_at:   datetime,
    entered_at:  datetime,
    flight_status: str = "IN_PROGRESS"
) -> None:
    """
    Closes a territory crossing by filling in exited_at,
    exited_date and duration_minutes.

    Called when an aircraft exits a country — either by entering
    a different country or by landing (flight_status = COMPLETED).

    duration_minutes is calculated as the difference between
    exited_at and entered_at in minutes. This is the exact
    amount of time the aircraft spent over this territory.
    """
    duration_minutes = (exited_at - entered_at).total_seconds() / 60

    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE daily_territory_crossings
            SET exited_at        = %s,
                exited_date      = %s,
                duration_minutes = %s,
                flight_status    = %s
            WHERE crossing_id = %s
            """,
            (
                exited_at,
                exited_at.date(),
                round(duration_minutes, 2),
                flight_status,
                crossing_id
            )
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        log.error(f"  close_territory_crossing error: {e}")
    finally:
        cursor.close()


# ─────────────────────────────────────────────────────────────
# REUSABLE: PUBLISH TO KAFKA
# ─────────────────────────────────────────────────────────────
def create_kafka_producer():
    """
    Creates and returns a Kafka producer client.

    The producer is what WRITES messages to Kafka.
    Uses the kafka-python library.

    value_serializer converts Python dicts to JSON bytes
    before sending — Kafka only stores bytes.

    Retries 5 times in case Kafka is still starting up.

    Returns None if Kafka is unavailable — the ingestion
    service will continue without Kafka rather than crashing.
    Kafka is useful but not critical for the pipeline to function.
    """
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
            api_version=(2, 8, 0)
        )
        log.info(f"  Kafka producer connected to {KAFKA_BROKER}.")
        return producer
    except Exception as e:
        log.warning(f"  Kafka unavailable: {e}. Continuing without Kafka.")
        return None


def publish_to_kafka(
    producer,
    state:      AircraftState,
    event_type: str
) -> None:
    """
    Publishes one flight event to the Kafka flight_events topic.

    The message key is icao24 — this ensures all events for
    the same aircraft land on the same Kafka partition,
    preserving chronological order for downstream consumers.

    If the producer is None (Kafka unavailable) this is a no-op.

    Reusable — any component can call this to publish events.
    """
    if not producer:
        return
    try:
        message = {
            "icao24":            state.icao24,
            "callsign":          state.callsign,
            "event_type":        event_type,
            "event_timestamp":   state.timestamp.isoformat(),
            "latitude":          state.latitude,
            "longitude":         state.longitude,
            "altitude_ft":       state.altitude_ft,
            "velocity_kmh":      state.velocity_kmh,
            "heading_deg":       state.heading_deg,
            "vertical_rate_fpm": state.vertical_rate_fpm,
            "on_ground":         state.on_ground,
            "source":            SOURCE
        }
        producer.send(
            KAFKA_TOPIC,
            key=state.icao24.encode("utf-8"),
            value=message
        )
    except Exception as e:
        log.warning(f"  Kafka publish error: {e}")


# ─────────────────────────────────────────────────────────────
# MAIN POLLING LOOP
# ─────────────────────────────────────────────────────────────
def run():
    """
    The main continuous loop. Runs until Ctrl+C is pressed.

    Every 60 seconds:
    1. Fetch live aircraft states from OpenSky
    2. For each aircraft compare to last known state
    3. Detect state changes → write to daily_flight_events + Kafka
    4. Detect territory changes → write to daily_territory_crossings
    5. Update in-memory state dictionaries
    6. Sleep 60 seconds
    """
    log.info("=" * 60)
    log.info("  Transit Radar 411 — OpenSky Ingestor Starting")
    log.info("=" * 60)
    log.info(f"  Polling interval : {POLL_INTERVAL_S}s")
    log.info(f"  Bounding box     : Africa + Middle East")
    log.info(f"  Kafka broker     : {KAFKA_BROKER}")

    # Establish database connection
    conn = get_connection()

    # Build airport lookup for ICAO -> IATA conversion
    airport_lookup = build_airport_lookup(conn)

    # Connect to Kafka (non-fatal if unavailable)
    kafka_producer = create_kafka_producer()

    # ── In-memory state dictionaries ────────────────────────
    # aircraft_memory: icao24 -> AircraftState (last known state)
    # Used to detect state changes between poll cycles.
    aircraft_memory: dict[str, AircraftState] = {}

    # territory_memory: icao24 -> TerritoryMemory
    # Tracks which country each aircraft is currently over
    # and the crossing_id of the open crossing row.
    territory_memory: dict[str, TerritoryMemory] = {}

    # last_event_id: icao24 -> event_id
    # Tracks the most recent flight event_id for each aircraft.
    # Used as flight_event_id when opening territory crossings.
    last_event_id: dict[str, int] = {}

    send_email(
        subject="Transit Radar 411 — Ingestor started",
        body=(
            f"opensky_ingestor.py is now running.\n\n"
            f"  Polling every   : {POLL_INTERVAL_S} seconds\n"
            f"  Bounding box    : Africa + Middle East\n"
            f"  Writing to      : daily_flight_events\n"
            f"                    daily_territory_crossings\n"
            f"  Publishing to   : Kafka topic '{KAFKA_TOPIC}'\n"
        )
    )

    cycle = 0

    try:
        while True:
            cycle += 1
            log.info(f"\n─── Cycle {cycle} ──────────────────────────────")

            # ── Step 1: Fetch live states from OpenSky ──────
            current_states = fetch_opensky_states()

            if not current_states:
                log.info("  No data returned. Sleeping...")
                time.sleep(POLL_INTERVAL_S)
                continue

            events_written    = 0
            crossings_updated = 0

            # ── Step 2: Process each aircraft ───────────────
            for state in current_states:
                icao24     = state.icao24
                prev_state = aircraft_memory.get(icao24)

                # ── Step 3: Detect state change ─────────────
                event_type = detect_event(prev_state, state)

                if event_type:
                    log.info(
                        f"  EVENT: {event_type:<12} | {icao24} | "
                        f"{state.callsign or 'unknown':>8} | "
                        f"alt={state.altitude_ft or 0:>8,.0f}ft"
                    )

                    # Determine origin/destination for DEPARTED/LANDED
                    origin_iata      = None
                    destination_iata = None

                    if event_type == "DEPARTED" and state.latitude and state.longitude:
                        # Find nearest airport to departure position
                        cursor = conn.cursor()
                        cursor.execute("""
                            SELECT airport_iata_code
                            FROM dim_airport
                            WHERE airport_iata_code IS NOT NULL
                            AND is_commercial = TRUE
                            ORDER BY ST_Distance(
                                ST_SetSRID(ST_Point(longitude_deg, latitude_deg), 4326),
                                ST_SetSRID(ST_Point(%s, %s), 4326)
                            )
                            LIMIT 1
                        """, (state.longitude, state.latitude))
                        result = cursor.fetchone()
                        cursor.close()
                        if result:
                            origin_iata = result[0]

                    # Write to daily_flight_events
                    event_id = write_flight_event(
                        state=state,
                        event_type=event_type,
                        conn=conn,
                        origin_iata=origin_iata,
                        destination_iata=destination_iata
                    )

                    if event_id:
                        last_event_id[icao24] = event_id
                        events_written += 1

                    # Publish to Kafka
                    publish_to_kafka(kafka_producer, state, event_type)

                    # Handle territory crossing role on LANDED
                    if event_type == "LANDED" and icao24 in territory_memory:
                        tm = territory_memory[icao24]
                        close_territory_crossing(
                            conn=conn,
                            crossing_id=tm.crossing_id,
                            exited_at=state.timestamp,
                            entered_at=tm.entered_at,
                            flight_status="COMPLETED"
                        )
                        del territory_memory[icao24]

                # ── Step 4: Detect territory change ─────────
                feid = last_event_id.get(icao24)
                if state.latitude and state.longitude and not state.on_ground and feid:
                    # Only detect territory if we have a valid flight event
                    # to link the crossing to. If last_event_id has no entry
                    # for this aircraft it means we have not seen a state
                    # change yet — the aircraft was already airborne when
                    # the ingestor started. Skip territory detection until
                    # we record a proper flight event for it
                    if icao24 not in last_event_id:
                        # Update memory so we can detect state changes
                        # next cycle but do not write any territory rows
                        aircraft_memory[icao24] = state
                        continue
                    territory = detect_territory(
                        conn=conn,
                        latitude=state.latitude,
                        longitude=state.longitude
                    )

                    if territory:
                        (
                            territory_type, territory_id, territory_name,
                            country_id, country_name, iso2
                        ) = territory

                        prev_territory = territory_memory.get(icao24)

                        # Detect a meaningful territory change.
                        # A change occurs when either:
                        # 1. No previous territory recorded (first detection)
                        # 2. The country changed
                        # 3. The territory_type changed (country → state
                        #    or state → country within same country)
                        # 4. The territory_id changed (entered a new state)
                        territory_changed = (
                            prev_territory is None
                            or prev_territory.country_id    != country_id
                            or prev_territory.territory_type != territory_type
                            or prev_territory.territory_id   != territory_id
                        )

                        if territory_changed:

                            # Close the previous crossing if one is open
                            if prev_territory:
                                close_territory_crossing(
                                    conn=conn,
                                    crossing_id=prev_territory.crossing_id,
                                    exited_at=state.timestamp,
                                    entered_at=prev_territory.entered_at,
                                    flight_status="IN_PROGRESS"
                                )

                            # Determine crossing role
                            if event_type == "DEPARTED":
                                crossing_role = "START"
                            elif event_type == "LANDED":
                                crossing_role = "END"
                            elif icao24 not in territory_memory:
                                crossing_role = "START"
                            else:
                                crossing_role = "ROUTE"

                            if not feid:
                                aircraft_memory[icao24] = state
                                continue

                            # Open new crossing with correct territory type
                            crossing_id = open_territory_crossing(
                                conn=conn,
                                flight_event_id=feid,
                                state=state,
                                territory_type=territory_type,
                                territory_id=territory_id,
                                territory_name=territory_name,
                                crossing_role=crossing_role
                            )

                            if crossing_id:
                                territory_memory[icao24] = TerritoryMemory(
                                    country_id=country_id,
                                    country_name=country_name,
                                    territory_type=territory_type,
                                    territory_id=territory_id,
                                    territory_name=territory_name,
                                    crossing_id=crossing_id,
                                    entered_at=state.timestamp
                                )
                                crossings_updated += 1
                                log.info(
                                    f"  TERRITORY: {crossing_role:<6} | "
                                    f"{icao24} | "
                                    f"{territory_type:<7} | "
                                    f"{territory_name}"
                                )

                # ── Step 5: Update memory ────────────────────
                aircraft_memory[icao24] = state

            # Flush Kafka to ensure all messages are sent
            if kafka_producer:
                kafka_producer.flush()

            log.info(
                f"  Cycle {cycle} complete — "
                f"events: {events_written} | "
                f"crossings: {crossings_updated} | "
                f"tracking: {len(aircraft_memory):,} aircraft"
            )

            time.sleep(POLL_INTERVAL_S)

    except KeyboardInterrupt:
        log.info("\n  Shutdown requested by user.")

    except Exception as e:
        log.error(f"  Fatal error: {e}")
        send_email(
            subject="Transit Radar 411 — Ingestor crashed",
            body=f"opensky_ingestor.py crashed with an unexpected error.\n\nError: {e}"
        )

    finally:
        if kafka_producer:
            kafka_producer.close()
        conn.close()
        log.info("  Connections closed. Goodbye.")


if __name__ == "__main__":
    run()