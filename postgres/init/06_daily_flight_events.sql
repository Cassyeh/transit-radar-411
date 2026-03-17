-- ============================================================
-- 06_daily_flight_events.sql
--
-- PURPOSE
-- -------
-- This is the "daily flights table" of the pipeline.
-- One row per meaningful flight state change detected today.
--
-- This table does NOT store raw position pings from OpenSky.
-- Instead, the ingestion service watches consecutive pings
-- and only writes a row when something meaningful changes —
-- a takeoff, a climb, a cruise, a descent, a landing.
-- This gives us roughly 4-5 rows per flight per day.
--
-- The icao24 column is the account_number equivalent —
-- the foreign key that links every event back to dim_aircraft.
--
-- EVENT TYPES
-- -----------
-- DEPARTED   — aircraft left the ground (on_ground FALSE,
--              was previously TRUE)
-- CLIMBING   — aircraft is gaining altitude (vertical_rate > 0)
-- CRUISING   — aircraft is at cruise altitude (vertical_rate ≈ 0,
--              altitude > 10,000ft)
-- DESCENDING — aircraft is losing altitude (vertical_rate < 0)
-- LANDED     — aircraft touched down (on_ground TRUE,
--              was previously FALSE)
--
-- ENRICHMENT
-- ----------
-- origin_iata and destination_iata start as NULL when a row
-- is first inserted by the ingestion service. They are filled
-- in by the Silver layer enrichment job which queries the
-- OpenSky flights endpoint once per unique icao24 per day.
--
-- END OF DAY
-- ----------
-- At 23:59:59 every night the Airflow DAG copies all rows
-- from this table into hist_flight_events, then truncates
-- this table clean for the next day.
--
-- FOREIGN KEYS
-- ------------
-- icao24          → dim_aircraft.icao24
-- origin_iata     → dim_airport.airport_iata_code
-- destination_iata → dim_airport.airport_iata_code
--
-- DATA SOURCE
-- -----------
-- Populated in real time by ingestion/opensky_ingestor.py
-- which polls https://opensky-network.org/api/states/all
-- every 60 seconds.
-- ============================================================


CREATE TABLE IF NOT EXISTS daily_flight_events (

    -- ── Internal surrogate key ──────────────────────────────
    event_id                SERIAL PRIMARY KEY,

    -- ── Aircraft identifier ──────────────────────────────────
    -- This is the foreign key that links every event back to
    -- dim_aircraft
    -- NOT NULL — every event must belong to a known aircraft.
    icao24                  VARCHAR(10) NOT NULL,

    -- ── Flight identifier ────────────────────────────────────
    -- The radio callsign broadcast by the aircraft.
    -- Trailing whitespace is stripped by the ingestion service
    -- before insertion — OpenSky pads callsigns to 8 chars.
    -- NULL is allowed — some aircraft do not broadcast a callsign.
    callsign                VARCHAR(20),

    -- ── Event classification ─────────────────────────────────
    -- The type of state change that triggered this row.
    -- Derived by the ingestion service by comparing consecutive
    -- position pings for the same aircraft.
    -- Allowed values: DEPARTED, CLIMBING, CRUISING,
    --                 DESCENDING, LANDED
    -- NOT NULL — every row must have a classified event type.
    event_type              VARCHAR(30) NOT NULL,

    -- ── Timing ───────────────────────────────────────────────
    -- Converted from Unix timestamp (OpenSky position 3)
    -- to a standard PostgreSQL TIMESTAMP by the ingestion
    -- service. Stored in UTC.
    -- NOT NULL — every event must have a timestamp.
    event_timestamp         TIMESTAMP NOT NULL,

    -- The date portion of event_timestamp stored separately.
    event_date              DATE NOT NULL DEFAULT CURRENT_DATE,

    -- ── Position at moment of event ──────────────────────────
    latitude                NUMERIC(9, 6),

    -- GPS longitude in decimal degrees.
    longitude               NUMERIC(9, 6),

    -- Barometric altitude in feet at moment of event.
    altitude_ft             NUMERIC(10, 2),

    -- Ground speed in kilometres per hour at moment of event.
    velocity_kmh            NUMERIC(8, 2),

    -- Direction of travel in degrees, 0–360.
    -- 0 = North, 90 = East, 180 = South, 270 = West.
    heading_deg             NUMERIC(6, 2),

    -- Rate of climb or descent in feet per minute.
    -- Positive value = climbing.
    -- Negative value = descending.
    -- Zero (approximately) = level flight.
    vertical_rate_fpm       NUMERIC(8, 2),

    -- Whether the aircraft was on the ground at this moment.
    -- TRUE  = on the ground (taxiing, parked, or just landed)
    -- FALSE = airborne
    on_ground               BOOLEAN,

    -- ── Airport context ──────────────────────────────────────
    -- The IATA code of the departure airport.
    -- NULL when first inserted — filled in by Silver layer
    -- enrichment job which queries the OpenSky flights
    -- endpoint once per unique icao24 per day.
    -- When enriched, links to dim_airport.airport_iata_code.
    -- Example: "LOS" for Lagos, "LHR" for London Heathrow.
    origin_iata             VARCHAR(10),

    -- The IATA code of the destination airport.
    -- NULL when first inserted — filled in by Silver layer
    -- enrichment job after the flight has landed and OpenSky
    -- has processed the full trajectory.
    -- Example: "LHR" for London Heathrow.
    destination_iata        VARCHAR(10),

    -- ── Data source ──────────────────────────────────────────
    source                  VARCHAR(20) DEFAULT 'opensky',

    -- ── Audit timestamp ──────────────────────────────────────
    created_at              TIMESTAMP DEFAULT NOW(),

    -- ── Foreign key constraint ───────────────────────────────
    -- Every event MUST link to a known aircraft in dim_aircraft.
    -- If an icao24 arrives that does not exist in dim_aircraft,
    -- the insert is rejected. This enforces data integrity —
    -- ON DELETE RESTRICT prevents deleting an aircraft from
    -- dim_aircraft if it has events in this table.
    CONSTRAINT fk_daily_events_aircraft
        FOREIGN KEY (icao24)
        REFERENCES dim_aircraft(icao24)
        ON DELETE RESTRICT
);


-- ============================================================
-- INDEXES
-- ============================================================

-- Most frequently queried column — every join from territory
-- crossings and Silver layer uses icao24
CREATE INDEX IF NOT EXISTS idx_daily_events_icao24
    ON daily_flight_events(icao24);

-- Used by the Airflow EOD DAG to select all rows for today
CREATE INDEX IF NOT EXISTS idx_daily_events_date
    ON daily_flight_events(event_date);

-- Used to filter by event type e.g. only DEPARTED events
CREATE INDEX IF NOT EXISTS idx_daily_events_type
    ON daily_flight_events(event_type);

-- Used to find all events for a specific flight
CREATE INDEX IF NOT EXISTS idx_daily_events_callsign
    ON daily_flight_events(callsign);

-- Used in Silver layer to find events with NULL origin_iata
-- that still need enrichment
CREATE INDEX IF NOT EXISTS idx_daily_events_origin
    ON daily_flight_events(origin_iata);

-- Used in Silver layer to find events with NULL destination
-- that still need enrichment
CREATE INDEX IF NOT EXISTS idx_daily_events_destination
    ON daily_flight_events(destination_iata);


-- ============================================================
-- CONFIRMATION
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE 'daily_flight_events created — daily transactions table ready.';
END $$;