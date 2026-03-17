-- ============================================================
-- 07_hist_flight_events.sql
--
-- PURPOSE
-- -------
-- This is the "historical flights table" of the pipeline.
-- It accumulates flight state-change events permanently —
-- one row per meaningful state change, forever.
--
-- This table is populated from two sources:
--
-- SOURCE 1 — DAILY EOD ROLL-UP
-- At 23:59:59 every night the Airflow DAG copies all rows
-- from daily_flight_events into this table, then truncates
-- daily_flight_events clean for the next day.
-- Rows inserted this way have source = 'opensky'.
--
-- SOURCE 2 — HISTORICAL SEED DATA
-- Before the live pipeline starts, seeds/load_historical_states.py
-- loads a representative sample of OpenSky historical state
-- vector data from 2020 onwards. The seed script processes
-- the raw position pings through the same state-change
-- detection logic used by the live ingestion service —
-- only writing DEPARTED, CLIMBING, CRUISING, DESCENDING
-- and LANDED events.
-- Rows inserted this way have source = 'opensky_historical'.
-- Sampling strategy: one week per month per year 2020–2025.
--
-- FOREIGN KEYS
-- ------------
-- origin_iata      → dim_airport.airport_iata_code
-- destination_iata → dim_airport.airport_iata_code
--
-- ============================================================


CREATE TABLE IF NOT EXISTS hist_flight_events (

    -- ── Internal surrogate key ──────────────────────────────
    event_id                BIGSERIAL PRIMARY KEY,

    -- ── Aircraft identifier ──────────────────────────────────
    icao24                  VARCHAR(10) NOT NULL,

    -- ── Flight identifier ────────────────────────────────────
    callsign                VARCHAR(20),

    -- ── Event classification ─────────────────────────────────
    event_type              VARCHAR(30) NOT NULL,

    -- ── Timing ───────────────────────────────────────────────
    event_timestamp         TIMESTAMP NOT NULL,

    -- The date portion of event_timestamp stored separately.
    event_date              DATE NOT NULL,

    -- ── Position at moment of event ──────────────────────────
    latitude                NUMERIC(9, 6),

    -- GPS longitude in decimal degrees.
    longitude               NUMERIC(9, 6),

    -- Barometric altitude in feet at moment of event.
    altitude_ft             NUMERIC(10, 2),

    -- Ground speed in kilometres per hour.
    velocity_kmh            NUMERIC(8, 2),

    -- Direction of travel in degrees 0–360.
    heading_deg             NUMERIC(6, 2),

    -- Rate of climb or descent in feet per minute.
    -- Positive = climbing, Negative = descending, ~0 = level.
    vertical_rate_fpm       NUMERIC(8, 2),

    -- Whether the aircraft was on the ground at this moment.
    on_ground               BOOLEAN,

    -- ── Airport context ──────────────────────────────────────
    -- The IATA code of the departure airport.
    origin_iata             VARCHAR(10),

    -- The IATA code of the destination airport.
    destination_iata        VARCHAR(10),

    -- ── Data source ──────────────────────────────────────────
    -- Identifies where this row came from.
    -- 'opensky'            — live pipeline via EOD roll-up
    -- 'opensky_historical' — historical seed data 2020–2025
    source                  VARCHAR(30) NOT NULL DEFAULT 'opensky',

    -- ── EOD audit columns ────────────────────────────────────
    inserted_at             TIMESTAMP DEFAULT NOW(),

    -- The date of the EOD batch that inserted this row.
    -- For live data: always the date the Airflow DAG ran.
    -- For historical seed data: the date the seed script ran.
    batch_date              DATE DEFAULT CURRENT_DATE,

    -- ── Original creation timestamp ──────────────────────────
    created_at              TIMESTAMP DEFAULT NOW()

);


-- ============================================================
-- INDEXES
-- ============================================================

-- Most frequently queried — joins from territory crossings
-- and ML feature queries filter on icao24
CREATE INDEX IF NOT EXISTS idx_hist_events_icao24
    ON hist_flight_events(icao24);

-- Critical for date range queries — ML training data is
-- always selected by date range
CREATE INDEX IF NOT EXISTS idx_hist_events_date
    ON hist_flight_events(event_date);

-- Used to filter by event type for analysis
CREATE INDEX IF NOT EXISTS idx_hist_events_type
    ON hist_flight_events(event_type);

-- Used to filter by callsign for flight-level analysis
CREATE INDEX IF NOT EXISTS idx_hist_events_callsign
    ON hist_flight_events(callsign);

-- Used to separate live data from historical seed data
CREATE INDEX IF NOT EXISTS idx_hist_events_source
    ON hist_flight_events(source);

-- Used for EOD batch auditing and debugging
CREATE INDEX IF NOT EXISTS idx_hist_events_batch_date
    ON hist_flight_events(batch_date);

-- Used in route frequency analysis in Gold layer
CREATE INDEX IF NOT EXISTS idx_hist_events_origin
    ON hist_flight_events(origin_iata);

-- Used in route frequency analysis in Gold layer
CREATE INDEX IF NOT EXISTS idx_hist_events_destination
    ON hist_flight_events(destination_iata);


-- ============================================================
-- CONFIRMATION
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE 'hist_flight_events created — historical transactions table ready.';
END $$;