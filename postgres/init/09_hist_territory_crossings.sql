-- ============================================================
-- 09_hist_territory_crossings.sql
--
-- PURPOSE
-- -------
-- Permanent record of every territory crossing ever detected.
-- Accumulates forever — one row per territory crossing per
-- flight, going back to the first day the pipeline ran.
--
-- This table is populated from two sources:
--
-- SOURCE 1 — DAILY EOD ROLL-UP
-- At 23:59:59 every night the Airflow DAG copies all rows
-- from daily_territory_crossings into this table, then
-- truncates daily_territory_crossings for the next day.
-- Rows from live pipeline data have source = 'opensky'.
--
-- SOURCE 2 — HISTORICAL SEED DATA
-- The historical backfill script seeds/load_historical_states.py
-- processes OpenSky state vectors from 2020 onwards through
-- the same territory detection logic and inserts the resulting
-- crossings here directly.
-- Rows from historical data have source = 'opensky_historical'.
--
-- MIDNIGHT CUTOFF HANDLING
-- ------------------------
-- Flights still airborne at 23:59:59 are copied here with:
--   flight_status = IN_PROGRESS
--   exited_at     = NULL
--   exited_date   = NULL
--   duration_minutes = NULL
--
-- The next day when those flights land, the ingestion service
-- updates these rows directly in hist_territory_crossings
-- filling in exited_at, exited_date, duration_minutes and
-- setting flight_status = COMPLETED.
-- The updated_at column records when this update happened.
--
-- KEY DIFFERENCES FROM daily_territory_crossings
-- ------------------------------------------------
-- 1. BIGSERIAL instead of SERIAL — grows forever
-- 2. FK on flight_event_id has no CASCADE — historical
--    crossing records must survive even if the linked
--    flight event is later cleaned up
-- 3. Three extra audit columns — inserted_at, batch_date,
--    updated_at — for full audit trail
--
-- FOREIGN KEYS
-- ------------
-- flight_event_id → hist_flight_events.event_id (no cascade)
-- territory_id    → dim_country.country_id (when type=COUNTRY)
-- ============================================================


CREATE TABLE IF NOT EXISTS hist_territory_crossings (

    -- ── Internal surrogate key ──────────────────────────────
    -- BIGSERIAL — this table grows forever, needs larger range
    crossing_id             BIGSERIAL PRIMARY KEY,

    -- ── Link to the flight event ─────────────────────────────
    -- Points to hist_flight_events.event_id.
    -- NOT NULL — every crossing must link to a flight event.
    flight_event_id         BIGINT NOT NULL,

    -- ── Denormalised aircraft identifiers ────────────────────
    -- Repeated here for fast queries without joins.
    -- Same intentional denormalisation as daily table.
    icao24                  VARCHAR(10) NOT NULL,
    callsign                VARCHAR(20),

    -- ── Territory classification ──────────────────────────────
    -- Currently always COUNTRY.
    -- Future values: STATE, LANDMARK, BODY_OF_WATER
    territory_type          VARCHAR(20) NOT NULL DEFAULT 'COUNTRY',

    -- ── Territory reference ───────────────────────────────────
    -- The ID of the specific territory from its dimension table.
    -- When territory_type = COUNTRY points to dim_country.country_id
    territory_id            INTEGER NOT NULL,

    -- Human-readable territory name stored directly.
    -- No join needed at query time.
    -- Example: "Nigeria", "France", "Mediterranean Sea"
    territory_name          TEXT NOT NULL,

    -- ── Crossing role ─────────────────────────────────────────
    -- START — territory where the flight departed from
    -- ROUTE — territory the flight passed through en route
    -- END   — territory where the flight landed
    crossing_role           VARCHAR(10) NOT NULL,

    -- ── Flight status ─────────────────────────────────────────
    -- IN_PROGRESS — flight has not yet landed.
    --               exited_at may be NULL for active crossing.
    --               This status is set for flights copied here
    --               at midnight before they have landed.
    -- COMPLETED   — flight has landed. All crossing rows for
    --               this flight have been updated to COMPLETED.
    --               updated_at records when this happened.
    flight_status           VARCHAR(20) NOT NULL DEFAULT 'IN_PROGRESS',

    -- ── Entry timestamp ───────────────────────────────────────
    -- UTC datetime when aircraft entered this territory.
    -- For historical seed data: derived from the "time" column
    -- in the OpenSky state vectors CSV.
    entered_at              TIMESTAMP NOT NULL,

    -- Date portion of entered_at stored separately.
    -- For flights crossing a territory boundary near midnight,
    -- entered_date and exited_date will be different dates.
    entered_date            DATE NOT NULL,

    -- ── Exit timestamp ────────────────────────────────────────
    -- UTC datetime when aircraft left this territory.
    -- NULL for:
    --   1. Flights still airborne at midnight when copied here
    --   2. The currently active crossing for in-progress flights
    -- Filled in when the flight lands the next day.
    exited_at               TIMESTAMP,

    -- Date portion of exited_at.
    -- NULL when exited_at is NULL.
    exited_date             DATE,

    -- ── Duration ─────────────────────────────────────────────
    -- Time spent inside this territory in minutes.
    -- NULL until exited_at is filled in.
    -- Calculated as:
    --   EXTRACT(EPOCH FROM (exited_at - entered_at)) / 60
    -- Stored to avoid recalculating on every query.
    -- Example: 103.50 = 1 hour 43 minutes 30 seconds
    duration_minutes        NUMERIC(8, 2),

    -- ── Data source ──────────────────────────────────────────
    -- Identifies where this row came from.
    -- 'opensky'            — live pipeline via EOD roll-up
    -- 'opensky_historical' — historical seed data 2020–2025
    source                  VARCHAR(30) NOT NULL DEFAULT 'opensky',

    -- ── EOD audit columns ────────────────────────────────────
    -- When this row was inserted into hist_territory_crossings
    -- by the Airflow EOD DAG or seed script.
    inserted_at             TIMESTAMP DEFAULT NOW(),

    -- Which EOD batch inserted this row.
    -- Example: 2026-03-16 means the DAG that ran on that night.
    -- For historical seed data: the date the seed script ran.
    batch_date              DATE DEFAULT CURRENT_DATE,

    -- ── Update tracking ──────────────────────────────────────
    -- The last time any column on this row was modified.
    -- This is critical for rows that arrive with
    -- flight_status = IN_PROGRESS and exited_at = NULL
    -- because they get updated the next day when the flight
    -- lands. updated_at tells you exactly when that update
    -- happened so you can audit the pipeline.
    -- DEFAULT NOW() sets the initial value at insert time.
    -- The ingestion service updates this column whenever it
    -- fills in exited_at, duration_minutes or flight_status.
    updated_at              TIMESTAMP DEFAULT NOW(),

    -- ── Original creation timestamp ──────────────────────────
    -- When the crossing row was first created in
    -- daily_territory_crossings before being moved here.
    -- For historical seed data: same as inserted_at.
    created_at              TIMESTAMP DEFAULT NOW(),

    -- ── Constraints ──────────────────────────────────────────
    -- Foreign key to hist_flight_events — no CASCADE.
    -- Historical crossing records must survive even if the
    -- linked flight event is later cleaned up.
    CONSTRAINT fk_hist_crossings_event
        FOREIGN KEY (flight_event_id)
        REFERENCES hist_flight_events(event_id)
        ON DELETE RESTRICT,

    -- Crossing role must be one of three valid values.
    CONSTRAINT chk_hist_crossing_role
        CHECK (crossing_role IN ('START', 'ROUTE', 'END')),

    -- Flight status must be one of two valid values.
    CONSTRAINT chk_hist_flight_status
        CHECK (flight_status IN ('IN_PROGRESS', 'COMPLETED')),

    -- Territory type must be a known value.
    CONSTRAINT chk_hist_territory_type
        CHECK (territory_type IN ('COUNTRY', 'STATE', 'LANDMARK', 'BODY_OF_WATER'))

);


-- ============================================================
-- INDEXES
-- ============================================================

-- Most frequently queried — find all crossings for an aircraft
CREATE INDEX IF NOT EXISTS idx_hist_crossings_icao24
    ON hist_territory_crossings(icao24);

-- Critical for ML training queries — always filter by date range
CREATE INDEX IF NOT EXISTS idx_hist_crossings_date
    ON hist_territory_crossings(entered_date);

-- Find all crossings for a specific flight event
CREATE INDEX IF NOT EXISTS idx_hist_crossings_event
    ON hist_territory_crossings(flight_event_id);

-- Find all crossings for a specific territory
CREATE INDEX IF NOT EXISTS idx_hist_crossings_territory
    ON hist_territory_crossings(territory_id, territory_type);

-- Find IN_PROGRESS rows that need updating when flights land
CREATE INDEX IF NOT EXISTS idx_hist_crossings_status
    ON hist_territory_crossings(flight_status)
    WHERE flight_status = 'IN_PROGRESS';

-- Find rows with NULL exited_at that still need updating
CREATE INDEX IF NOT EXISTS idx_hist_crossings_exited
    ON hist_territory_crossings(exited_at)
    WHERE exited_at IS NULL;

-- Used for EOD batch auditing
CREATE INDEX IF NOT EXISTS idx_hist_crossings_batch
    ON hist_territory_crossings(batch_date);

-- Used for route frequency analysis in Gold layer
CREATE INDEX IF NOT EXISTS idx_hist_crossings_role
    ON hist_territory_crossings(crossing_role);

-- Used to separate live from historical seed data
CREATE INDEX IF NOT EXISTS idx_hist_crossings_source
    ON hist_territory_crossings(source);


-- ============================================================
-- CONFIRMATION
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE 'hist_territory_crossings created — historical territory crossings table ready.';
END $$;