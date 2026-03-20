-- ============================================================
-- 08_daily_territory_crossings.sql
--
-- PURPOSE
-- -------
-- Records every territory crossing detected for flights today.
-- One row is written each time an aircraft enters a new named
-- territory — a country, and in future iterations a state,
-- landmark or body of water.
--
-- This table does NOT write a row for every position ping.
-- The geospatial detection logic compares consecutive pings
-- for the same aircraft and only writes when the territory
-- changes. If consecutive pings both show the aircraft over
-- Nigeria, nothing is written. Only when the aircraft crosses
-- into Niger does a new row appear.
--
-- HOW TERRITORY DETECTION WORKS
-- ------------------------------
-- For each position ping the ingestion service runs:
--
--   SELECT country_id, country_name
--   FROM dim_country
--   WHERE ST_Contains(
--       boundary,
--       ST_SetSRID(ST_Point(longitude, latitude), 4326)
--   )
--   LIMIT 1;
--
-- If the result is different from the last recorded territory
-- for that icao24, a new crossing row is written and the
-- previous row's exited_at and duration_minutes are updated.
--
-- If the coordinate falls over open ocean or uninhabited
-- airspace with no named territory, nothing is written until
-- the aircraft enters a named territory again.
--
-- CROSSING ROLE
-- -------------
-- START — the first territory crossing after a DEPARTED event.
--         Only one START row per flight.
-- ROUTE — every crossing between departure and arrival.
-- END   — the territory crossing active when LANDED is detected.
--         Only one END row per flight.
--
-- FLIGHT STATUS
-- -------------
-- IN_PROGRESS — the flight has not yet landed. exited_at and
--               duration_minutes may be NULL for the active
--               crossing.
-- COMPLETED   — the flight has landed. All crossing rows for
--               this flight are updated to COMPLETED by the
--               ingestion service when LANDED is detected.
--
-- MIDNIGHT CUTOFF
-- ---------------
-- When the Airflow EOD DAG runs at 23:59:59, flights that
-- are still airborne are copied to hist_territory_crossings
-- with flight_status = IN_PROGRESS and exited_at = NULL.
-- The next day when those flights land, the ingestion service
-- updates the hist_territory_crossings rows directly to fill
-- in exited_at, duration_minutes and flight_status = COMPLETED.
--
-- END OF DAY
-- ----------
-- At 23:59:59 the Airflow DAG copies all rows from this table
-- into hist_territory_crossings then truncates this table
-- clean for the next day.
--
-- CURRENT SCOPE
-- -------------
-- territory_type is currently always COUNTRY.
-- STATE, LANDMARK and BODY_OF_WATER will be added in a future
-- iteration when dim_state and dim_landmark are populated.
--
-- FOREIGN KEYS
-- ------------
-- flight_event_id → daily_flight_events.event_id
-- territory_id    → dim_country.country_id (when type=COUNTRY)
-- ============================================================


CREATE TABLE IF NOT EXISTS daily_territory_crossings (

    -- ── Internal surrogate key ──────────────────────────────
    crossing_id             SERIAL PRIMARY KEY,

    -- ── Link to the flight event ─────────────────────────────
    -- The specific flight event in daily_flight_events that
    -- triggered this territory crossing detection.
    -- Links to the DEPARTED event for START crossings,
    -- and to the most recent event for ROUTE and END crossings.
    -- NOT NULL — every crossing must link to a flight event.
    flight_event_id         INTEGER NOT NULL,

    -- ── Denormalised aircraft identifiers ────────────────────
    -- icao24 and callsign are repeated here from
    -- daily_flight_events deliberately. This allows queries
    -- like "show all territories aircraft X flew over today"
    -- without joining back to daily_flight_events.
    -- This is called denormalisation — trading storage space
    -- for query performance. Intentional design decision.
    icao24                  VARCHAR(10) NOT NULL,
    callsign                VARCHAR(20),

    -- ── Territory classification ──────────────────────────────
    -- The type of territory this crossing belongs to.
    -- Currently always COUNTRY.
    -- Future values: STATE, LANDMARK, BODY_OF_WATER
    -- VARCHAR(20) gives room for future territory types.
    territory_type          VARCHAR(20) NOT NULL DEFAULT 'COUNTRY',

    -- ── Territory reference ───────────────────────────────────
    -- The ID of the specific territory from its dimension table.
    -- When territory_type = COUNTRY this points to dim_country.country_id
    -- When territory_type = STATE this will point to dim_state.state_id
    -- When territory_type = LANDMARK this will point to dim_landmark.landmark_id
    -- NOT NULL — every crossing must identify a specific territory.
    territory_id            INTEGER NOT NULL,

    -- The human-readable name of the territory.
    -- Stored directly here so dashboards can display it
    -- without joining to dim_country, dim_state or dim_landmark.
    -- Example: "Nigeria", "France", "Mediterranean Sea"
    -- NOT NULL — every crossing must have a readable name.
    territory_name          TEXT NOT NULL,

    -- ── Crossing role ─────────────────────────────────────────
    -- The role this territory plays in the flight journey.
    -- START — territory where the flight departed from.
    --         Assigned when a DEPARTED event is detected and
    --         the territory containing the departure airport
    --         is identified. Only one START per flight.
    -- ROUTE — territory the flight passed through en route.
    --         All crossings between START and END are ROUTE.
    -- END   — territory where the flight landed.
    --         Assigned when a LANDED event is detected.
    --         Only one END per flight.
    -- NOT NULL — every crossing must have a role.
    crossing_role           VARCHAR(10) NOT NULL,

    -- ── Flight status ─────────────────────────────────────────
    -- Whether the flight that generated this crossing has
    -- completed its journey or is still in progress.
    -- IN_PROGRESS — flight has not yet landed. exited_at may
    --               be NULL for the currently active crossing.
    -- COMPLETED   — flight has landed. All crossing rows for
    --               this flight have been updated to COMPLETED
    --               by the ingestion service at landing time.
    -- DEFAULT IN_PROGRESS — every crossing starts as in progress
    -- and only updates to COMPLETED when the flight lands.
    flight_status           VARCHAR(20) NOT NULL DEFAULT 'IN_PROGRESS',

    -- ── Entry timestamp ───────────────────────────────────────
    -- The exact UTC datetime when the aircraft was first
    -- detected inside this territory.
    -- NOT NULL — every crossing must have an entry time.
    entered_at              TIMESTAMP NOT NULL,

    -- The date portion of entered_at stored separately.
    -- Important for flights that cross a territory boundary
    -- near midnight — entered_date and exited_date may be
    -- different dates for the same crossing row.
    -- Example: entered_date = 2026-03-16
    --          exited_date  = 2026-03-17
    -- NOT NULL — always derived from entered_at.
    entered_date            DATE NOT NULL,

    -- ── Exit timestamp ────────────────────────────────────────
    -- The exact UTC datetime when the aircraft was last
    -- detected inside this territory before crossing out.
    -- NULL means the aircraft is still inside this territory
    -- at the time this record was last updated. This includes:
    --   1. The currently active crossing for any in-flight aircraft
    --   2. Any crossing rows copied to hist_territory_crossings
    --      at 23:59:59 for flights still airborne at midnight
    -- Filled in by the ingestion service when the next ping
    -- shows the aircraft in a different territory.
    exited_at               TIMESTAMP,

    -- The date portion of exited_at stored separately.
    -- NULL when exited_at is NULL.
    exited_date             DATE,

    -- ── Duration ─────────────────────────────────────────────
    -- How long the aircraft spent inside this territory
    -- in minutes. Calculated as:
    --   EXTRACT(EPOCH FROM (exited_at - entered_at)) / 60
    -- NULL while the aircraft is still inside the territory.
    -- Filled in at the same time as exited_at.
    -- Stored to avoid recalculating on every query.
    -- Example: 103.50 means 1 hour 43 minutes 30 seconds.
    duration_minutes        NUMERIC(8, 2),

    -- ── Audit timestamp ──────────────────────────────────────
    created_at              TIMESTAMP DEFAULT NOW(),

    -- ── Foreign key constraints ───────────────────────────────
    -- Every crossing must link to a valid flight event.
    CONSTRAINT fk_daily_crossings_event
        FOREIGN KEY (flight_event_id)
        REFERENCES daily_flight_events(event_id)
        ON DELETE CASCADE,
    -- ON DELETE CASCADE means if a flight event is deleted,
    -- all its territory crossings are automatically deleted too.
    -- This keeps the two tables in sync.

    -- Crossing role must be one of the three valid values.
    CONSTRAINT chk_crossing_role
        CHECK (crossing_role IN ('START', 'ROUTE', 'END')),

    -- Flight status must be one of the two valid values.
    CONSTRAINT chk_flight_status
        CHECK (flight_status IN ('IN_PROGRESS', 'COMPLETED')),

    -- Territory type must be a known value.
    CONSTRAINT chk_territory_type
        CHECK (territory_type IN ('COUNTRY', 'STATE', 'LANDMARK', 'BODY_OF_WATER'))

);


-- ============================================================
-- INDEXES
-- ============================================================

-- Most frequently queried — find all crossings for an aircraft
CREATE INDEX IF NOT EXISTS idx_daily_crossings_icao24
    ON daily_territory_crossings(icao24);

-- Find all crossings for a specific flight event
CREATE INDEX IF NOT EXISTS idx_daily_crossings_event
    ON daily_territory_crossings(flight_event_id);

-- Find all crossings for a specific territory
CREATE INDEX IF NOT EXISTS idx_daily_crossings_territory
    ON daily_territory_crossings(territory_id, territory_type);

-- Find all in-progress flights for status updates
CREATE INDEX IF NOT EXISTS idx_daily_crossings_status
    ON daily_territory_crossings(flight_status);

-- Find all START crossings for departure analysis
CREATE INDEX IF NOT EXISTS idx_daily_crossings_role
    ON daily_territory_crossings(crossing_role);

-- Find crossings that need exited_at filled in
CREATE INDEX IF NOT EXISTS idx_daily_crossings_exited
    ON daily_territory_crossings(exited_at)
    WHERE exited_at IS NULL;


-- ============================================================
-- CONFIRMATION
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE 'daily_territory_crossings created — territory detection table ready.';
END $$;