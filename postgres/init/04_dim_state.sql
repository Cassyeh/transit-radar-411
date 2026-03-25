-- ============================================================
-- 04_dim_state.sql
--
-- PURPOSE
-- -------
-- State / province reference table. One row per first-level
-- administrative division (Admin-1) in the world.
--
-- Examples:
--   - Lagos (Nigeria)
--   - California (USA)
--   - Ontario (Canada)
--
-- GEOSPATIAL TERRITORY DETECTION
-- ------------------------------
-- The "boundary" column stores the full polygon of each
-- state's border. When an aircraft position ping arrives
-- with coordinates (latitude, longitude), PostGIS can check
-- which state the aircraft is flying over using ST_Contains().
--
-- Example query:
-- SELECT state_name FROM dim_state
-- WHERE ST_Contains(boundary, ST_Point(3.39, 6.45));
-- → Returns "Lagos"
--
--
-- BUSINESS KEY
-- ------------
-- adm1_code — unique identifier for each state from
-- Natural Earth.
-- Example: "NGA-2850" (Lagos), "USA-3521" (California)
--
-- This is enforced as NOT NULL and UNIQUE.
--
--
-- FOREIGN KEY RELATIONSHIP
-- ------------------------
-- country_iso3 links each state to its parent country
-- in dim_country using the ISO 3-letter code.
--
--
-- DATA SOURCE
-- -----------
-- Loaded once by seeds/load_geodata.py from:
--   Natural Earth ne_10m_admin_1_states_provinces.shp
--   https://www.naturalearthdata.com/
--
-- Filter applied:
--   featurecla = 'Admin-1 states provinces'
--
--
-- REQUIRES
-- --------
-- PostGIS extension — enabled by 00_extensions.sql
-- dim_country table — created by 03_dim_country.sql
-- ============================================================


CREATE TABLE IF NOT EXISTS dim_state (

    -- ── Internal surrogate key ──────────────────────────────
    state_id           SERIAL PRIMARY KEY,

    -- ── Business key ────────────────────────────────────────
    -- Unique identifier for each state/province.
    -- Example: "NGA-2850", "USA-3521"
    adm1_code          VARCHAR(10) NOT NULL UNIQUE,

    -- ISO 3166-2 code for the state.
    -- Example: "NG-LA" for Lagos, "US-CA" for California
    iso_3166_2         VARCHAR(10),

    -- ── State details ───────────────────────────────────────
    state_name         TEXT NOT NULL,

    -- Type of administrative division.
    -- Examples: "State", "Province", "Region"
    state_type         VARCHAR(36),

    -- ── Country linkage ─────────────────────────────────────
    -- 3-letter ISO country code (FK to dim_country.iso3)
    country_iso3       VARCHAR(3),

    -- 2-letter ISO country code (for convenience joins)
    country_iso2       VARCHAR(2),

    -- Country name for context / denormalized readability
    country_name       TEXT NOT NULL,  ---each state should have a country

    -- ── Representative coordinates ──────────────────────────PostGIS uses (lon, lat) order.
    centroid_lon       NUMERIC(9, 6),

    centroid_lat       NUMERIC(9, 6),

    -- ── Geospatial boundary ─────────────────────────────────
    -- Full polygon of the state/province boundary.
    boundary           geometry(Geometry, 4326),

    -- ── Audit timestamp ─────────────────────────────────────
    created_at         TIMESTAMP DEFAULT NOW(),

    -- ── Foreign key constraint ──────────────────────────────
    CONSTRAINT fk_dim_state_country
        FOREIGN KEY (country_iso3)
        REFERENCES dim_country(iso3)

);


-- ============================================================
-- INDEXES
-- ============================================================

-- Fast lookup using business key
CREATE INDEX IF NOT EXISTS idx_dim_state_adm1_code
    ON dim_state(adm1_code);

-- Used for joins with dim_country
CREATE INDEX IF NOT EXISTS idx_dim_state_country_iso3
    ON dim_state(country_iso3);

-- Used for joins using ISO2 where needed
CREATE INDEX IF NOT EXISTS idx_dim_state_country_iso2
    ON dim_state(country_iso2);

-- Used for filtering by state name
CREATE INDEX IF NOT EXISTS idx_dim_state_name
    ON dim_state(state_name);

-- Used for filtering by state type (State, Province, etc.)
CREATE INDEX IF NOT EXISTS idx_dim_state_type
    ON dim_state(state_type);

-- ── Spatial index ───────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_dim_state_boundary
    ON dim_state USING GIST(boundary);


-- ============================================================
-- CONFIRMATION
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE 'dim_state created — state/province reference table with PostGIS boundary ready.';
END $$;