-- ============================================================
-- 03_dim_country.sql
--
-- PURPOSE
-- -------
-- Country reference table. One row per country in the world.
--
-- GEOSPATIAL TERRITORY DETECTION
--    The "boundary" column stores the full polygon of each
--    country's border. When an aircraft position ping arrives
--    with coordinates (latitude, longitude), PostGIS checks
--    those coordinates against every country boundary using
--    ST_Contains() to determine which country the aircraft
--    is currently flying over.
--
--    Example query:
--    SELECT country_name FROM dim_country
--    WHERE ST_Contains(boundary, ST_Point(3.39, 6.45));
--    → Returns "Nigeria"
--
--
-- PRIMARY KEY
-- -----------
-- iso2 — the 2-letter ISO 3166-1 alpha-2 country code.
-- Example: "NG" for Nigeria, "GB" for United Kingdom
-- This is the same code stored in dim_aircraft.country_iso2
-- and dim_airport.iso_country — enabling clean direct joins.
--
-- NOTE ON iso2 VALUES
-- -------------------
-- Some territories in Natural Earth have iso2 = "-99" meaning
-- no official ISO code has been assigned. Examples include
-- Kosovo and some disputed territories. These rows are still
-- stored but cannot be joined to dim_aircraft or dim_airport.
-- The seed script stores them as NULL rather than "-99".
--
-- DATA SOURCE
-- -----------
-- Loaded once by seeds/load_geodata.py from:
--   Natural Earth ne_110m_admin_0_countries.shp
--   https://www.naturalearthdata.com/downloads/110m-cultural-vectors/
--
-- REQUIRES
-- --------
-- PostGIS extension — enabled by 00_extensions.sql which
-- runs before this file. 
-- ============================================================


CREATE TABLE IF NOT EXISTS dim_country (

    -- ── Internal surrogate key ──────────────────────────────
    country_id          SERIAL PRIMARY KEY,

    -- ── Primary business keys ────────────────────────────────
    -- The 2-letter ISO country code.
    -- Example: "NG" for Nigeria, "AE" for UAE, "GB" for UK
    -- NULL is allowed for territories with no official ISO code.
    iso2                VARCHAR(2),

    -- The 3-letter ISO country code.
    -- Example: "NGA" for Nigeria, "ARE" for UAE, "GBR" for UK
    -- Used in some aviation datasets and international systems.
    iso3                VARCHAR(3),

    -- ── Country names ────────────────────────────────────────
    country_name        TEXT NOT NULL,

    -- The full official formal name of the country.
    -- Example: "Federal Republic of Nigeria"
    formal_name         TEXT,

    -- ── Geographic classification ────────────────────────────
    -- The continent this country belongs to.
    continent           TEXT,

    -- A more specific geographic grouping within the continent.
    -- Examples: "Western Africa", "Eastern Africa",
    -- Useful for regional filtering in the Gold layer dashboard.
    subregion           TEXT,

    -- ── Representative coordinates ───────────────────────────
    centroid_lon        NUMERIC(9, 6),

    -- The latitude of the best label placement point.
    centroid_lat        NUMERIC(9, 6),

    -- ── Population ───────────────────────────────────────────
    -- Estimated population of the country.
    -- Example: 218,541,212 for Nigeria (2019 estimate)
    -- Used as a contextual data point in Gold layer analysis.
    population          BIGINT,

    -- ── Geospatial boundary ──────────────────────────────────
    -- The full border polygon of the country stored as a
    -- PostGIS geometry object.
    --
    -- Can store both POLYGON (single landmass countries like
    -- Tanzania) and MULTIPOLYGON (island nations like Fiji
    -- or Indonesia that consist of multiple separate pieces).
    boundary            geometry(Geometry, 4326),

    -- ── Audit timestamp ──────────────────────────────────────
    created_at          TIMESTAMP DEFAULT NOW()

);


-- ============================================================
-- INDEXES
-- ============================================================

-- Used for joins from dim_aircraft and dim_airport
CREATE INDEX IF NOT EXISTS idx_dim_country_iso2
    ON dim_country(iso2);

-- Used for joins and lookups by 3-letter code
CREATE INDEX IF NOT EXISTS idx_dim_country_iso3
    ON dim_country(iso3);

-- Used for continent-level filtering in Gold layer
CREATE INDEX IF NOT EXISTS idx_dim_country_continent
    ON dim_country(continent);

-- Used for subregion-level filtering in Gold layer
CREATE INDEX IF NOT EXISTS idx_dim_country_subregion
    ON dim_country(subregion);

-- ── Spatial index ───────────────────────────────────────────
-- With a spatial index, PostGIS uses a
-- bounding box pre-filter to eliminate most countries
-- instantly before doing the precise polygon check.
--
-- GIST is the index type designed for geometric data.
CREATE INDEX IF NOT EXISTS idx_dim_country_boundary
    ON dim_country USING GIST(boundary);


-- ============================================================
-- CONFIRMATION
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE 'dim_country created — country reference table with PostGIS boundary ready.';
END $$;