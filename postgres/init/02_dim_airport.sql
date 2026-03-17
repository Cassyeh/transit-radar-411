-- ============================================================
-- 02_dim_airport.sql
--
-- PURPOSE
-- -------
-- Airport reference table. One row per airport in the world.
--
-- Every flight event references an origin and destination
-- airport by its IATA code. This table provides the full
-- context behind that code — name, location, country,
-- region and whether it handles commercial flights.
--
-- The latitude and longitude columns are critical — they are
-- used by the geospatial territory detection logic to determine
-- which country or territory an airport sits inside.
--
-- PRIMARY KEY
-- -----------
-- airport_iata_code — the 3-letter IATA code assigned to
-- every commercial airport in the world.
-- Example: "LOS" for Murtala Muhammed International Lagos
--
-- NOTE: Not all airports have an IATA code. Military airfields,
-- small private strips and some regional airports only have
-- an ICAO code. Rows with no IATA code are still stored but
-- cannot be used as foreign keys from flight event tables.
--
-- DATA SOURCE
-- -----------
-- Loaded once by seeds/load_openflights.py using two files:
--   1. OurAirports airports.csv  — airport details
--      https://davidmegginson.github.io/ourairports-data/airports.csv
--   2. seeds/regions.csv         — region code to full name
--
-- The seed script joins these two files on iso_region = code
-- to resolve the full region name before inserting.
-- ============================================================


CREATE TABLE IF NOT EXISTS dim_airport (

    -- ── Internal surrogate key ──────────────────────────────
    airport_id          SERIAL PRIMARY KEY,

    -- ── OurAirports identifier ───────────────────────────────
    -- The unique identifier assigned by OurAirports to every
    -- airport in their database. 
    -- Example: "AAD" for Adado Airport
    ident               VARCHAR(10),

    -- ── Primary business keys ────────────────────────────────
    -- The 3-letter IATA code. 
    -- Example: "LOS", "LHR", "ADD", "JNB"
    -- NULL is allowed — not all airports have an IATA code.
    airport_iata_code   VARCHAR(3),

    -- The 4-letter ICAO code used in official aviation
    -- communications, flight plans and air traffic control.
    -- Example: "DNMM" for Lagos, "EGLL" for Heathrow
    airport_icao_code   VARCHAR(4),

    -- ── Airport details ──────────────────────────────────────
    -- Full official name of the airport.
    -- Example: "Murtala Muhammed International Airport"
    airport_name        TEXT,

    -- The category of airport.
    airport_type                VARCHAR(20),

    -- Whether this airport operates scheduled commercial flights.
    is_commercial       BOOLEAN DEFAULT FALSE,

    -- The city or town this airport primarily serves.
    -- Example: "Lagos", "London", "Addis Ababa"
    municipality        TEXT,

    -- ── Geographic and political context ─────────────────────
    -- The 2-letter ISO country code of the country this
    -- airport is located in.
    -- Example: "NG" for Nigeria, "GB" for United Kingdom
    iso_country         VARCHAR(2),

    -- The 2-letter continent code.
    continent           VARCHAR(2),

    -- The ISO 3166-2 region code within the country.
    -- Format: countrycode-regioncode
    -- Example: "SO-GA" for Galguduud region in Somalia
    iso_region          VARCHAR(10),

    -- The full human-readable name of the region.
    region_name         TEXT,

    -- ── Coordinates ──────────────────────────────────────────
    -- GPS latitude in decimal degrees.
    -- Positive = North, Negative = South
    latitude_deg        NUMERIC(9, 6),

    -- GPS longitude in decimal degrees.
    -- Positive = East, Negative = West
    longitude_deg       NUMERIC(9, 6),

    -- ── Audit timestamp ──────────────────────────────────────
    created_at          TIMESTAMP DEFAULT NOW()

);


-- ============================================================
-- INDEXES
-- ============================================================

-- Primary join column — every flight event references this
CREATE INDEX IF NOT EXISTS idx_dim_airport_iata
    ON dim_airport(airport_iata_code);

-- Used in aviation communication lookups
CREATE INDEX IF NOT EXISTS idx_dim_airport_icao
    ON dim_airport(airport_icao_code);

-- Used to join to dim_country and filter by country
CREATE INDEX IF NOT EXISTS idx_dim_airport_iso_country
    ON dim_airport(iso_country);

-- Used to filter to commercial airports only
CREATE INDEX IF NOT EXISTS idx_dim_airport_is_commercial
    ON dim_airport(is_commercial);

-- Used in continent-level analysis in the Gold layer
CREATE INDEX IF NOT EXISTS idx_dim_airport_continent
    ON dim_airport(continent);


-- ============================================================
-- CONFIRMATION
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE 'dim_airport created — airport reference table ready.';
END $$;