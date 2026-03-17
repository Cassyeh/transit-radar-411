-- ============================================================
-- 01_dim_aircraft.sql
--
-- PURPOSE
-- -------
-- This is the "aircraft table" — the most important reference
-- table in the entire pipeline. One row per aircraft in the world.
--
-- PRIMARY KEY
-- -----------
-- icao24 — a permanent 6-character hexadecimal code assigned to
-- every aircraft in the world by aviation authorities. It never
-- changes for the lifetime of the aircraft.
-- Example: "0101be" is a Boeing 737 operated by Air Peace Nigeria.
--
-- DATA SOURCE
-- -----------
-- Loaded once by seeds/load_openflights.py using three files:
--   1. OpenSky aircraftDatabase.csv  — aircraft details
--   2. seeds/aircraft_reg-prefixes.csv        — prefix to ISO2 mapping
--   3. seeds/countries_code.csv           — ISO2 to country name

-- ============================================================


CREATE TABLE IF NOT EXISTS dim_aircraft (

    -- ── Internal surrogate key ──────────────────────────────
    aircraft_id         SERIAL PRIMARY KEY,

    -- ── The primary business key ────────────────────────────
    icao24              VARCHAR(10) UNIQUE NOT NULL,

    -- ── Aircraft identity ───────────────────────────────────
    -- The tail number painted on the outside of the aircraft.
    registration        VARCHAR(20),

    -- ── Manufacturer and model ──────────────────────────────
    -- TEXT is used instead of VARCHAR here because manufacturer
    -- names have no predictable maximum length.
    manufacturername        TEXT,

    -- The specific aircraft model.
    model               TEXT,

    -- ICAO type designator
    type_code   TEXT,

    -- ICAO aircraft type code — encodes three pieces of
    -- information in three characters.
    -- Format: [engine location][engine count][engine type]
    icao_aircraft_type     VARCHAR(10),

    -- The weight category of the aircraft.
    -- Example: "Heavy (> 300000 lbs)", "Medium", "Light"
    weight_category         TEXT,

    -- ── Airline information ─────────────────────────────────
    -- The full name of the airline operating this aircraft.
    -- Example: "Air Peace", "Ethiopian Airlines", "Emirates"
    airline_name        TEXT,

    -- Operator callsign used in live tracking
    -- The 2-letter IATA airline code.
    -- Example: "P4" for Air Peace, "ET" for Ethiopian Airlines
    -- The 3-letter ICAO airline code.
    operatorcallsign    TEXT,
    airline_icao        TEXT,
    airline_iata        VARCHAR(5),

    -- The country code where this aircraft is registered.
    -- Example: "NG", "AE"
    country_iso2        VARCHAR(2),
    -- The full human-readable country name.
    country_of_reg     TEXT,

    -- Engine field is a useful ML feature, engine type affects delays
    engines     TEXT,

    -- ── Aircraft age proxy ──────────────────────────────────
    -- When this aircraft first flew. 
    first_flight_date   DATE,

    -- ── Status flag ─────────────────────────────────────────
    -- TRUE means the aircraft is currently active in service.
    -- FALSE means it has been retired or decommissioned.
    -- DEFAULT TRUE because any aircraft we seed is assumed active.
    is_active           BOOLEAN DEFAULT TRUE,

    -- ── Audit timestamps ────────────────────────────────────
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW()

);


-- ============================================================
-- INDEXES
--
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_dim_aircraft_icao24
    ON dim_aircraft(icao24);

CREATE INDEX IF NOT EXISTS idx_dim_aircraft_airline
    ON dim_aircraft(airline_iata);

-- Used to join to dim_country and filter by country
CREATE INDEX IF NOT EXISTS idx_dim_aircraft_country_iso2
    ON dim_aircraft(country_iso2);


-- ============================================================
-- CONFIRMATION MESSAGE
-- This prints to the PostgreSQL logs when the file runs
-- successfully on first boot.
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE 'dim_aircraft table created successfully.';
END $$;