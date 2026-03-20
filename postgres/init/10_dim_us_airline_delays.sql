-- ============================================================
-- 10_dim_us_airline_delays.sql
--
-- PURPOSE
-- -------
-- Stores US domestic airline on-time performance data sourced
-- from the Bureau of Transportation Statistics (BTS).
-- One row per flight.
--
-- This table serves two purposes:
--
-- 1. ML TRAINING DATA FOR US CARRIERS
--    Provides real departure and arrival delay values for
--    US airlines. The XGBoost delay prediction model uses
--    this data to learn delay patterns for US carriers —
--    which airlines delay most, on which routes, at which
--    airports, at which times of day and year.
--
-- 2. DELAY CAUSE ANALYSIS
--    The five delay cause columns (carrier, weather, NAS,
--    security, late aircraft) allow the Gold layer to break
--    down what percentage of delays are attributable to
--    each cause per airline and route.
--
-- SCOPE
-- -----
-- US domestic flights only — carriers like United, Delta,
-- American, Southwest, JetBlue etc.
-- Non-US carriers (Air Peace, Ethiopian, Emirates etc.) are
-- not in this dataset. For those carriers the ML model uses
-- computed features only — aircraft age, weight category,
-- congestion score, departure hour and route type.
--
-- DATA SOURCE
-- -----------
-- Bureau of Transportation Statistics Airline On-Time
-- Performance Database. Free download, no account needed.
-- URL: https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGJ
-- Loaded once by seeds/load_bts_delays.py
--
-- JOINS
-- -----
-- airline_iata     → dim_aircraft.airline_iata
-- origin_iata      → dim_airport.airport_iata_code
-- destination_iata → dim_airport.airport_iata_code
-- ============================================================


CREATE TABLE IF NOT EXISTS dim_us_airline_delays (

    -- ── Internal surrogate key ──────────────────────────────
    delay_id                SERIAL PRIMARY KEY,

    -- ── Flight identification ─────────────────────────────────
    -- The date this flight operated.
    -- Sourced from BTS FL_DATE column.
    -- Example: 2019-01-09
    flight_date             DATE NOT NULL,

    -- Full airline name.
    -- Example: "United Air Lines Inc."
    -- Sourced from BTS AIRLINE column.
    airline_name            TEXT,

    -- 2-letter IATA airline code.
    -- Example: "UA" for United, "DL" for Delta
    -- Used to join to dim_aircraft.airline_iata
    -- Sourced from BTS AIRLINE_CODE column.
    airline_iata            VARCHAR(5),

    -- The flight number operated by this airline on this date.
    -- Example: 1562 for United flight 1562
    -- Sourced from BTS FL_NUMBER column.
    flight_number           INTEGER,

    -- ── Route ────────────────────────────────────────────────
    -- Origin airport IATA code.
    -- Example: "FLL" for Fort Lauderdale
    -- Sourced from BTS ORIGIN column.
    origin_iata             VARCHAR(3),

    -- Destination airport IATA code.
    -- Example: "EWR" for Newark
    -- Sourced from BTS DEST column.
    destination_iata        VARCHAR(3),

    -- Route distance in miles.
    -- Example: 1065 miles from Fort Lauderdale to Newark
    -- Strong predictor of delay — longer routes have more
    -- opportunity to recover or accumulate delay.
    -- Sourced from BTS DISTANCE column.
    distance_miles          INTEGER,

    -- ── Scheduled times ──────────────────────────────────────
    -- Scheduled departure time in HHMM integer format.
    -- Example: 1155 means scheduled to depart at 11:55
    -- BTS stores times as integers not TIME values.
    -- Sourced from BTS CRS_DEP_TIME column.
    scheduled_dep_time      INTEGER,

    -- Scheduled arrival time in HHMM integer format.
    -- Example: 1501 means scheduled to arrive at 15:01
    -- Sourced from BTS CRS_ARR_TIME column.
    scheduled_arr_time      INTEGER,

    -- Scheduled flight duration in minutes.
    -- Example: 186 minutes = 3 hours 6 minutes
    -- Sourced from BTS CRS_ELAPSED_TIME column.
    scheduled_elapsed_min   INTEGER,

    -- ── Actual times ─────────────────────────────────────────
    -- Actual departure time in HHMM integer format.
    -- NULL if the flight was cancelled.
    -- Example: 1151 means actually departed at 11:51
    -- Sourced from BTS DEP_TIME column.
    actual_dep_time         INTEGER,

    -- Actual arrival time in HHMM integer format.
    -- NULL if the flight was cancelled or diverted.
    -- Sourced from BTS ARR_TIME column.
    actual_arr_time         INTEGER,

    -- Actual total elapsed time in minutes gate to gate.
    -- NULL if the flight was cancelled.
    -- Sourced from BTS ELAPSED_TIME column.
    actual_elapsed_min      INTEGER,

    -- Actual airborne time in minutes wheels off to wheels on.
    -- Different from elapsed time — does not include taxi time.
    -- NULL if the flight was cancelled.
    -- Sourced from BTS AIR_TIME column.
    air_time_min            INTEGER,

    -- ── Taxi times ───────────────────────────────────────────
    -- Time in minutes from gate pushback to wheels off.
    -- High taxi out times indicate airport congestion.
    -- Useful ML feature for the congestion score.
    -- Sourced from BTS TAXI_OUT column.
    taxi_out_min            INTEGER,

    -- Time in minutes from wheels on to gate arrival.
    -- Sourced from BTS TAXI_IN column.
    taxi_in_min             INTEGER,

    -- ── Delay values ─────────────────────────────────────────
    -- Departure delay in minutes.
    -- Negative = departed early. Example: -4 = 4 minutes early
    -- Positive = departed late.  Example: 25 = 25 minutes late
    -- NULL if the flight was cancelled.
    -- Sourced from BTS DEP_DELAY column.
    departure_delay_min     NUMERIC(6, 1),

    -- Arrival delay in minutes.
    -- This is the primary target variable for the ML model.
    -- Negative = arrived early.
    -- Positive = arrived late.
    -- NULL if the flight was cancelled or diverted.
    -- Sourced from BTS ARR_DELAY column.
    arrival_delay_min       NUMERIC(6, 1),

    -- ── Flight outcome ───────────────────────────────────────
    -- Whether the flight was cancelled.
    -- TRUE = cancelled, FALSE = operated
    -- Sourced from BTS CANCELLED column (0/1 → FALSE/TRUE).
    is_cancelled            BOOLEAN DEFAULT FALSE,

    -- The reason for cancellation if is_cancelled = TRUE.
    -- NULL if the flight was not cancelled.
    -- A = Carrier (airline's own fault)
    -- B = Weather
    -- C = National Air System (ATC, airport operations)
    -- D = Security
    -- Sourced from BTS CANCELLATION_CODE column.
    cancellation_code       VARCHAR(1),

    -- Whether the flight was diverted to a different airport.
    -- TRUE = diverted, FALSE = arrived at scheduled destination
    -- Sourced from BTS DIVERTED column (0/1 → FALSE/TRUE).
    is_diverted             BOOLEAN DEFAULT FALSE,

    -- ── Delay cause breakdown ────────────────────────────────
    -- These five columns break down the total delay by cause.
    -- All are NULL when arrival_delay_min <= 0 (no delay).
    -- The seed script converts NULL to 0 for non-delayed flights
    -- so the ML model can treat them as numeric features.
    -- All values are in minutes.

    -- Delay caused by the airline itself — late crew,
    -- aircraft maintenance, baggage loading etc.
    -- Sourced from BTS DELAY_DUE_CARRIER column.
    delay_carrier_min       NUMERIC(6, 1),

    -- Delay caused by weather conditions.
    -- Sourced from BTS DELAY_DUE_WEATHER column.
    delay_weather_min       NUMERIC(6, 1),

    -- Delay caused by the National Airspace System —
    -- air traffic control, airport operations, heavy traffic.
    -- Sourced from BTS DELAY_DUE_NAS column.
    delay_nas_min           NUMERIC(6, 1),

    -- Delay caused by security issues at the airport.
    -- Rare — usually zero.
    -- Sourced from BTS DELAY_DUE_SECURITY column.
    delay_security_min      NUMERIC(6, 1),

    -- Delay caused by the incoming aircraft arriving late
    -- from a previous flight on the same aircraft.
    -- Very common — a delay on flight 1 cascades to flight 2.
    -- Sourced from BTS DELAY_DUE_LATE_AIRCRAFT column.
    delay_late_aircraft_min NUMERIC(6, 1),

    -- ── Audit timestamp ──────────────────────────────────────
    created_at              TIMESTAMP DEFAULT NOW()

);


-- ============================================================
-- INDEXES
-- ============================================================

-- Join from gold_delay_predictions and ML feature queries
CREATE INDEX IF NOT EXISTS idx_bts_airline_iata
    ON dim_us_airline_delays(airline_iata);

-- Date range queries for training data selection
CREATE INDEX IF NOT EXISTS idx_bts_flight_date
    ON dim_us_airline_delays(flight_date);

-- Route-level analysis
CREATE INDEX IF NOT EXISTS idx_bts_origin
    ON dim_us_airline_delays(origin_iata);

CREATE INDEX IF NOT EXISTS idx_bts_destination
    ON dim_us_airline_delays(destination_iata);

-- Filter to delayed flights only for analysis
CREATE INDEX IF NOT EXISTS idx_bts_arrival_delay
    ON dim_us_airline_delays(arrival_delay_min);

-- Filter to cancelled flights
CREATE INDEX IF NOT EXISTS idx_bts_cancelled
    ON dim_us_airline_delays(is_cancelled);


-- ============================================================
-- CONFIRMATION
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE 'dim_us_airline_delays created — BTS on-time performance table ready.';
END $$;