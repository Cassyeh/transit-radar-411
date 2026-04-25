-- gold_route_frequency.sql
--
-- PURPOSE
-- -------
-- Pre-aggregates the busiest flight routes from hist_flight_events
-- into a Gold layer table that Metabase queries instantly.
--
-- Without this model Metabase would run a GROUP BY on millions
-- of rows every time the Route Frequency dashboard loads.
-- With this model Metabase reads a pre-built summary — instant.
--
-- WHAT IT PRODUCES
-- ----------------
-- One row per unique origin-destination pair containing:
--   - Total flight count (all time)
--   - Flight count per year (2019, 2020, 2021, 2022)
--   - Average flight duration in minutes
--   - Most common airline on this route
--   - Whether this is an international route
--
-- HOW IT RUNS
-- -----------
-- dbt run --select gold_route_frequency
--
-- SCHEDULE
-- --------
-- Runs once per day after the EOD rollup completes.
-- Add to Airflow as a task after eod_rollup_dag truncation.

{{ config(
    materialized = 'table',
    schema       = 'public'
) }}

WITH

-- Step 1: Get all DEPARTED events with valid route information
departed AS (
    SELECT
        h.icao24,
        h.callsign,
        h.origin_iata,
        h.destination_iata,
        h.event_date,
        h.event_timestamp          AS departed_at,
        d.airline_name,
        d.airline_iata,
        d.country_iso2             AS aircraft_country
    FROM {{ source('public', 'hist_flight_events') }} h
    LEFT JOIN {{ source('public', 'dim_aircraft') }} d
        ON d.icao24 = h.icao24
    WHERE h.event_type        = 'DEPARTED'
    AND   h.origin_iata       IS NOT NULL
    AND   h.destination_iata  IS NOT NULL
),

-- Step 2: Join to LANDED events to get flight duration
with_duration AS (
    SELECT
        d.origin_iata,
        d.destination_iata,
        d.event_date,
        d.airline_name,
        d.airline_iata,
        d.aircraft_country,
        EXTRACT(YEAR FROM d.event_date)::INT  AS flight_year,
        -- Duration in minutes from departure to landing
        EXTRACT(EPOCH FROM (l.event_timestamp - d.departed_at)) / 60
            AS duration_minutes
    FROM departed d
    LEFT JOIN {{ source('public', 'hist_flight_events') }} l
        ON  l.icao24     = d.icao24
        AND l.event_type = 'LANDED'
        AND l.event_date = d.event_date
        AND l.source     = 'opensky_historical'
),

-- Step 3: Get the origin and destination country for each airport
with_countries AS (
    SELECT
        w.*,
        orig.iso_country  AS origin_country,
        dest.iso_country  AS destination_country
    FROM with_duration w
    LEFT JOIN {{ source('public', 'dim_airport') }} orig
        ON orig.airport_iata_code = w.origin_iata
    LEFT JOIN {{ source('public', 'dim_airport') }} dest
        ON dest.airport_iata_code = w.destination_iata
),

-- Step 4: Aggregate by route
aggregated AS (
    SELECT
        origin_iata,
        destination_iata,
        origin_country,
        destination_country,
        -- International if origin and destination countries differ
        CASE
            WHEN origin_country != destination_country THEN TRUE
            ELSE FALSE
        END                                          AS is_international,
        COUNT(*)                                     AS total_flights,
        -- Year breakdowns
        COUNT(*) FILTER (WHERE flight_year = 2019)  AS flights_2019,
        COUNT(*) FILTER (WHERE flight_year = 2020)  AS flights_2020,
        COUNT(*) FILTER (WHERE flight_year = 2021)  AS flights_2021,
        COUNT(*) FILTER (WHERE flight_year = 2022)  AS flights_2022,
        -- Duration stats
        ROUND(AVG(duration_minutes)::NUMERIC, 1)    AS avg_duration_min,
        ROUND(MIN(duration_minutes)::NUMERIC, 1)    AS min_duration_min,
        ROUND(MAX(duration_minutes)::NUMERIC, 1)    AS max_duration_min,
        -- Most common airline on this route
        MODE() WITHIN GROUP (ORDER BY airline_name) AS primary_airline,
        MODE() WITHIN GROUP (ORDER BY airline_iata) AS primary_airline_iata,
        -- Date range
        MIN(event_date)                              AS first_seen_date,
        MAX(event_date)                              AS last_seen_date,
        -- Friendly route label for Metabase display
        origin_iata || ' → ' || destination_iata    AS route_label
    FROM with_countries
    GROUP BY
        origin_iata, destination_iata,
        origin_country, destination_country
),

-- Step 5: Add rank by total flight count
ranked AS (
    SELECT
        *,
        RANK() OVER (ORDER BY total_flights DESC) AS route_rank
    FROM aggregated
    WHERE total_flights >= 2    -- Filter noise — routes with only 1 flight
)

SELECT * FROM ranked
ORDER BY route_rank
