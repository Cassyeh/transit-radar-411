-- gold_territory_heatmap.sql
--
-- PURPOSE
-- -------
-- Pre-aggregates territory crossing data from hist_territory_crossings
-- into a Gold layer table showing which countries and states see
-- the most air traffic.
--
-- WHAT IT PRODUCES
-- ----------------
-- One row per territory (country or state) containing:
--   - Total crossing count (all time)
--   - Crossing count per year
--   - Average time spent in this territory (minutes)
--   - Most common crossing role (START/ROUTE/END)
--   - Number of unique aircraft that crossed this territory
--   - Number of unique airlines that crossed this territory
--
-- Metabase uses this for:
--   - Choropleth map coloured by crossing count
--   - Bar chart of top 20 countries
--   - Bar chart of top 15 states
--   - Year-over-year trend lines
--
-- HOW IT RUNS
-- -----------
-- dbt run --select gold_territory_heatmap

{{ config(
    materialized = 'table',
    schema       = 'public'
) }}

WITH

-- Step 1: Join crossings to aircraft for airline information
crossings_enriched AS (
    SELECT
        tc.territory_type,
        tc.territory_id,
        tc.territory_name,
        tc.crossing_role,
        tc.entered_date,
        tc.duration_minutes,
        tc.icao24,
        EXTRACT(YEAR FROM tc.entered_date)::INT  AS crossing_year,
        d.airline_iata,
        d.airline_name,
        d.country_iso2                           AS aircraft_country
    FROM {{ source('public', 'hist_territory_crossings') }} tc
    LEFT JOIN {{ source('public', 'dim_aircraft') }} d
        ON d.icao24 = tc.icao24
    WHERE tc.duration_minutes IS NOT NULL
    AND   tc.duration_minutes > 0
),

-- Step 2: Aggregate by territory
territory_stats AS (
    SELECT
        territory_type,
        territory_id,
        territory_name,
        -- Overall counts
        COUNT(*)                                        AS total_crossings,
        COUNT(DISTINCT icao24)                          AS unique_aircraft,
        COUNT(DISTINCT airline_iata)                    AS unique_airlines,
        -- Year breakdowns
        COUNT(*) FILTER (WHERE crossing_year = 2019)   AS crossings_2019,
        COUNT(*) FILTER (WHERE crossing_year = 2020)   AS crossings_2020,
        COUNT(*) FILTER (WHERE crossing_year = 2021)   AS crossings_2021,
        COUNT(*) FILTER (WHERE crossing_year = 2022)   AS crossings_2022,
        -- Duration statistics
        ROUND(AVG(duration_minutes)::NUMERIC, 1)        AS avg_duration_min,
        ROUND(SUM(duration_minutes)::NUMERIC, 1)        AS total_duration_min,
        ROUND(MAX(duration_minutes)::NUMERIC, 1)        AS max_duration_min,
        -- Role breakdown
        COUNT(*) FILTER (WHERE crossing_role = 'START') AS start_crossings,
        COUNT(*) FILTER (WHERE crossing_role = 'ROUTE') AS route_crossings,
        COUNT(*) FILTER (WHERE crossing_role = 'END')   AS end_crossings,
        -- Most common aircraft origin country
        MODE() WITHIN GROUP (ORDER BY aircraft_country) AS top_aircraft_country,
        -- Most common airline
        MODE() WITHIN GROUP (ORDER BY airline_name)     AS top_airline,
        -- Date range
        MIN(entered_date)                               AS first_crossing_date,
        MAX(entered_date)                               AS last_crossing_date
    FROM crossings_enriched
    GROUP BY territory_type, territory_id, territory_name
),

-- Step 3: Add rank within each territory type
ranked AS (
    SELECT
        *,
        RANK() OVER (
            PARTITION BY territory_type
            ORDER BY total_crossings DESC
        ) AS rank_within_type
    FROM territory_stats
)

SELECT * FROM ranked
ORDER BY territory_type, rank_within_type
