-- ============================================================
-- 11_gold_delay_predictions.sql
--
-- PURPOSE
-- -------
-- Stores the output of the XGBoost flight delay prediction
-- model. One row per flight prediction.
--
-- This is the Gold layer output table — the final product
-- of the entire pipeline. Every other table in the database
-- exists to produce the features that feed this table.
--
-- WHEN PREDICTIONS ARE MADE
-- -------------------------
-- A prediction is made at the exact moment a DEPARTED event
-- is detected for an aircraft. At that moment the pipeline
-- has everything it needs:
--   - Aircraft identity and age from dim_aircraft
--   - Departure airport from origin_iata
--   - Live congestion score from OpenSky states/all
--   - Departure hour from OpenSky flights endpoint
--   - Historical delay data from dim_us_airline_delays
--     (US carriers only)
--
-- The XGBoost model scores the flight and writes one row
-- here immediately. Metabase reads from this table and
-- updates the dashboard in near real-time.
--
-- PREDICTION TARGET
-- -----------------
-- The model predicts whether a flight will experience a
-- significant delay — defined as arriving more than 15
-- minutes late at its destination.
-- Output:
--   delay_probability    — probability of 15+ min delay
--   predicted_delay_class — ON_TIME, MINOR_DELAY, MAJOR_DELAY
--
-- CLASS THRESHOLDS
-- ----------------
-- ON_TIME      — delay_probability < 0.30
-- MINOR_DELAY  — delay_probability >= 0.30 and < 0.65
-- MAJOR_DELAY  — delay_probability >= 0.65
--
-- ML FEATURES USED
-- ----------------
-- aircraft_age_years      — older aircraft delay more
-- weight_category         — heavy aircraft have different profiles
-- icao_aircraft_type      — encodes engine count and type
-- departure_hour          — morning flights delay less
-- departure_day_of_week   — Fridays and Mondays delay more
-- origin_congestion_score — busy airports cause delays
-- is_international        — international flights have more
--                           complexity and delay risk
-- departure_delay_min     — US carriers only from BTS data,
--                           NULL for non-US carriers
-- airline_iata            — categorical feature per airline
--
-- FOREIGN KEYS
-- ------------
-- icao24           → dim_aircraft.icao24
-- origin_iata      → dim_airport.airport_iata_code
-- destination_iata → dim_airport.airport_iata_code
-- ============================================================


CREATE TABLE IF NOT EXISTS gold_delay_predictions (

    -- ── Internal surrogate key ──────────────────────────────
    prediction_id           SERIAL PRIMARY KEY,

    -- ── Flight identification ─────────────────────────────────
    -- The aircraft being predicted.
    -- Links to dim_aircraft for full aircraft context.
    icao24                  VARCHAR(10) NOT NULL,

    -- The radio callsign at time of prediction.
    -- Example: "UAL1736"
    callsign                VARCHAR(20),

    -- The date this flight is operating.
    -- Derived from the DEPARTED event timestamp.
    flight_date             DATE NOT NULL,

    -- ── Route ────────────────────────────────────────────────
    -- Departure airport IATA code.
    -- Assigned at DEPARTED event using nearest airport
    -- proximity query against dim_airport.
    -- Example: "LOS" for Lagos
    origin_iata             VARCHAR(10),

    -- Destination airport IATA code.
    -- NULL at prediction time — filled in by Silver layer
    -- after the flight lands using OpenSky flights endpoint.
    -- Example: "LHR" for London Heathrow
    destination_iata        VARCHAR(10),

    -- The 2-letter IATA airline code.
    -- Sourced from dim_aircraft at prediction time.
    -- Used as a categorical feature in the ML model.
    -- Also used to join to dim_us_airline_delays for US carriers.
    -- Example: "UA" for United, "EK" for Emirates
    airline_iata            VARCHAR(5),

    -- ── ML features stored at prediction time ────────────────
    -- These are the exact values fed into the XGBoost model
    -- when this prediction was made. Storing them here means
    -- you can always audit why the model made a specific
    -- prediction — even months later when the live data
    -- has changed.

    -- Aircraft age in years at time of prediction.
    -- Computed as: EXTRACT(YEAR FROM AGE(first_flight_date))
    -- from dim_aircraft. Rounded to 1 decimal place.
    -- Example: 8.5 means the aircraft is 8.5 years old.
    -- Older aircraft tend to have higher maintenance delay rates.
    aircraft_age_years      NUMERIC(5, 1),

    -- Weight category of the aircraft.
    -- Sourced directly from dim_aircraft.weight_category.
    -- Example: "Heavy (> 300000 lbs)", "Medium", "Light"
    -- Heavy aircraft have longer turnaround times.
    weight_category         TEXT,

    -- ICAO aircraft type code.
    -- Sourced from dim_aircraft.icao_aircraft_type.
    -- Example: "L2J" = Landplane, 2 engines, Jet
    -- The middle character encodes engine count — a key
    -- maintenance and reliability feature.
    icao_aircraft_type      VARCHAR(10),

    -- Local hour of departure at the origin airport.
    -- Range: 0–23
    -- Derived from OpenSky flights endpoint firstSeen
    -- timestamp converted to local airport time using
    -- dim_airport.timezone.
    -- Example: 14 means the flight departed at 2pm local time.
    -- Early morning flights (6-9am) delay less than
    -- evening flights (6-10pm) because delays accumulate
    -- throughout the day.
    departure_hour          INTEGER,

    -- Day of week the flight departed.
    -- 1 = Monday, 2 = Tuesday ... 7 = Sunday
    -- Derived from flight_date.
    -- Fridays and Sundays typically have higher delay rates
    -- due to peak passenger volumes.
    departure_day_of_week   INTEGER,

    -- Number of aircraft within 50km of the origin airport
    -- at the moment of the DEPARTED event.
    -- Sourced from the live OpenSky states/all response
    -- that triggered the DEPARTED event detection.
    -- Higher values indicate more congestion and higher
    -- delay risk.
    -- Example: 12 aircraft near Lagos at peak hour.
    origin_congestion_score NUMERIC(5, 2),

    -- Whether the flight crosses an international border.
    -- TRUE  = origin and destination are in different countries
    -- FALSE = domestic flight within same country
    -- Derived by comparing dim_airport.iso_country for
    -- origin and destination airports.
    -- International flights have more complexity — customs,
    -- immigration, longer turnarounds — and higher delay risk.
    is_international        BOOLEAN,

    -- Departure delay in minutes from BTS data.
    -- Only populated for US carriers where BTS data is available.
    -- NULL for all non-US carriers.
    -- When available this is a very strong predictor —
    -- a flight that departed late is very likely to arrive late.
    -- Sourced from dim_us_airline_delays joined on
    -- airline_iata + origin_iata + destination_iata + flight_date.
    departure_delay_min     NUMERIC(6, 1),

    -- ── Model output ─────────────────────────────────────────
    -- The probability that this flight will arrive more than
    -- 15 minutes late at its destination.
    -- Range: 0.0000 to 1.0000
    -- Example: 0.7823 = 78.23% chance of significant delay.
    -- Produced by XGBoost model predict_proba() method.
    delay_probability       NUMERIC(5, 4),

    -- Human-readable delay classification derived from
    -- delay_probability using fixed thresholds:
    --   ON_TIME      — delay_probability < 0.30
    --   MINOR_DELAY  — delay_probability >= 0.30 and < 0.65
    --   MAJOR_DELAY  — delay_probability >= 0.65
    -- This is what Metabase displays on the dashboard.
    predicted_delay_class   VARCHAR(20),

    -- The version identifier of the XGBoost model that
    -- produced this prediction.
    -- Example: "v1.0.0" for the initial model.
    -- Allows you to compare predictions across model versions
    -- and measure whether retraining improved accuracy.
    model_version           VARCHAR(20),

    -- ── Audit timestamps ─────────────────────────────────────
    -- When the XGBoost model made this prediction.
    -- This is the timestamp of the DEPARTED event that
    -- triggered the prediction.
    predicted_at            TIMESTAMP DEFAULT NOW(),

    -- When this row was inserted into the database.
    created_at              TIMESTAMP DEFAULT NOW(),

    -- ── Constraints ──────────────────────────────────────────
    -- Every prediction must link to a known aircraft.
    CONSTRAINT fk_predictions_aircraft
        FOREIGN KEY (icao24)
        REFERENCES dim_aircraft(icao24)
        ON DELETE RESTRICT,

    -- Predicted delay class must be one of three values.
    CONSTRAINT chk_predicted_delay_class
        CHECK (predicted_delay_class IN (
            'ON_TIME',
            'MINOR_DELAY',
            'MAJOR_DELAY'
        )),

    -- Delay probability must be between 0 and 1.
    CONSTRAINT chk_delay_probability
        CHECK (delay_probability >= 0 AND delay_probability <= 1),

    -- Departure hour must be a valid hour of the day.
    CONSTRAINT chk_departure_hour
        CHECK (departure_hour >= 0 AND departure_hour <= 23),

    -- Day of week must be between 1 and 7.
    CONSTRAINT chk_departure_day
        CHECK (departure_day_of_week >= 1 AND departure_day_of_week <= 7)

);


-- ============================================================
-- INDEXES
-- ============================================================

-- Most frequently queried — Metabase dashboard filters by date
CREATE INDEX IF NOT EXISTS idx_predictions_date
    ON gold_delay_predictions(flight_date);

-- Find all predictions for a specific aircraft
CREATE INDEX IF NOT EXISTS idx_predictions_icao24
    ON gold_delay_predictions(icao24);

-- Filter by predicted class for dashboard views
CREATE INDEX IF NOT EXISTS idx_predictions_class
    ON gold_delay_predictions(predicted_delay_class);

-- Filter by airline for airline-level analysis
CREATE INDEX IF NOT EXISTS idx_predictions_airline
    ON gold_delay_predictions(airline_iata);

-- Filter by origin for airport-level delay analysis
CREATE INDEX IF NOT EXISTS idx_predictions_origin
    ON gold_delay_predictions(origin_iata);

-- Filter by model version for model performance comparison
CREATE INDEX IF NOT EXISTS idx_predictions_model_version
    ON gold_delay_predictions(model_version);

-- Find high-probability delay predictions for alerting
CREATE INDEX IF NOT EXISTS idx_predictions_probability
    ON gold_delay_predictions(delay_probability);


-- ============================================================
-- CONFIRMATION
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE 'gold_delay_predictions created — ML predictions output table ready.';
    RAISE NOTICE '================================================';
    RAISE NOTICE 'All 11 tables created successfully.';
    RAISE NOTICE '  00_extensions.sql            — PostGIS enabled';
    RAISE NOTICE '  01_dim_aircraft.sql          — aircraft master';
    RAISE NOTICE '  02_dim_airport.sql           — airport reference';
    RAISE NOTICE '  03_dim_country.sql           — country boundaries';
    RAISE NOTICE '  04_dim_state.sql             — placeholder';
    RAISE NOTICE '  05_dim_landmark.sql          — placeholder';
    RAISE NOTICE '  06_daily_flight_events.sql   — daily transactions';
    RAISE NOTICE '  07_hist_flight_events.sql    — historical transactions';
    RAISE NOTICE '  08_daily_territory_crossings.sql — daily crossings';
    RAISE NOTICE '  09_hist_territory_crossings.sql  — historical crossings';
    RAISE NOTICE '  10_dim_us_airline_delays.sql — BTS delay data';
    RAISE NOTICE '  11_gold_delay_predictions.sql — ML predictions';
    RAISE NOTICE '================================================';
END $$;