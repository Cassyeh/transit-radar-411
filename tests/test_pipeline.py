"""
Transit Radar 411 — Integration Tests
======================================
Tests cover:
  1. Rollup idempotency — running the EOD rollup twice produces no duplicate rows
  2. Territory detection — ST_Contains correctly identifies countries and states
     from real-world coordinates

Prerequisites:
  - Docker containers must be running (postgres)
  - dim_country and dim_state must be seeded
  - hist_flight_events must exist

Run with:
  pytest tests/test_pipeline.py -v
"""

import os
import pytest
import psycopg2
from datetime import datetime, timezone, date
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────
# DATABASE CONNECTION
# ─────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB", "radar_db"),
    "user":     os.getenv("POSTGRES_USER", "radar"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}


@pytest.fixture(scope="module")
def conn():
    """
    Module-scoped database connection.
    Shared across all tests in this file to avoid repeated reconnects.
    Rolled back and closed after all tests complete.
    """
    connection = psycopg2.connect(**DB_CONFIG)
    connection.autocommit = False
    yield connection
    connection.rollback()
    connection.close()


@pytest.fixture(scope="module")
def cursor(conn):
    cur = conn.cursor()
    yield cur
    cur.close()


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def insert_dummy_flight_event(cursor, icao24: str, event_timestamp: datetime) -> int:
    """
    Inserts a minimal dummy row into hist_flight_events and returns its event_id.
    Used to satisfy the FK constraint when testing territory crossings.
    """
    cursor.execute("""
        INSERT INTO hist_flight_events (
            icao24, callsign, event_type, event_timestamp, event_date,
            latitude, longitude, altitude_ft, velocity_kmh, heading_deg,
            vertical_rate_fpm, on_ground, source, inserted_at, batch_date, created_at
        ) VALUES (
            %s, 'TEST01', 'CRUISING', %s, %s,
            6.45, 3.39, 35000, 800, 90,
            0, false, 'test', NOW(), %s, NOW()
        )
        RETURNING event_id
    """, (icao24, event_timestamp, event_timestamp.date(), event_timestamp.date()))
    return cursor.fetchone()[0]


def get_hist_flight_count(cursor, icao24: str, event_timestamp: datetime) -> int:
    cursor.execute("""
        SELECT COUNT(*) FROM hist_flight_events
        WHERE icao24 = %s AND event_timestamp = %s AND event_type = 'CRUISING'
    """, (icao24, event_timestamp))
    return cursor.fetchone()[0]


def detect_country(cursor, lon: float, lat: float) -> str | None:
    """
    Replicates the ingestor's territory detection logic.
    Checks dim_state first (STATE takes priority), then dim_country.
    Returns territory name or None if not found.
    """
    # Check state first
    cursor.execute("""
        SELECT state_name FROM dim_state
        WHERE ST_Contains(boundary, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
        LIMIT 1
    """, (lon, lat))
    row = cursor.fetchone()
    if row:
        return row[0]

    # Fall back to country
    cursor.execute("""
        SELECT country_name FROM dim_country
        WHERE ST_Contains(boundary, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
        LIMIT 1
    """, (lon, lat))
    row = cursor.fetchone()
    return row[0] if row else None


def detect_country_only(cursor, lon: float, lat: float) -> str | None:
    """Checks dim_country only — used for country-level assertions."""
    cursor.execute("""
        SELECT country_name FROM dim_country
        WHERE ST_Contains(boundary, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
        LIMIT 1
    """, (lon, lat))
    row = cursor.fetchone()
    return row[0] if row else None


def detect_state_only(cursor, lon: float, lat: float) -> str | None:
    """Checks dim_state only — used for state-level assertions."""
    cursor.execute("""
        SELECT state_name FROM dim_state
        WHERE ST_Contains(boundary, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
        LIMIT 1
    """, (lon, lat))
    row = cursor.fetchone()
    return row[0] if row else None


# ─────────────────────────────────────────────────────────────
# 1. ROLLUP IDEMPOTENCY TESTS
# ─────────────────────────────────────────────────────────────

class TestRollupIdempotency:
    """
    Verifies that running the hist_flight_events INSERT twice for the same
    (icao24, event_timestamp, event_type) combination does not create duplicates.

    This tests the WHERE NOT EXISTS guard introduced to replace
    ON CONFLICT DO NOTHING (which conflicted on the BIGSERIAL PK only
    and therefore never prevented duplicates).
    """

    TEST_ICAO24 = "tstidm01"
    TEST_TIMESTAMP = datetime(2026, 4, 22, 12, 0, 0, tzinfo=timezone.utc)
    TEST_DATE = date(2026, 4, 22)

    ROLLUP_INSERT = """
        INSERT INTO hist_flight_events (
            icao24, callsign, event_type, event_timestamp, event_date,
            latitude, longitude, altitude_ft, velocity_kmh, heading_deg,
            vertical_rate_fpm, on_ground, source, inserted_at, batch_date, created_at
        )
        SELECT
            d.icao24, d.callsign, d.event_type, d.event_timestamp, d.event_date,
            d.latitude, d.longitude, d.altitude_ft, d.velocity_kmh, d.heading_deg,
            d.vertical_rate_fpm, d.on_ground, d.source, NOW(), %s, d.created_at
        FROM daily_flight_events d
        WHERE d.icao24 = %s
        AND NOT EXISTS (
            SELECT 1 FROM hist_flight_events h
            WHERE h.icao24          = d.icao24
            AND   h.event_timestamp = d.event_timestamp
            AND   h.event_type      = d.event_type
        )
    """

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, conn, cursor):
        """
        Inserts a dummy row into daily_flight_events before each test,
        and cleans up both daily and hist tables after.
        """
        try:
            # Insert dummy aircraft first to satisfy FK
            cursor.execute("""
                INSERT INTO dim_aircraft (icao24)
                VALUES (%s)
                ON CONFLICT (icao24) DO NOTHING
            """, (self.TEST_ICAO24,))

            cursor.execute("""
                INSERT INTO daily_flight_events (
                    icao24, callsign, event_type, event_timestamp, event_date,
                    latitude, longitude, altitude_ft, velocity_kmh, heading_deg,
                    vertical_rate_fpm, on_ground, source, created_at
                ) VALUES (
                    %s, 'TEST01', 'CRUISING', %s, %s,
                    6.45, 3.39, 35000, 800, 90,
                    0, false, 'test', NOW()
                )
            """, (self.TEST_ICAO24, self.TEST_TIMESTAMP, self.TEST_DATE))
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e

        yield
        conn.rollback()

        cursor.execute(
            "DELETE FROM hist_flight_events WHERE icao24 = %s", (self.TEST_ICAO24,)
        )
        cursor.execute(
            "DELETE FROM daily_flight_events WHERE icao24 = %s", (self.TEST_ICAO24,)
        )
        cursor.execute(
            "DELETE FROM dim_aircraft WHERE icao24 = %s", (self.TEST_ICAO24,)
        )
        conn.commit()

    def test_first_rollup_inserts_one_row(self, conn, cursor):
        """Running the rollup once inserts exactly one row."""
        cursor.execute(self.ROLLUP_INSERT, (self.TEST_DATE, self.TEST_ICAO24))
        conn.commit()

        count = get_hist_flight_count(cursor, self.TEST_ICAO24, self.TEST_TIMESTAMP)
        assert count == 1, (
            f"Expected 1 row after first rollup, got {count}"
        )

    def test_second_rollup_does_not_duplicate(self, conn, cursor):
        """Running the rollup twice produces the same row count as running it once."""
        # First run
        cursor.execute(self.ROLLUP_INSERT, (self.TEST_DATE, self.TEST_ICAO24))
        conn.commit()

        count_after_first = get_hist_flight_count(
            cursor, self.TEST_ICAO24, self.TEST_TIMESTAMP
        )

        # Second run — should insert 0 new rows
        cursor.execute(self.ROLLUP_INSERT, (self.TEST_DATE, self.TEST_ICAO24))
        conn.commit()

        count_after_second = get_hist_flight_count(
            cursor, self.TEST_ICAO24, self.TEST_TIMESTAMP
        )

        assert count_after_first == count_after_second == 1, (
            f"Duplicate detected: count went from {count_after_first} "
            f"to {count_after_second} on second rollup run"
        )

    def test_ten_rollups_still_one_row(self, conn, cursor):
        """
        Stress test: running the rollup 10 times (simulating repeated manual
        triggers during testing) still produces exactly one row.
        """
        for _ in range(10):
            cursor.execute(self.ROLLUP_INSERT, (self.TEST_DATE, self.TEST_ICAO24))
            conn.commit()

        count = get_hist_flight_count(cursor, self.TEST_ICAO24, self.TEST_TIMESTAMP)
        assert count == 1, (
            f"Expected 1 row after 10 rollup runs, got {count}. "
            f"WHERE NOT EXISTS guard is not working correctly."
        )


# ─────────────────────────────────────────────────────────────
# 2. TERRITORY DETECTION TESTS
# ─────────────────────────────────────────────────────────────

class TestTerritoryDetectionCountry:
    """
    Verifies that ST_Contains on dim_country correctly identifies
    countries from real-world coordinates.

    Coordinates sourced from known city centres.
    """

    def test_lagos_returns_nigeria(self, cursor):
        """
        Lagos, Nigeria (3.39°E, 6.45°N).
        This is the canonical test from the project setup —
        confirmed working during dim_country seed verification.
        """
        result = detect_country_only(cursor, lon=3.39, lat=6.45)
        assert result is not None, "No country found for Lagos coordinates"
        assert "Nigeria" in result, (
            f"Expected Nigeria for Lagos coordinates, got '{result}'"
        )

    def test_nairobi_returns_kenya(self, cursor):
        """Nairobi, Kenya (36.82°E, -1.29°N)."""
        result = detect_country_only(cursor, lon=36.82, lat=-1.29)
        assert result is not None, "No country found for Nairobi coordinates"
        assert "Kenya" in result, (
            f"Expected Kenya for Nairobi coordinates, got '{result}'"
        )

    def test_cairo_returns_egypt(self, cursor):
        """Cairo, Egypt (31.24°E, 30.06°N)."""
        result = detect_country_only(cursor, lon=31.24, lat=30.06)
        assert result is not None, "No country found for Cairo coordinates"
        assert "Egypt" in result, (
            f"Expected Egypt for Cairo coordinates, got '{result}'"
        )

    def test_johannesburg_returns_south_africa(self, cursor):
        """Johannesburg, South Africa (28.04°E, -26.20°N)."""
        result = detect_country_only(cursor, lon=28.04, lat=-26.20)
        assert result is not None, "No country found for Johannesburg coordinates"
        assert "South Africa" in result, (
            f"Expected South Africa for Johannesburg coordinates, got '{result}'"
        )

    def test_mid_atlantic_returns_none(self, cursor):
        """
        A point in the middle of the Atlantic Ocean (-30°E, 0°N).
        Should return None — no country boundary contains open ocean.
        """
        result = detect_country_only(cursor, lon=-30.0, lat=0.0)
        assert result is None, (
            f"Expected None for mid-Atlantic coordinates, got '{result}'"
        )


class TestTerritoryDetectionState:
    """
    Verifies that ST_Contains on dim_state correctly identifies
    states and provinces from real-world coordinates.

    Uses the same priority logic as the ingestor:
    STATE is checked before COUNTRY when both boundaries overlap.
    """

    def test_lagos_coords_return_lagos_state(self, cursor):
        """
        Lagos State centroid (3.967°E, 6.536°N) should resolve to
        Lagos State, not just Nigeria, because dim_state is checked first.
        """
        result = detect_state_only(cursor, lon=3.967, lat=6.536)
        assert result is not None, (
            "No state found for Lagos coordinates — check dim_state is seeded correctly"
        )
        assert "Lagos" in result, (
            f"Expected Lagos State for Lagos coordinates, got '{result}'"
        )

    def test_abuja_returns_fct(self, cursor):
        """
        Abuja (7.49°E, 9.07°N) — Federal Capital Territory.
        """
        result = detect_state_only(cursor, lon=7.49, lat=9.07)
        assert result is not None, "No state found for Abuja coordinates"
        assert any(
            keyword in result for keyword in ["Federal Capital", "FCT", "Abuja"]
        ), f"Expected FCT/Abuja for Abuja coordinates, got '{result}'"

    def test_kano_returns_kano_state(self, cursor):
        """Kano city centre (8.52°E, 12.00°N) — Kano State, Nigeria."""
        result = detect_state_only(cursor, lon=8.52, lat=12.00)
        assert result is not None, "No state found for Kano coordinates"
        assert "Kano" in result, (
            f"Expected Kano State for Kano coordinates, got '{result}'"
        )

    def test_state_takes_priority_over_country(self, cursor):
        """
        The ingestor writes STATE rows when a state is found, not COUNTRY rows.
        This test verifies that for Lagos coordinates, a state IS found —
        confirming that the priority logic would write a STATE row.
        """
        state_result  = detect_state_only(cursor, lon=3.967, lat=6.536)
        country_result = detect_country_only(cursor, lon=3.967, lat=6.536)

        # Both should find something
        assert country_result is not None, "dim_country returned None for Lagos"
        assert state_result is not None,   "dim_state returned None for Lagos"

        # State result should be more specific than country
        assert "Lagos" in state_result, (
            f"State detection should return Lagos State, got '{state_result}'"
        )
        assert "Nigeria" in country_result, (
            f"Country detection should return Nigeria, got '{country_result}'"
        )