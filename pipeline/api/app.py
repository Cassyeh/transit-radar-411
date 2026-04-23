"""
pipeline/api/app.py

PURPOSE
-------
A lightweight Flask REST API that serves live aircraft position
data to the Leaflet.js map dashboard.

One endpoint:
    GET /api/positions  — returns latest position of every
                          airborne aircraft as JSON

TWO DATA SOURCES (in priority order)
-------------------------------------
1. Kafka consumer cache (primary)
   A background thread subscribes to the flight_events Kafka
   topic and keeps an in-memory dictionary of the latest
   known state for every aircraft. When the map requests
   positions it reads from this dictionary instantly —
   no database query needed.

2. PostgreSQL fallback (when Kafka unavailable)
   If Kafka is not running the API falls back to querying
   daily_flight_events directly for the most recent position
   of each airborne aircraft.

CORS
----
Cross-Origin Resource Sharing is enabled so the HTML file
can call this API even when opened directly from disk
(file:// protocol) without a web server.

HOW TO RUN
----------
    pip install flask flask-cors kafka-python psycopg2-binary

    In one terminal — run the ingestor:
        python ingestion/opensky_ingestor.py

    In another terminal — run the API:
        python pipeline/api/app.py

    Open the map:
        dashboard/live_map.html

The API runs at http://localhost:5000
The map fetches from http://localhost:5000/api/positions
"""

import os
import sys
import json
import logging
import threading
import psycopg2
from datetime import datetime, timezone
from flask import Flask, jsonify
from flask_cors import CORS
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB"),
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC  = "flight_events"

# ─────────────────────────────────────────────────────────────
# IN-MEMORY AIRCRAFT CACHE
# ─────────────────────────────────────────────────────────────
# Keyed by icao24 — stores the latest known state of each
# aircraft. Updated by the Kafka consumer thread in real time.
# Read by the /api/positions endpoint on every map refresh.
aircraft_cache: dict = {}
cache_lock = threading.Lock()
kafka_available = False


# ─────────────────────────────────────────────────────────────
# KAFKA CONSUMER THREAD
# ─────────────────────────────────────────────────────────────
def start_kafka_consumer():
    """
    Runs in a background thread. Subscribes to the flight_events
    Kafka topic and updates aircraft_cache with every message.

    Each message is one flight event published by opensky_ingestor.py.
    We keep only the latest message per icao24 — older positions
    are overwritten as new ones arrive.

    The thread runs forever. If Kafka disconnects it retries
    every 30 seconds rather than crashing the API.
    """
    global kafka_available
    import time

    while True:
        try:
            from kafka import KafkaConsumer
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="latest",     # Only read new messages
                enable_auto_commit=True,
                group_id="live_map_consumer",
                consumer_timeout_ms=5000,        # Don't block forever
                api_version=(2, 8, 0)
            )

            kafka_available = True
            log.info(f"  Kafka consumer connected to {KAFKA_BROKER}")

            for message in consumer:
                event = message.value
                icao24 = event.get("icao24")
                if not icao24:
                    continue

                # Only cache airborne aircraft
                if event.get("on_ground"):
                    with cache_lock:
                        aircraft_cache.pop(icao24, None)
                    continue

                # Update cache with latest position
                with cache_lock:
                    aircraft_cache[icao24] = {
                        "icao24":      icao24,
                        "callsign":    event.get("callsign") or icao24,
                        "latitude":    event.get("latitude"),
                        "longitude":   event.get("longitude"),
                        "altitude_ft": event.get("altitude_ft"),
                        "velocity_kmh": event.get("velocity_kmh"),
                        "heading_deg": event.get("heading_deg"),
                        "event_type":  event.get("event_type"),
                        "updated_at":  event.get("event_timestamp"),
                        "source":      "kafka"
                    }

        except Exception as e:
            kafka_available = False
            log.warning(f"  Kafka consumer error: {e}. Retrying in 30s...")
            import time
            time.sleep(30)


# ─────────────────────────────────────────────────────────────
# POSTGRESQL FALLBACK
# ─────────────────────────────────────────────────────────────
def fetch_positions_from_postgres() -> list[dict]:
    """
    Queries daily_flight_events for the most recent position
    of every airborne aircraft today.

    Uses DISTINCT ON (icao24) ORDER BY event_timestamp DESC
    which is a PostgreSQL-specific way to get the latest row
    per group efficiently — no subquery needed.

    Called only when Kafka is unavailable.
    """
    try:
        conn   = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT ON (icao24)
                icao24,
                callsign,
                latitude,
                longitude,
                altitude_ft,
                velocity_kmh,
                heading_deg,
                event_type,
                event_timestamp
            FROM daily_flight_events
            WHERE event_date  = CURRENT_DATE
            AND   latitude    IS NOT NULL
            AND   longitude   IS NOT NULL
            AND   on_ground   = FALSE
            ORDER BY icao24, event_timestamp DESC
        """)
        rows    = cursor.fetchall()
        columns = [
            "icao24", "callsign", "latitude", "longitude",
            "altitude_ft", "velocity_kmh", "heading_deg",
            "event_type", "updated_at"
        ]
        cursor.close()
        conn.close()

        result = []
        for row in rows:
            d = dict(zip(columns, row))
            d["callsign"] = d["callsign"] or d["icao24"]
            d["source"]   = "postgres"
            # Convert datetime to ISO string for JSON
            if d["updated_at"]:
                d["updated_at"] = d["updated_at"].isoformat()
            result.append(d)

        return result

    except Exception as e:
        log.error(f"  PostgreSQL fallback error: {e}")
        return []


# ─────────────────────────────────────────────────────────────
# API ENDPOINTS
# ─────────────────────────────────────────────────────────────
@app.route("/api/positions")
def get_positions():
    """
    Returns the latest position of every airborne aircraft
    as a JSON array.

    The map calls this endpoint every 10 seconds and updates
    aircraft markers based on the response.

    Response format:
    {
        "count": 42,
        "source": "kafka",
        "timestamp": "2026-04-18T12:00:00+00:00",
        "aircraft": [
            {
                "icao24": "aa8c39",
                "callsign": "UAL1736",
                "latitude": 6.45,
                "longitude": 3.39,
                "altitude_ft": 35000.0,
                "velocity_kmh": 890.0,
                "heading_deg": 270.0,
                "event_type": "CRUISING",
                "updated_at": "2026-04-18T12:00:00+00:00",
                "source": "kafka"
            },
            ...
        ]
    }
    """
    if kafka_available and aircraft_cache:
        with cache_lock:
            aircraft = [
                v for v in aircraft_cache.values()
                if v.get("latitude") and v.get("longitude")
            ]
        source = "kafka"
    else:
        aircraft = fetch_positions_from_postgres()
        source   = "postgres"

    return jsonify({
        "count":     len(aircraft),
        "source":    source,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "aircraft":  aircraft
    })


@app.route("/api/health")
def health():
    """
    Health check endpoint. Returns API status and data source.
    Useful for confirming the API is running before opening the map.
    """
    return jsonify({
        "status":          "ok",
        "kafka_available": kafka_available,
        "cached_aircraft": len(aircraft_cache),
        "timestamp":       datetime.now(timezone.utc).isoformat()
    })


@app.route("/api/stats")
def stats():
    """
    Returns summary statistics from daily_flight_events.
    Used by the dashboard stat cards.
    """
    try:
        conn   = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                COUNT(DISTINCT icao24)          AS unique_aircraft,
                COUNT(*)                        AS total_events,
                SUM(CASE WHEN event_type = 'DEPARTED' THEN 1 ELSE 0 END) AS departures,
                SUM(CASE WHEN event_type = 'LANDED'   THEN 1 ELSE 0 END) AS landings,
                SUM(CASE WHEN event_type = 'CRUISING' THEN 1 ELSE 0 END) AS cruising,
                ROUND(
                    100.0 * COUNT(origin_iata) / NULLIF(COUNT(*), 0), 1
                ) AS enrichment_pct
            FROM daily_flight_events
            WHERE event_date = CURRENT_DATE
        """)
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        return jsonify({
            "unique_aircraft":  row[0] or 0,
            "total_events":     row[1] or 0,
            "departures":       row[2] or 0,
            "landings":         row[3] or 0,
            "cruising":         row[4] or 0,
            "enrichment_pct":   float(row[5] or 0)
        })
    except Exception as e:
        log.error(f"  Stats error: {e}")
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("=" * 60)
    log.info("  Transit Radar 411 — Live Map API")
    log.info("=" * 60)
    log.info(f"  Kafka broker : {KAFKA_BROKER}")
    log.info(f"  API port     : 5000")
    log.info(f"  Endpoints    : /api/positions")
    log.info(f"                 /api/health")
    log.info(f"                 /api/stats")

    # Start Kafka consumer in background thread
    # daemon=True means it stops when the main process stops
    kafka_thread = threading.Thread(
        target=start_kafka_consumer,
        daemon=True,
        name="kafka-consumer"
    )
    kafka_thread.start()
    log.info("  Kafka consumer thread started.")

    # Start Flask API
    # debug=False in production — True shows detailed errors
    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False
    )