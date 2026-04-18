from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "flight_events",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

for message in consumer:
    event = message.value
    print(
        f"{event['event_type']:<12} | "
        f"{event['icao24']} | "
        f"{event.get('callsign', 'unknown'):>8} | "
        f"alt={event.get('altitude_ft', 0):>8,.0f}ft"
    )