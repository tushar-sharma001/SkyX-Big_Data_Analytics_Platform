"""
kafka_producer.py
-------------------
Real Kafka producer for the streaming architecture (see docker-compose.yml
at repo root). Polls the live connectors (open_meteo, imd_scraper, reddit,
twitter) plus replays the seed dataset for volume, and publishes every
report as a JSON message onto the `weather-reports` Kafka topic.

This is the literal "high-volume streaming ingestion" layer the problem
statement asks for: producer -> Kafka topic -> Spark Structured Streaming
consumer (spark_consumer.py) -> ML pipeline -> centralized store.

Requires: pip install kafka-python
Requires: Kafka running (docker compose up -d, from repo root)

Run:  python3 backend/streaming/kafka_producer.py
"""

import os
import sys
import json
import time
import random
import uuid
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

TOPIC = "weather-reports"
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def get_producer():
    from kafka import KafkaProducer
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=3,
    )


def collect_batch():
    """Pulls from real live connectors first, tops up with seed-data replay
    so there's always a meaningful stream volume even when live sources
    return nothing (e.g. off-hours, no credentials configured yet)."""
    import pandas as pd
    from connectors import fetch_all_live_reports

    batch = fetch_all_live_reports(verbose=False)

    seed_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                              "data", "weather_reports_seed.csv")
    if os.path.exists(seed_path) and len(batch) < 5:
        df = pd.read_csv(seed_path).sample(n=min(8, len(pd.read_csv(seed_path))))
        for _, row in df.iterrows():
            r = row.to_dict()
            r["report_id"] = str(uuid.uuid4())[:8]
            r["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            batch.append(r)
    return batch


def run(poll_interval_seconds: int = 10, iterations: int = None):
    producer = get_producer()
    print(f"[kafka_producer] connected to {BOOTSTRAP_SERVERS}, publishing to '{TOPIC}'")
    i = 0
    while iterations is None or i < iterations:
        batch = collect_batch()
        for report in batch:
            report.setdefault("report_id", str(uuid.uuid4())[:8])
            producer.send(TOPIC, value=report)
        producer.flush()
        print(f"[kafka_producer] published {len(batch)} reports (cycle {i+1})")
        i += 1
        if iterations is None or i < iterations:
            time.sleep(poll_interval_seconds)


if __name__ == "__main__":
    run()
