"""
spark_consumer.py
--------------------
Real Spark Structured Streaming consumer — this is the piece that turns
"big data ingestion/storage/processing at scale" from an architecture
diagram into an actual distributed streaming job.

Reads the `weather-reports` Kafka topic as a micro-batch stream, applies
the same ML pipeline (ml/pipeline.py: event classification, fake-report
detection, dedup, IMD corroboration) to each micro-batch via
`foreachBatch`, and writes the processed results into the centralized
store. At real production scale you would swap `write_batch_to_sink()`'s
SQLite write for a Postgres/Cassandra/Elasticsearch sink — the streaming
topology and the ML step do not change.

Why foreachBatch + pandas UDF pattern instead of a pure-Spark ML pipeline:
our scikit-learn models are trained once and are small (TF-IDF + linear
models), so the fastest correct way to apply them inside Spark is to
collect each micro-batch to the driver as a pandas DataFrame, run the
existing WeatherMLPipeline unchanged, and write results back out. This
keeps exactly one ML implementation in the codebase (no duplicate
PySpark-native reimplementation of the same models) while still being a
genuine Spark Structured Streaming job for the ingestion/processing layer.

Requires: pip install pyspark kafka-python
Requires: Kafka running (docker compose up -d, from repo root) and
          kafka_producer.py publishing to the `weather-reports` topic.

Run:  python3 backend/streaming/spark_consumer.py
"""

import os
import sys
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "weather-reports"


def write_batch_to_sink(batch_df, batch_id):
    """Called once per Spark micro-batch. Runs the shared ML pipeline on
    every row in the batch, then writes processed rows into the same
    SQLite store the single-process demo uses (db.py) — swap this call
    for a Postgres/Elasticsearch writer to go from demo-scale to
    production-scale without touching anything upstream."""
    import db as dbmod
    from ml.pipeline import WeatherMLPipeline

    rows = [json.loads(r.value) for r in batch_df.select("value").collect()]
    if not rows:
        print(f"[spark_consumer] batch {batch_id}: empty, skipping")
        return

    ml = write_batch_to_sink._ml_singleton
    dbmod.init_db(reset=False)

    processed_count = 0
    for raw in rows:
        try:
            processed = ml.process(raw)
            dbmod.insert_report(processed)
            processed_count += 1
        except Exception as e:
            print(f"[spark_consumer] failed to process report {raw.get('report_id')}: {e}")

    dbmod.log_batch(processed_count, f"spark_batch_{batch_id}")
    print(f"[spark_consumer] batch {batch_id}: processed {processed_count}/{len(rows)} reports")


def run():
    from pyspark.sql import SparkSession
    from ml.pipeline import WeatherMLPipeline

    # Load the ML pipeline once on the driver and stash it on the batch
    # function so every micro-batch reuses the same fitted models instead
    # of re-loading from disk each time.
    write_batch_to_sink._ml_singleton = WeatherMLPipeline()

    spark = (SparkSession.builder
             .appName("WeatherPulseStreamingIngestion")
             .config("spark.jars.packages",
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    stream_df = (spark.readStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
                 .option("subscribe", TOPIC)
                 .option("startingOffsets", "latest")
                 .load())

    query = (stream_df.selectExpr("CAST(value AS STRING) as value")
             .writeStream
             .foreachBatch(write_batch_to_sink)
             .trigger(processingTime="5 seconds")
             .start())

    print(f"[spark_consumer] streaming from Kafka topic '{TOPIC}' @ {BOOTSTRAP_SERVERS} ...")
    query.awaitTermination()


if __name__ == "__main__":
    run()
