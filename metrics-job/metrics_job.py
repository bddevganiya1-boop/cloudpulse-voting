"""
metrics_job.py

Spark Structured Streaming Job for CloudPulse Metrics Pipeline.

Consumes system metrics from Kafka topic `system-metrics-raw` and maintains
three real-time Elasticsearch views:
1. `metrics_raw`       - Individual metric readings for full-resolution history.
2. `metrics_windowed`  - 30-second tumbling window aggregations (avg, min, max) with flat window_start/window_end.
3. `metrics_anomalies` - Real-time alerts when CPU or Memory usage exceeds 90%.
"""

import argparse
import json
import logging
import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_format,
    from_json,
    lit,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    to_timestamp,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("metrics_job")

METRIC_SCHEMA = StructType([
    StructField("host", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("cpu_percent", DoubleType(), True),
    StructField("memory_percent", DoubleType(), True),
    StructField("disk_io_mb_per_sec", DoubleType(), True),
    StructField("network_io_mb_per_sec", DoubleType(), True),
    StructField("uptime_seconds", DoubleType(), True),
])


def write_bulk_to_es(records: list, es_host: str, index: str):
    """Writes a list of dictionaries to Elasticsearch via the _bulk REST API."""
    if not records:
        return
    bulk_lines = []
    for doc in records:
        bulk_lines.append(json.dumps({"index": {"_index": index}}))
        bulk_lines.append(json.dumps(doc, default=str))
    payload = "\n".join(bulk_lines) + "\n"

    try:
        resp = requests.post(
            f"http://{es_host}/_bulk",
            data=payload,
            headers={"Content-Type": "application/x-ndjson"},
            timeout=15,
        )
        if resp.status_code >= 300:
            log.warning("[es] Bulk write issue (%s): %d %s", index, resp.status_code, resp.text[:200])
        else:
            log.debug("[es] Wrote %d records to %s", len(records), index)
    except requests.RequestException as exc:
        log.error("[es] HTTP write failed (%s): %s", index, exc)


def process_micro_batch(batch_df, epoch_id, es_host: str):
    """Processes each streaming micro-batch to populate raw, windowed, and anomaly ES indices."""
    if batch_df.rdd.isEmpty():
        return

    log.info("--- Processing micro-batch %d ---", epoch_id)

    # 1. VIEW 1: metrics_raw (raw records formatted with ISO timestamp)
    raw_formatted_df = batch_df.select(
        col("host"),
        date_format(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("timestamp"),
        spark_round(col("cpu_percent"), 2).alias("cpu_percent"),
        spark_round(col("memory_percent"), 2).alias("memory_percent"),
        spark_round(col("disk_io_mb_per_sec"), 2).alias("disk_io_mb_per_sec"),
        spark_round(col("network_io_mb_per_sec"), 2).alias("network_io_mb_per_sec"),
        col("uptime_seconds"),
    )
    raw_records = [r.asDict() for r in raw_formatted_df.collect()]
    write_bulk_to_es(raw_records, es_host, "metrics_raw")
    log.info("Published %d raw metric events to metrics_raw", len(raw_records))

    # 2. VIEW 2: metrics_windowed (30-second tumbling window per host)
    windowed_df = (
        batch_df.groupBy(
            window(col("timestamp"), "30 seconds"),
            col("host"),
        )
        .agg(
            spark_round(avg("cpu_percent"), 2).alias("avg_cpu_percent"),
            spark_round(spark_min("cpu_percent"), 2).alias("min_cpu_percent"),
            spark_round(spark_max("cpu_percent"), 2).alias("max_cpu_percent"),
            spark_round(avg("memory_percent"), 2).alias("avg_memory_percent"),
            spark_round(spark_min("memory_percent"), 2).alias("min_memory_percent"),
            spark_round(spark_max("memory_percent"), 2).alias("max_memory_percent"),
            spark_round(avg("disk_io_mb_per_sec"), 2).alias("avg_disk_io_mb_per_sec"),
            spark_round(spark_min("disk_io_mb_per_sec"), 2).alias("min_disk_io_mb_per_sec"),
            spark_round(spark_max("disk_io_mb_per_sec"), 2).alias("max_disk_io_mb_per_sec"),
            spark_round(avg("network_io_mb_per_sec"), 2).alias("avg_network_io_mb_per_sec"),
            spark_round(spark_min("network_io_mb_per_sec"), 2).alias("min_network_io_mb_per_sec"),
            spark_round(spark_max("network_io_mb_per_sec"), 2).alias("max_network_io_mb_per_sec"),
            count(lit(1)).alias("sample_count"),
        )
        .select(
            col("host"),
            date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_start"),
            date_format(col("window.end"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("window_end"),
            col("avg_cpu_percent"),
            col("min_cpu_percent"),
            col("max_cpu_percent"),
            col("avg_memory_percent"),
            col("min_memory_percent"),
            col("max_memory_percent"),
            col("avg_disk_io_mb_per_sec"),
            col("min_disk_io_mb_per_sec"),
            col("max_disk_io_mb_per_sec"),
            col("avg_network_io_mb_per_sec"),
            col("min_network_io_mb_per_sec"),
            col("max_network_io_mb_per_sec"),
            col("sample_count"),
        )
    )
    windowed_records = [r.asDict() for r in windowed_df.collect()]
    write_bulk_to_es(windowed_records, es_host, "metrics_windowed")
    log.info("Published %d windowed metric summaries to metrics_windowed", len(windowed_records))

    # 3. VIEW 3: metrics_anomalies (cpu_percent > 90 OR memory_percent > 90)
    anomalies = []
    for w in windowed_records:
        if w.get("max_cpu_percent") is not None and w["max_cpu_percent"] > 90.0:
            val = w["max_cpu_percent"]
            anomalies.append({
                "host": w["host"],
                "metric_name": "cpu_percent",
                "value": val,
                "severity": "CRITICAL" if val >= 95.0 else "WARNING",
                "window_start": w["window_start"],
            })
        if w.get("max_memory_percent") is not None and w["max_memory_percent"] > 90.0:
            val = w["max_memory_percent"]
            anomalies.append({
                "host": w["host"],
                "metric_name": "memory_percent",
                "value": val,
                "severity": "CRITICAL" if val >= 95.0 else "WARNING",
                "window_start": w["window_start"],
            })

    if anomalies:
        write_bulk_to_es(anomalies, es_host, "metrics_anomalies")
        log.warning("Detected & published %d anomaly events to metrics_anomalies: %s", len(anomalies), anomalies)


def main():
    parser = argparse.ArgumentParser(description="CloudPulse Spark Structured Streaming Metrics Job")
    parser.add_argument("--kafka", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--topic", default=os.environ.get("METRICS_TOPIC", "system-metrics-raw"))
    parser.add_argument("--es-host", default=os.environ.get("ES_HOST", "elasticsearch:9200"))
    parser.add_argument("--checkpoint-dir", default="/tmp/cloudpulse-checkpoints/metrics")
    args = parser.parse_args()

    log.info("Starting CloudPulse Spark Metrics Job")
    log.info("Kafka: %s | Topic: %s | ES: %s", args.kafka, args.topic, args.es_host)

    spark = (
        SparkSession.builder
        .appName("CloudPulseMetricsPipeline")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Read stream from Kafka topic
    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON and cast timestamp
    parsed_df = (
        kafka_stream.select(from_json(col("value").cast("string"), METRIC_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("timestamp", to_timestamp(col("timestamp")))
        .filter(col("host").isNotNull() & col("timestamp").isNotNull())
    )

    # Process micro-batches via foreachBatch
    query = (
        parsed_df.writeStream
        .foreachBatch(lambda df, epoch_id: process_micro_batch(df, epoch_id, args.es_host))
        .option("checkpointLocation", args.checkpoint_dir)
        .start()
    )

    log.info("Streaming query started. Awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
