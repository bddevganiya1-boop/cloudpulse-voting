"""
vote_aggregator.py

Spark Structured Streaming job — the analytical core of CloudPulse Voting.

Reads the `votes-raw` Kafka topic (published by vote-producer) and
maintains THREE views of the data in Elasticsearch, each serving a
different purpose on the dashboard:

  1. votes_raw        — every individual vote event, full audit trail.
  2. votes_windowed    — votes per option per 30-second window: shows the
                         *rate* of voting over time (a time series).
  3. vote_totals       — ONE document per option, holding the current
                         running total — a true live leaderboard.

This mirrors a realistic Big Data pattern: an OLTP system (Postgres) feeds
a streaming layer (Kafka + Spark) which maintains both historical/audit
data and real-time aggregate state for a live dashboard.
"""

import argparse
import datetime
import json

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, max as spark_max
from pyspark.sql.types import StringType, StructType, StructField, BooleanType

ARTICLE_SCHEMA = StructType([
    StructField("voter_id", StringType()),
    StructField("vote", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("is_change", BooleanType()),
])

WINDOW_DURATION = "30 seconds"


def clean_row_for_es(row_dict):
    """Ensures datetime objects are serialized as ISO 8601 strings for Elasticsearch."""
    cleaned = {}
    for k, v in row_dict.items():
        if isinstance(v, (datetime.datetime, datetime.date)):
            cleaned[k] = v.isoformat()
        else:
            cleaned[k] = v
    return cleaned


def write_batch_to_es(df, es_host: str, index: str):
    rows = [r.asDict(recursive=True) for r in df.collect()]
    if not rows:
        return
    bulk_lines = []
    for row in rows:
        bulk_lines.append(json.dumps({"index": {"_index": index}}))
        bulk_lines.append(json.dumps(clean_row_for_es(row), default=str))
    payload = "\n".join(bulk_lines) + "\n"
    try:
        resp = requests.post(
            f"http://{es_host}/_bulk?refresh=true",
            data=payload,
            headers={"Content-Type": "application/x-ndjson"},
            timeout=10,
        )
        if resp.status_code >= 300:
            print(f"[es] bulk write issue ({index}): {resp.status_code} {resp.text[:200]}")
    except requests.RequestException as exc:
        print(f"[es] write failed ({index}): {exc}")


def upsert_running_totals(df, es_host: str):
    """Overwrites each vote option's single document in `vote_totals` with
    the CURRENT CUMULATIVE count Spark has computed for it. Document id = option."""
    rows = df.collect()
    if not rows:
        return

    bulk_lines = []
    for row in rows:
        option = row["vote"]
        action = {"index": {"_index": "vote_totals", "_id": option}}
        last_updated = row["latest_event_timestamp"]
        if isinstance(last_updated, (datetime.datetime, datetime.date)):
            last_updated = last_updated.isoformat()

        body = {
            "option": option,
            "count": int(row["total_count"]),
            "last_updated": str(last_updated),
        }
        bulk_lines.append(json.dumps(action))
        bulk_lines.append(json.dumps(body))

    payload = "\n".join(bulk_lines) + "\n"
    try:
        resp = requests.post(
            f"http://{es_host}/_bulk?refresh=true",
            data=payload,
            headers={"Content-Type": "application/x-ndjson"},
            timeout=10,
        )
        if resp.status_code >= 300:
            print(f"[es] bulk write issue (totals): {resp.status_code} {resp.text[:200]}")
    except requests.RequestException as exc:
        print(f"[es] write failed (totals): {exc}")


def process_raw_batch(df, epoch_id, es_host):
    if df.rdd.isEmpty():
        return
    write_batch_to_es(df, es_host, "votes_raw")


def process_windowed_batch(df, epoch_id, es_host):
    if df.rdd.isEmpty():
        return

    flat = (
        df.withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
    )
    write_batch_to_es(flat, es_host, "votes_windowed")


def process_totals_batch(df, epoch_id, es_host):
    if df.rdd.isEmpty():
        return
    upsert_running_totals(df, es_host)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka", default="kafka:9092")
    parser.add_argument("--es-host", default="elasticsearch:9200")
    parser.add_argument("--checkpoint-dir", default="/tmp/cpv-checkpoints")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("CloudPulseVoteAggregator").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka)
        .option("subscribe", "votes-raw")
        .option("startingOffsets", "latest")
        .load()
    )

    votes = (
        raw.select(from_json(col("value").cast("string"), ARTICLE_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", col("event_timestamp").cast("timestamp"))
    )

    # 1. Full audit trail -> votes_raw
    raw_query = (
        votes.writeStream
        .trigger(processingTime="100 milliseconds")
        .foreachBatch(lambda df, eid: process_raw_batch(df, eid, args.es_host))
        .option("checkpointLocation", f"{args.checkpoint_dir}/raw")
        .start()
    )

    # 2. Voting rate over time -> votes_windowed (time series panel)
    windowed = (
        votes
        .withWatermark("event_time", "2 minutes")
        .groupBy(window(col("event_time"), WINDOW_DURATION), col("vote"))
        .agg(count("*").alias("vote_count"))
    )
    windowed_query = (
        windowed.writeStream
        .trigger(processingTime="500 milliseconds")
        .outputMode("update")
        .foreachBatch(lambda df, eid: process_windowed_batch(df, eid, args.es_host))
        .option("checkpointLocation", f"{args.checkpoint_dir}/windowed")
        .start()
    )

    # 3. Running totals -> vote_totals (live leaderboard panel)
    totals = (
        votes.groupBy(col("vote"))
        .agg(count("*").alias("total_count"), spark_max("event_timestamp").alias("latest_event_timestamp"))
    )
    totals_query = (
        totals.writeStream
        .trigger(processingTime="100 milliseconds")
        .outputMode("update")
        .foreachBatch(lambda df, eid: process_totals_batch(df, eid, args.es_host))
        .option("checkpointLocation", f"{args.checkpoint_dir}/totals")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
