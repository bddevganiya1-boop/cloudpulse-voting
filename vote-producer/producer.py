"""
producer.py

Bridges the voting app's OLTP store (Postgres) into the streaming
analytics pipeline. The official voting app's `worker` writes every vote
into a simple `votes` table with (at minimum) `id` (voter id) and `vote`
(the chosen option) columns.

Rather than touching the voting app's internals (its Redis queue, its
worker), this polls that table on an interval and treats it as a change
feed: any voter id whose stored vote differs from what we last saw is
published as a new event to Kafka. This correctly captures both first-
time votes and vote changes, without assuming a particular auto-increment
id scheme or timestamp column that different versions of the app may or
may not have.

Publishes to Kafka topic: votes-raw
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import psycopg2
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("vote_producer")

DB_HOST = os.environ.get("DB_HOST", "db")
DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "postgres")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "2.0"))
TOPIC = "votes-raw"

OPTION_LABELS = {
    "a": "Cats",
    "b": "Dogs",
    "cats": "Cats",
    "dogs": "Dogs",
    "Cats": "Cats",
    "Dogs": "Dogs",
}


def connect_db():
    """Retries connecting since the voting app's worker may not have
    created the `votes` table yet in the first few seconds after startup."""
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
            )
            conn.autocommit = True
            log.info("Connected to PostgreSQL at %s/%s", DB_HOST, DB_NAME)
            return conn
        except psycopg2.OperationalError as exc:
            log.warning("Postgres not ready yet (%s), retrying in 3s...", exc)
            time.sleep(3)


def connect_kafka():
    """Retries connecting to Kafka until the broker is ready."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
            )
            log.info("Connected to Kafka bootstrap at %s", KAFKA_BOOTSTRAP)
            return producer
        except Exception as exc:
            log.warning("Kafka not ready yet (%s), retrying in 3s...", exc)
            time.sleep(3)


def table_exists(cur) -> bool:
    cur.execute(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'votes')"
    )
    return cur.fetchone()[0]


def main():
    conn = connect_db()
    producer = connect_kafka()

    log.info("Polling Postgres `votes` table every %ss, publishing to '%s'", POLL_INTERVAL, TOPIC)

    last_seen = {}  # voter_id -> last published vote value

    while True:
        try:
            with conn.cursor() as cur:
                if not table_exists(cur):
                    log.info("`votes` table doesn't exist yet (worker initializing) — waiting...")
                    time.sleep(POLL_INTERVAL)
                    continue

                cur.execute("SELECT id, vote FROM votes")
                rows = cur.fetchall()

            for voter_id, raw_vote in rows:
                if last_seen.get(voter_id) == raw_vote:
                    continue  # already published, unchanged

                # Map 'a' -> 'Cats', 'b' -> 'Dogs'
                friendly_vote = OPTION_LABELS.get(str(raw_vote).strip(), str(raw_vote))

                record = {
                    "voter_id": str(voter_id),
                    "vote": friendly_vote,
                    "event_timestamp": datetime.now(timezone.utc).isoformat(),
                    "is_change": voter_id in last_seen,
                }
                producer.send(TOPIC, value=record)
                producer.flush()
                last_seen[voter_id] = raw_vote
                log.info("🚀 Published vote to Kafka: voter=%s choice=%s (raw=%s) changed=%s",
                         voter_id, friendly_vote, raw_vote, record["is_change"])

        except psycopg2.OperationalError as exc:
            log.warning("Lost DB connection (%s), reconnecting...", exc)
            conn = connect_db()
        except Exception as exc:
            log.error("Unexpected error in poll loop: %s", exc)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
