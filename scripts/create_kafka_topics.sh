#!/usr/bin/env bash
# Creates the Kafka topics required for the CloudPulse metrics pipeline.
set -euo pipefail

TOPICS=("system-metrics-raw")

for topic in "${TOPICS[@]}"; do
  echo "Creating topic: $topic"
  docker exec cloudpulse-kafka kafka-topics \
    --create \
    --if-not-exists \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic "$topic"
done

echo "Done. Listing Kafka topics:"
docker exec cloudpulse-kafka kafka-topics --list --bootstrap-server localhost:9092
