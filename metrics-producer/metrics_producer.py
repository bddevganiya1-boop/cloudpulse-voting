"""
metrics_producer.py

High-volume, multi-host system metrics generator.
- Simulates 8 hosts (host-01 through host-08) emitting realistic metrics every 2 seconds
  into Kafka topic `system-metrics-raw`.
- Headless backend service (pure Python, self-contained, no manual pip install step).
"""

import argparse
import json
import logging
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("metrics_producer")

HOST_COUNT = 8
HOSTS = [f"host-{i:02d}" for i in range(1, HOST_COUNT + 1)]


class HostSimulator:
    """Simulates realistic metric streams with natural variation, drift, and spikes for a host."""

    def __init__(self, host_id: str):
        self.host = host_id
        # Start uptime between 1 hour and 10 days
        self.uptime_seconds = random.randint(3600, 864000)
        # Baseline CPU (25 - 45%)
        self.base_cpu = random.uniform(25.0, 45.0)
        # Initial memory usage (35 - 65%)
        self.memory_percent = random.uniform(35.0, 65.0)
        # Spike state tracking
        self.is_spiking = False
        self.spike_ticks_remaining = 0

    def sample(self) -> dict:
        self.uptime_seconds += 2

        # 1. Memory drift: smooth random walk with boundary elasticity
        mem_drift = random.gauss(0, 0.4)
        self.memory_percent = max(15.0, min(95.0, self.memory_percent + mem_drift))
        # 3% chance of a transient memory load increase
        if random.random() < 0.03:
            self.memory_percent = min(98.5, self.memory_percent + random.uniform(8.0, 20.0))

        # 2. CPU with normal variation (20-60%) and occasional spikes (85-99%)
        if self.is_spiking:
            cpu_percent = random.uniform(88.0, 99.5)
            self.spike_ticks_remaining -= 1
            if self.spike_ticks_remaining <= 0:
                self.is_spiking = False
        else:
            # 5% chance of naturally triggering a CPU spike lasting 3 to 7 ticks (6 to 14 seconds)
            if random.random() < 0.05:
                self.is_spiking = True
                self.spike_ticks_remaining = random.randint(3, 7)
                cpu_percent = random.uniform(88.0, 99.5)
            else:
                variation = random.gauss(0, 4.5)
                cpu_percent = max(15.0, min(65.0, self.base_cpu + variation))

        # 3. Disk I/O (0 - 50 MB/s)
        if self.is_spiking:
            disk_io = random.uniform(25.0, 48.0)
        else:
            disk_io = max(0.2, min(45.0, random.expovariate(1.0 / 8.0)))

        # 4. Network I/O (0 - 100 MB/s)
        if self.is_spiking:
            net_io = random.uniform(65.0, 98.0)
        else:
            net_io = max(0.5, min(95.0, random.expovariate(1.0 / 20.0)))

        return {
            "host": self.host,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "cpu_percent": round(cpu_percent, 2),
            "memory_percent": round(self.memory_percent, 2),
            "disk_io_mb_per_sec": round(disk_io, 2),
            "network_io_mb_per_sec": round(net_io, 2),
            "uptime_seconds": self.uptime_seconds,
        }


def connect_kafka(bootstrap_servers: str, max_retries: int = 30, retry_interval: int = 2) -> KafkaProducer:
    """Connects to Kafka with retry logic until brokers become available."""
    for attempt in range(1, max_retries + 1):
        try:
            log.info("Connecting to Kafka at %s (attempt %d/%d)...", bootstrap_servers, attempt, max_retries)
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
                request_timeout_ms=10000,
            )
            log.info("Successfully connected to Kafka broker at %s", bootstrap_servers)
            return producer
        except (NoBrokersAvailable, Exception) as exc:
            log.warning("Kafka broker not ready (%s). Retrying in %ds...", exc, retry_interval)
            time.sleep(retry_interval)
    raise RuntimeError(f"Failed to connect to Kafka at {bootstrap_servers} after {max_retries} attempts.")


def main():
    parser = argparse.ArgumentParser(description="Multi-host System Metrics Producer")
    parser.add_argument(
        "--kafka",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        help="Kafka bootstrap server address (default: env KAFKA_BOOTSTRAP_SERVERS or kafka:9092)",
    )
    parser.add_argument(
        "--topic",
        default=os.environ.get("METRICS_TOPIC", "system-metrics-raw"),
        help="Kafka topic for raw metrics (default: system-metrics-raw)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.environ.get("PUBLISH_INTERVAL_SECONDS", "2.0")),
        help="Emit interval in seconds per host (default: 2.0)",
    )
    args = parser.parse_args()

    log.info("Initializing metrics producer for %d hosts: %s", len(HOSTS), HOSTS)
    log.info("Target topic: %s | Kafka: %s | Interval: %.1fs", args.topic, args.kafka, args.interval)

    producer = connect_kafka(args.kafka)
    simulators = [HostSimulator(h) for h in HOSTS]

    published_total = 0

    while True:
        cycle_start = time.time()
        for sim in simulators:
            reading = sim.sample()
            try:
                producer.send(args.topic, value=reading)
                published_total += 1
            except Exception as exc:
                log.error("Failed to send metric for %s: %s", sim.host, exc)

        producer.flush()

        if published_total % 40 == 0:
            log.info("Emitted %d total metric events across %d hosts to topic %s", published_total, len(HOSTS), args.topic)

        elapsed = time.time() - cycle_start
        sleep_time = max(0.0, args.interval - elapsed)
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
