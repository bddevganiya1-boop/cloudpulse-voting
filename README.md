# CloudPulse Voting — Real-Time Streaming Analytics & Observability Platform

A self-contained Big Data pipeline that pairs an enhanced **Cats vs Dogs Voting Application** with high-throughput stream processing (**Kafka**, **Spark Structured Streaming**, **Elasticsearch**) and real-time visualization on **Grafana** (styled after the Telegraf #928 System Dashboard).

---

## 🏗️ Architecture & Data Flow

```
+-------------------------------------------------------------------------+
|                  Modernized Cats vs Dogs Voting UI                      |
|       - Neon Dark-Mode Battle Arena (Cats vs Dogs)                      |
|       - Built-in Auto-Voting Engine (Pumps random votes every 5s)       |
|       - Manual voting + instant burst triggers                          |
+------------------------------------+------------------------------------+
                                     | (Writes vote payloads)
                                     v
+-------------------------------------------------------------------------+
|                           Redis In-Memory Queue                         |
|                   List: `votes` (voter_id, vote choice)                 |
+------------------------------------+------------------------------------+
                                     |
                                     | (High-speed consumer drain)
                                     v
+-------------------------------------------------------------------------+
|                          .NET Core Background Worker                    |
|             Upserts voter record into PostgreSQL `votes` table          |
+------------------------------------+------------------------------------+
                                     |
                                     | (Change Data Capture / Poll stream)
                                     v
+-------------------------------------------------------------------------+
|                    Vote Producer Service (Python)                       |
|           Streams new & changed votes to Kafka topic `votes-raw`        |
+------------------------------------+------------------------------------+
                                     |
                                     | (Durable event log stream)
                                     v
+-------------------------------------------------------------------------+
|                          Apache Kafka 7.5.0                             |
|                        Topic: `votes-raw`                               |
+------------------------------------+------------------------------------+
                                     |
                                     | (Spark Kafka Streaming Source)
                                     v
+-------------------------------------------------------------------------+
|             Apache Spark 3.5.1 (Structured Streaming)                   |
|                   Micro-batch Stream Processor                          |
|                                                                         |
|  +--------------------+  +--------------------+  +--------------------+ |
|  |     votes_raw      |  |   votes_windowed   |  |    vote_totals     | |
|  |  (Audit log stream)|  | (30s tumbling win) |  | (Leaderboard state)| |
|  +----------+---------+  +---------+----------+  +---------+----------+ |
+-------------|----------------------|-----------------------|------------+
              |                      |                       |
              +----------------------+-----------------------+
                                     | (Bulk REST API writes)
                                     v
+-------------------------------------------------------------------------+
|                        Elasticsearch 8.13.4                             |
|           Indices: `votes_raw`, `votes_windowed`, `vote_totals`         |
+------------------------------------+------------------------------------+
                                     |
                                     | (Auto-provisioned proxy data sources)
                                     v
+-------------------------------------------------------------------------+
|               Grafana 11.0.0 Dashboard (Telegraf #928 Edition)          |
|  - Quick Overview: Total Votes, Voting Rate, Cats/Dogs Gauges, Status   |
|  - Voting Dynamics: Multi-line rate timeseries with legend table stats  |
|  - Leaderboard & Share: Horizontal Bar Gauge & Donut distribution       |
|  - Audit Trail: Real-time tabular event stream with status badges       |
+-------------------------------------------------------------------------+
```

---

## 🌟 Key Features

### 1. 🐱🐶 Redesigned Voting App UI
- **Modern Neon Glassmorphism**: Tailored dark-mode UI with glowing cyan (Team Cats) and radiant sunset-amber (Team Dogs) cards.
- **⚡ Automated Voting Engine**: 
  - Automatically emits simulated random votes **every 5.0 seconds** to feed the live streaming pipeline.
  - Interactive browser toggle to Pause / Resume the auto-voter.
  - Live animated countdown progress bar (`Next vote in: X.Xs`).
  - "Burst 5 Votes" button for testing burst ingestion load.
  - Session votes counter and real-time pop-up notification toasts.
- **Persistent Voter Identity**: Random voter ID cookie session tracking with vote-changing support.

### 2. ⚡ Big Data Streaming Core
- **Apache Kafka**: Decouples OLTP database writes from downstream analytical compute, serving as a replayable, fault-tolerant buffer.
- **Apache Spark Structured Streaming**:
  - `votes_raw`: Full audit trail of all vote transactions.
  - `votes_windowed`: 30-second sliding/tumbling window aggregates to evaluate voting velocity over time.
  - `vote_totals`: Stateful unwindowed running totals maintaining live leaderboard state without database contention.
- **Elasticsearch**: Sub-second full-text and aggregation search engine storing raw, windowed, and totals indices.

### 3. 📊 Telegraf #928-Inspired Grafana Dashboard
Styled after the popular **Telegraf System Dashboard (#928)**:
- **Quick Overview Row**:
  - `Total Votes Cast` (Stat with historical area trend)
  - `Active Voting Velocity` (Current voting rate with color thresholds)
  - `Cats Votes & Progress` (Threshold-colored gauge)
  - `Dogs Votes & Progress` (Threshold-colored gauge)
  - `Pipeline Status` (Real-time `HEALTHY / STREAMING` indicator)
- **Voting Dynamics Row**:
  - `Voting Rate Over Time — Cats vs Dogs` (Multi-line timeseries graph with smooth area fill and legend table showing mean, last, max, min, sum).
  - `Vote Share Distribution` (Donut chart showing percentages and counts).
- **Leaderboard & Raw Audit Stream**:
  - `Live Leaderboard` (Horizontal gradient bar gauge).
  - `Recent Raw Votes Stream` (Real-time tabular event stream with timestamps and voter IDs).

---

## 🚀 Quickstart

Run a single command to build and launch all 13 services:

```bash
docker compose up -d --build
```

### 📋 Service Inventory & Endpoints

| Service | Container Name | Host Port | Internal Port | Description |
| :--- | :--- | :--- | :--- | :--- |
| **Voting App** | `cpv-vote` | **8080** | 80 | Redesigned Cats vs Dogs voting app with 5s auto-voter |
| **Results App** | `cpv-result` | **8081** | 80 | Live vote count service |
| **Grafana** | `cpv-grafana` | **3000** | 3000 | Telegraf #928 live observability dashboard (`admin`/`admin`) |
| **Spark Master** | `cpv-spark-master` | **8090** | 8080 | Spark Master Web UI |
| **Elasticsearch** | `cpv-elasticsearch` | **9200** | 9200 | Time-series document store & search API |
| **Kafka Broker** | `cpv-kafka` | **29092** | 9092 | High-throughput streaming buffer (`votes-raw`) |
| **Redis** | `cpv-redis` | — | 6379 | In-memory voting queue |
| **PostgreSQL** | `cpv-db` | — | 5432 | Relational vote store (`votes` table) |
| **Worker** | `cpv-worker` | — | — | .NET Core worker consuming Redis -> Postgres |
| **Vote Producer** | `cpv-vote-producer`| — | — | Python daemon streaming Postgres -> Kafka |
| **Spark Streaming**| `cpv-spark-job` | — | — | PySpark Structured Streaming aggregation job |
| **ES Init** | `cpv-es-init` | — | — | One-shot index mappings initializer |
| **Zookeeper** | `cpv-zookeeper` | **2181** | 2181 | Kafka cluster coordination |

---

## 🔍 Verification & Testing

### 1. View Voting Web Application
Open your browser at **[http://localhost:8080](http://localhost:8080)**:
- Watch the countdown bar tick down and emit a vote every 5 seconds.
- Click **Vote for Cats** or **Vote for Dogs** to cast manual votes.
- Click **⚡ Burst 5 Votes** to fire rapid vote batches.

### 2. View Grafana Live Dashboard
Open **[http://localhost:3000/d/cloudpulse-voting-dashboard/](http://localhost:3000/d/cloudpulse-voting-dashboard/)** (login `admin` / `admin`):
- Watch the **Total Votes Cast** counter increment live.
- Observe the **Voting Rate Over Time** timeseries graph plotting Cats vs Dogs trends.
- View the **Live Leaderboard** and **Vote Share Distribution**.
- Inspect the **Recent Raw Votes Stream** table updating in real time.

### 3. Check Pipeline Logs
```bash
# Check voting app auto-voter emissions
docker logs cpv-vote --tail 20

# Check Kafka producer streaming
docker logs cpv-vote-producer --tail 20

# Query Elasticsearch index counts
curl -s "http://localhost:9200/_cat/indices?v"
```

---

## 🛑 Clean Up

```bash
# Stop all services and preserve data
docker compose down

# Stop all services and remove persistent volumes
docker compose down -v
```
