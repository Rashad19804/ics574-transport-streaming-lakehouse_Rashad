# ICS574 Course Project – Option 1 (Transport Streaming Lakehouse)

This repo is an **original implementation** (not a clone) inspired by the open-source architecture in `oiivantsov/transport-streaming-lakehouse`.

## What you get (Option 1)
- **Real-time ingestion** from HSL High-Frequency Positioning (HFP) **MQTT** feed to **Kafka**.
- **Stream processing** with **Spark Structured Streaming**.
- **Lakehouse storage** as **Delta Lake tables** in an S3-compatible object store (**MinIO**).
- **Serving layer for dashboarding** in **PostgreSQL**.
- **Visualization** using **Grafana**...

## Tech Stack
Kafka, Spark 3.5 (Structured Streaming), Delta Lake, MinIO, PostgreSQL, Grafana, Docker Compose.

## How to run (local)

### 0) Prerequisites
- Docker Desktop
- At least 8 GB RAM (recommended 16 GB)

### 1) Start the infrastructure
```bash
docker compose up -d zookeeper kafka minio minio-mc postgres spark-master spark-worker grafana
```

### 2) Start the MQTT→Kafka producer
```bash
docker compose up -d producer
```

### 3) Run the Spark streaming job (Kafka→Delta + Postgres)
```bash
docker compose up spark-bronze-stream
```

### 4) Open UIs
- Spark Master UI: http://localhost:8080
- Spark Worker UI: http://localhost:8081
- MinIO Console: http://localhost:9001 (user: `minio`, pass: `minio123`)
- Grafana: http://localhost:3000 (user: `admin`, pass: `admin`)

## Data source
- HSL MQTT broker `mqtt.hsl.fi` (TLS port 8883) and topic tree under `/hfp/v2/journey/ongoing/vp/#` (vehicle positions). See Digitransit documentation.

## Notes
- The Spark job writes Bronze Delta tables to: `s3a://lakehouse/bronze/hsl_vehicle_positions`
- A simple live “count of vehicles” dashboard is provisioned in Grafana.

---

If you want, I can extend this to add **Silver/Gold layers** (batch cleaning + aggregations) and **Trino SQL** to query Delta tables.
