# Network Intrusion Detection

**Real-Time Network Intrusion Detection with Big Data Technologies**

## 1. Project Overview

This academic and portfolio project aims to build a Big Data pipeline for network traffic analysis and intrusion detection.

The platform is designed to support batch processing and real-time streaming using Apache Kafka, Apache Spark, HDFS and PostgreSQL.

The planned reference dataset is CIC-IDS-2017, published by the Canadian Institute for Cybersecurity.

## 2. Objectives

- Ingest network traffic in batch and streaming modes.
- Transport streaming events through Apache Kafka.
- Store historical and processed data in HDFS.
- Process network traffic using Apache Spark and Spark SQL.
- Develop a binary classification model to distinguish `BENIGN` traffic from `ATTACK` traffic.
- Integrate real-time inference using Spark Structured Streaming.
- Store detection results in PostgreSQL.
- Build a monitoring dashboard.

## 3. Architecture

```text
                Network Traffic
                 CIC-IDS-2017
                       |
              Batch / Streaming
                       |
                 Apache Kafka
                       |
                  Apache Spark
                 /            \
          Batch Processing   Structured Streaming
                 |                   |
             HDFS / Parquet     Intrusion Detection
                                     |
                                 PostgreSQL
                                     |
                                  Dashboard
```

The architecture is being implemented incrementally. The current Kafka-to-Spark-to-HDFS integration test validates event transport and storage; it does not yet implement machine-learning-based intrusion detection.

## 4. Technology Stack

| Component | Technology |
|---|---|
| Containerization | Docker and Docker Compose |
| Event streaming | Apache Kafka |
| Distributed storage | HDFS |
| Distributed processing | Apache Spark |
| Data analysis | Spark SQL |
| Storage format | Parquet (planned) |
| Machine Learning | Spark ML (planned) |
| Results database | PostgreSQL |
| Version control | Git and GitHub |

## 5. Repository Structure

```text
network-intrusion-detection/
├── .gitignore
├── .env.example
├── docker-compose.yml
├── README.md
├── test_spark.py
├── test_kafka_connector.py
├── stream_kafka_to_hdfs.py
├── stream_kafka_to_hdfs_debug.py
└── data/
    ├── raw/
    └── processed/
```

The structure may evolve as development progresses.

**Important:** Dataset files, local environment files, checkpoints and generated data should not be committed to Git.

## 6. Prerequisites

Each collaborator needs:

- Git
- Docker Desktop
- Docker Compose support through Docker Desktop
- Sufficient memory and disk space to run the containers

Spark, Kafka, HDFS and PostgreSQL are provided through Docker images. Collaborators do not need to install these services manually on Windows.

The first startup downloads the required images, so it may take longer than subsequent startups.

## 7. Set Up a Local Environment

### Step 1 — Clone the repository

Open a terminal and run:

```cmd
git clone https://github.com/chaymaeeroual/network-intrusion-detection.git
cd network-intrusion-detection
```

### Step 2 — Create the local environment file

Copy the example file:

```cmd
copy .env.example .env
```

Open `.env`:

```cmd
notepad .env
```

Set a strong local development password:

```dotenv
POSTGRES_PASSWORD=YOUR_LOCAL_PASSWORD
```

Replace `YOUR_LOCAL_PASSWORD` with a password of your choice. Do not commit `.env` to Git or share it publicly.

### Step 3 — Validate the Docker Compose configuration

```cmd
docker compose config --quiet
```

If the command returns without an error, the Compose configuration passes this validation.

### Step 4 — Start the services

On a collaborator's own machine, run:

```cmd
docker compose up -d
```

This command is intended for setting up the collaborator's local environment, not for changing an already-running shared environment.

### Step 5 — Check the containers

```cmd
docker compose ps
```

Check the service logs if a container is not healthy:

```cmd
docker compose logs --tail 100
```

## 8. Services and Ports

| Service | Host port | Purpose |
|---|---:|---|
| Spark Master UI | 8080 | Monitor the Spark cluster |
| Spark Master | 7077 | Spark cluster connection |
| Kafka | 9092 | Host access to Kafka |
| HDFS NameNode UI | 9871 | Browse HDFS through the web interface |
| PostgreSQL | 5433 | Connect to PostgreSQL from the host |

The service names and internal network addresses used by applications inside Docker differ from the host ports. For example, applications running in the Compose network use `kafka:9092` for Kafka and `hdfs://namenode:9000` for HDFS.

Open these interfaces after starting the services:

- Spark: http://localhost:8080
- HDFS: http://localhost:9871

## 9. Streaming Pipeline

The initial streaming script is `stream_kafka_to_hdfs.py`.

It is designed to:

1. Read events from the Kafka topic `intrusion-test`.
2. Convert the Kafka message value into a string.
3. Write the events to HDFS using Spark Structured Streaming.
4. Maintain a checkpoint in HDFS.

The configured HDFS output path is:

```text
hdfs://namenode:9000/network-intrusion-detection/streaming-output
```

The checkpoint path is:

```text
hdfs://namenode:9000/network-intrusion-detection/checkpoint
```

**Dependency note:** Spark's Kafka source requires a compatible Spark Kafka connector. The exact `spark-submit` command and dependency resolution procedure must be documented and validated before the streaming job is considered reproducible on every collaborator's machine.

## 10. Dataset

The project plans to use CIC-IDS-2017 from the Canadian Institute for Cybersecurity.

Official source:

https://cicresearch.ca/CICDataset/CIC-IDS-2017/browse.php?t=1792967905994

The dataset has not yet been downloaded. Data exploration, quality checks, preprocessing, batch ingestion and model training will be performed in subsequent stages.

Raw and processed dataset files must remain local or in an agreed data-storage location rather than being committed to Git.

## 11. Current Progress

- [x] Project objectives and initial architecture defined.
- [x] Docker Compose infrastructure configured.
- [x] Kafka, Spark and HDFS services started.
- [x] Kafka-to-Spark-to-HDFS integration test completed.
- [x] Public GitHub repository created.
- [x] Local environment file excluded from Git.
- [ ] Finalize and validate the collaborator setup procedure.
- [ ] Document the reproducible Spark Kafka connector submission command.
- [ ] Download and validate CIC-IDS-2017.
- [ ] Implement batch ingestion and processing.
- [ ] Build and evaluate the intrusion detection model.
- [ ] Implement real-time inference.
- [ ] Persist detection results in PostgreSQL.
- [ ] Develop the monitoring dashboard.

## 12. Security and Collaboration

- Never commit `.env` or real credentials.
- Use `.env.example` to document required environment variables.
- Review changes before pushing to the public repository.
- Avoid committing datasets, generated files, logs or checkpoints.
- Coordinate changes to the shared Compose configuration with the team.
- Use feature branches for substantial development changes.

## 13. Disclaimer

This is an academic and portfolio project for learning Big Data engineering and network intrusion detection. Detection quality and model performance must be evaluated experimentally before any operational use.

## 14. License

A suitable license will be selected before public distribution of the project.
