# Network Intrusion Detection

**Real-Time Network Intrusion Detection with Big Data Technologies**

## Overview

This project aims to build a Big Data pipeline for network traffic analysis and intrusion detection. It combines batch processing and real-time streaming to process network traffic data and support the identification of benign and malicious activity.

The project uses the CIC-IDS-2017 dataset as its planned reference dataset.

## Objectives

- Ingest network traffic data in batch and streaming modes.
- Use Apache Kafka for event streaming.
- Store processed and historical data in HDFS using Parquet.
- Process and analyze data with Apache Spark and Spark SQL.
- Develop a binary classification model to distinguish `BENIGN` traffic from `ATTACK` traffic.
- Process incoming events through Spark Structured Streaming.
- Store detection results in PostgreSQL.
- Provide a dashboard for monitoring intrusion detection results (planned).

## Architecture

```text
Network Traffic / CIC-IDS-2017
              |
      Batch and Streaming
              |
       Apache Kafka
              |
     Apache Spark
      /          \
 Batch Processing  Structured Streaming
      |                    |
 HDFS / Parquet       Detection Results
                           |
                       PostgreSQL
                           |
                    Dashboard (planned)
```

## Technology Stack

| Component | Technology |
|---|---|
| Containerization | Docker |
| Event streaming | Apache Kafka |
| Distributed storage | HDFS |
| Distributed processing | Apache Spark |
| Data analysis | Spark SQL |
| Data format | Parquet |
| Machine Learning | Spark ML (planned) |
| Results storage | PostgreSQL |
| Version control | Git and GitHub |

## Dataset

The project plans to use the official CIC-IDS-2017 dataset from the Canadian Institute for Cybersecurity.

Official source: https://cicresearch.ca/CICDataset/CIC-IDS-2017/browse.php?t=1792967905994

The dataset has not yet been downloaded. Data exploration, quality checks, preprocessing, and model training will be performed in later stages.

## Current Progress

- [x] Project objectives and initial architecture defined.
- [x] Docker infrastructure configured.
- [x] HDFS, Spark, and Kafka services started.
- [x] Kafka-to-Spark-to-HDFS integration test completed.
- [x] Initial Git commit created.
- [ ] Publish the repository on GitHub.
- [ ] Add project collaborators.
- [ ] Download and validate the dataset.
- [ ] Implement batch ingestion and processing.
- [ ] Build and evaluate the intrusion detection model.
- [ ] Implement real-time inference.
- [ ] Store detection results in PostgreSQL.
- [ ] Develop the monitoring dashboard.

## Repository Structure

```text
network-intrusion-detection/
├── .gitignore
├── docker-compose.yml
├── test_spark.py
├── test_kafka_connector.py
├── stream_kafka_to_hdfs.py
├── stream_kafka_to_hdfs_debug.py
├── data/
│   └── raw/
└── README.md
```

The repository structure may evolve as the project develops. Dataset files and generated data should not be committed to Git.

## Getting Started

### Prerequisites

- Docker Desktop
- Git
- Python
- A compatible Docker Compose environment

### Start the infrastructure

From the project root, run:

```powershell
docker compose up -d
```

Check the running containers:

```powershell
docker ps
```

The Docker Compose configuration defines the services and ports used by the project.

## Future Work

1. Download and validate CIC-IDS-2017.
2. Implement batch ingestion and data quality checks.
3. Build the distributed processing pipeline.
4. Train and compare classification models.
5. Integrate real-time detection with Kafka and Spark Structured Streaming.
6. Persist detection results and expose monitoring indicators.

## Disclaimer

This is an academic and portfolio project for learning Big Data engineering and network intrusion detection. Model performance and detection quality will need to be evaluated experimentally.

## License

A license will be selected before public distribution of the project.