# Envision – ClickHouse Data Pipeline

This project provides a complete data ingestion and aggregation pipeline for transactional data using ClickHouse, designed for OLAP workloads. It includes scripts for incremental ingestion, pre‑aggregation, and analytical queries.

---

## Table of Contents

- [Project Structure](#project-structure)
- [Setup](#setup)
  - [Prerequisites](#prerequisites)
  - [Installation Steps](#installation-steps)
- [Configuration](#configuration)
- [Database Schema](#database-schema)
- [Ingestion Pipeline](#ingestion-pipeline)
- [Pre‑aggregation Tables](#pre-aggregation-tables)
- [Analytics Queries](#analytics-queries)
- [Usage Notes](#usage-notes)

---

## Project Structure
```text
.
├── docker-compose.yml # ClickHouse service definition
├── schema.sql # Table schema for raw transactions
├── requirements.txt # Python dependencies
├── utils.py # Shared utilities (config, client, helpers)
├── ingest.py # JSON ingestion script
├── ingestion_tracking.txt # Tracks already processed files
├── aggregate.py # Builds pre‑aggregation tables
├── C1.py # Daily total transaction count
├── C2.py # Daily total transfer amount
├── C3.py # Daily count by currency
├── C4.py # Top 20 usernames by amount (date range)
├── Bonus-1.py # Daily username count by site ID (date range)
├── config.json # Connection & runtime configuration
└── json_files/ # Input directory for JSON files
```

---

## Setup

### Prerequisites

- Python 3.13+
- Docker & Docker Compose
- Git
- (Optional) Visual Studio Code

### Installation Steps

1. **Clone the repository**
   ```bash
   git clone https://github.com/Cyen-L/Envision.git
   cd Envision

2. **Download the sample dataset**
   ```bash
   wget https://big-data-interview.s3.ap-southeast-2.amazonaws.com/sample_data.zip

3. **Create input directory and extract data**
   ```bash
   mkdir json_files
   unzip sample_data.zip -d json_files

4. **Set up Python virtual environment**
   ```bash
   python -m venv env
   source env/bin/activate 

5. **Install dependencies**
   ```bash
   pip install -r requirements.txt

6. **Start ClickHouse with Docker Compose**
   ```bash
   docker-compose up -d

7. **Ingest JSON files**
   ```bash
   python ingest.py

8. **Build pre‑aggregation tables**
   ```bash
   python aggregate.py

9. **Explore data via ClickHouse Play**
   Open [ClickHouse Play](http://localhost:8123/play) in your browser.

---

## PART 0 - Local Docker Environment Setup

The pipeline uses a single ClickHouse server defined in `docker-compose.yml`. Below is the recommended configuration:
```yaml
version: '3.8'
services:
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    container_name: clickhouse_server
    ports:
      - "8123:8123"   # HTTP interface (ClickHouse Play)
      - "9000:9000"   # Native TCP interface
    volumes:
      - clickhouse_data:/var/lib/clickhouse
    environment:
      CLICKHOUSE_DB: olap_db
      CLICKHOUSE_USER: admin
      CLICKHOUSE_PASSWORD: admin
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: 1
    ulimits:
      nofile:
        soft: 262144
        hard: 262144

volumes:
  clickhouse_data:
```
### Key Settings
| Setting                | Purpose                                                                 |
|------------------------|-------------------------------------------------------------------------|
| `ports: 8123:8123`     | Exposes ClickHouse Play web UI at `http://localhost:8123/play`          |
| `ports: 9000:9000`     | Exposes native TCP port for `clickhouse-driver` (used by Python scripts)|
| `volumes`              | Persists data across container restarts                                 |
| `ulimits.nofile`       | Increases file descriptor limit for high‑throughput ingestion           |

## Part A - Schema & Table Design
The table serves as the core fact table for an OLAP environment, storing transactional data with support for time-series analysis.
```sql
-- Create database if not exists
CREATE DATABASE IF NOT EXISTS olap_db;

-- Raw transactions table
CREATE TABLE IF NOT EXISTS olap_db.transactions (
    transaction_id    UInt64,
    bill_id           UInt64,
    site_id           UInt32,
    username          String,
    item_id           UInt32,
    currency_code     String,
    transfer_amount   Decimal(18, 2),
    transaction_time  DateTime64(3), 
    _version          UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (site_id, username, transaction_time, _version)
PARTITION BY toYYYYMM(transaction_time);
```
| Element                | Rationale                                                                 |
|------------------------|-------------------------------------------------------------------------|
| `DateTime64(3)`     | Ensure store timestamps with millisecond precision.          |
| `Decimal(18,2)`     | Ensures exact precision for monetary values.|
| `UInt64`              | For IDs and versions to support high cardinality.                                 |
| `ReplacingMergeTree`       | Handles late‑arriving / duplicate data based on _version           |
---


```sql
```