# Envision – ClickHouse Data Pipeline

This project provides a complete data ingestion and aggregation pipeline for transactional data using ClickHouse, designed for OLAP workloads. It includes scripts for incremental ingestion, pre‑aggregation, and analytical queries.

---

## Table of Contents

- [Project Structure](#project-structure)
- [Setup](#setup)
  - [Prerequisites](#prerequisites)
  - [Installation Steps](#installation-steps)
- [PART 0 - Local Docker Environment Setup](#part-0---local-docker-environment-setup)
  - [Key Settings](#key-settings)
- [Part A - Schema & Table Design](#part-a---schema--table-design)
- [Part B - Ingestion Pipeline](#part-b---ingestion-pipeline)
  - [Purpose:](#purpose)
  - [Key Functions:](#key-functions)
  - [Data Flow:](#data-flow)
  - [Ingest Performance:](#ingest-performance)
- [Part C - Pre-aggregation Tables + Analytics Outputs](#part-c---pre-aggregation-tables--Analytics-outputs)
  - [C1: Daily Total Transaction Count](#c1-daily-total-transaction-count)
  - [C2: Daily Total Transfer Amount](#c2-daily-total-transfer-amount)
  - [C3: Daily Count by Currency](#c3-daily-count-by-currency)
  - [C4: Top 20 Usernames by Total Amount (Date Range)](#c4-top-20-usernames-by-total-amount-date-range)
  - [C5 & C6: Pre-aggregation Tables](#c5--c6-pre-aggregation-tables)
- [Bonus](#bonus)
- [Bonus 1: Unique usernames per daily](#bonus-1-unique-usernames-per-daily)
- [Bonus 2: Incremental ingestion (only new files)](#bonus-2-incremental-ingestion-only-new-files)

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
## Part B - Ingestion Pipeline
Ingestion Script (ingest.py)
### Purpose:
Read JSON files and insert them into the transactions table. It keeps a tracking file (ingestion_tracking.txt) of successfully processed files to avoid re‑processing.

### Key Functions:
process_file(file_path, client, batch_size) – 
main() – discovers .json files in INPUT_DIR, filters out already processed ones, processes each, and prints an ingestion summary (total rows, time, throughput).

| Script                | Function                                                                 |
|------------------------|-------------------------------------------------------------------------|
| `load_processed_files()` / `mark_file_processed()`     | Manage the set of already ingested files.          |
| `parse_time_ms(value)`     | Converts epoch milliseconds (as int or string) to a Python datetime object (ClickHouse expects DateTime64).|
| `process_file(file_path, client, batch_size)`              | Reads a JSON file, validates each row (must be a list of 8 elements), transforms values to the correct types, and inserts in batches.                                 |
| `main()`       | Discovers .json files in INPUT_DIR, filters out already processed ones, processes each, and prints an ingestion summary (total rows, time, throughput).           |

### Data Flow:
JSON file → read → validate → convert → batch → INSERT INTO olap_db.transactions

### Ingest Performance:
| Parameter                | Value                                                                 |
|------------------------|-------------------------------------------------------------------------|
| `batch_size`     | 1,000,000          |
| Total rows inserted     | 31,094,882|
| Total time              | 261.08 sec                                 |
| Throughput       | 119,101 rows/sec           |

## Part C - Pre-aggregation Tables + Analytics Outputs
All the analytics outputs and aggregated tables are generated thro Python script. The following shows the usage of the script as well as the SQL statement:
### C1: Daily Total Transaction Count
#### Python Script:
```python
python C1.py [--sort-by {day | total_count}] [--descending]
```
#### SQL Query:
```sql
SELECT toDate(transaction_time) AS day, COUNT(*) AS total_count
FROM olap_db.transactions
GROUP BY day
ORDER BY {day|total_count} {ASC | DESC};
```

### C2: Daily Total Transfer Amount
#### Python Script:
```python
python C2.py [--sort-by {day | total_amount}] [--descending]
```
#### SQL Query:
```sql
SELECT toDate(transaction_time) AS day, SUM(transfer_amount) AS total_amount 
FROM olap_db.transactions 
GROUP BY day 
ORDER BY {day | total_amount} {ASC | DESC};
```

### C3: Daily Count by Currency
#### Python Script:
```python
python C3.py [--sort-by {day | currency_code | total_count}] [--descending]
```
#### SQL Query:
```sql
SELECT toDate(transaction_time) AS day, currency_code, COUNT(*) AS total_count 
FROM olap_db.transactions 
GROUP BY day, currency_code 
ORDER BY {day | currency_code | total_count} {ASC | DESC};
```

### C4: Top 20 Usernames by Total Amount (Date Range)
#### Python Script:
```python
python C4.py [--start-date {YYYY-MM-DD}] [--end-date {YYYY-MM-DD}] [--sort-by {username | total_amount}] [--descending]
```
#### SQL Query:
```sql
SELECT username, sum(transfer_amount) AS total_amount
FROM olap_db.transactions
WHERE transaction_time BETWEEN '{YYYY-MM-DD} 00:00:00.000' AND '{YYYY-MM-DD} 23:59:59.999'
GROUP BY username
ORDER BY {username | total_amount} {ASC | DESC}
LIMIT 20;
```

### C5 & C6: Pre-aggregation Tables
Python script was used to generate two pre-aggregation tables as requested:
- agg_username_30min
- agg_site_daily
```python
python aggregate.py
```

#### Querying the Aggregated Tables
- Table agg_username_30min

| Column                | Description                                                                 |
|------------------------|-------------------------------------------------------------------------|
| `window_start` (DateTime64(3))     | The start of the 30‑minute interval, either on the hour (00:00) or half-hour (30:00).          |
| `site_id` (UInt32)     | Unique site identifier.|
| `username` (String)              | Unique username.                                 |
| `txn_count` (UInt64)       | Number of transactions in that interval.           |
| `total_amount` (Decimal(18,2))       | Sum of transfer_amount.           |

- Table agg_site_daily

| Column                | Description                                                                 |
|------------------------|-------------------------------------------------------------------------|
| `day` (Date)     | The date of the aggregation.          |
| `site_id` (UInt32)     | Unique site identifier.|
| `txn_count` (UInt64)       | Total transactions for that site on that day.           |
| `total_amount` (Decimal(18,2))       | Total amount for that site on that day.           |

## Bonus
### Bonus 1: Unique usernames per daily
#### Python Script:
```python
python Bonus-1.py [--start-date {YYYY-MM-DD}] [--end-date {YYYY-MM-DD}] [--site-id]
```
#### SQL Query:
```sql
SELECT toDate(transaction_time) AS day, site_id, uniq(username) AS total_count
FROM olap_db.transactions
WHERE transaction_time BETWEEN '{start date} 00:00:00.000' AND '{end date} 23:59:59.999'
GROUP BY {clause_col}
ORDER BY {clause_col};
```

### Bonus 2: Incremental ingestion (only new files)
Read `ingest.py` section.