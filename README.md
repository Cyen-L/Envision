== Re-initialization ==
docker-compose down -v
Remove-Item -Recurse -Force clickhouse-data
docker-compose up -d
python ./ingest.py

C1 - Daily total transaction count (query output)
SELECT toDate(transaction_time) AS day, COUNT(*) AS total_count FROM olap_db.transactions GROUP BY day ORDER BY day;

C2 - Daily total transfer amount (query output)
SELECT toDate(transaction_time) AS day, SUM(transfer_amount) AS total_amount FROM olap_db.transactions GROUP BY day ORDER BY day;

C3 - Daily count by currency (query output)
SELECT toDate(transaction_time) AS day, currency_code, COUNT(*) AS total_count FROM olap_db.transactions GROUP BY day, currency_code ORDER BY day, currency_code;
==============================================
Setup
Prerequisites
- Python 3.13.12
- Docker Compose Engine
- Visual Studio Code


==============================================
PART 0 - Local Docker Environment Setup
Clickhouse Connection Detail
Host: localhost
Port: 9000
Database: olap_db
User: admin
Password: admin

For testing purpose, all the configuration can be set from CONFIG.JSON file.

Part A - Schema & Table Design
The table serves as the core fact table for an OLAP environment, storing transactional data with support for time-series analysis.

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

Data Types:
DateTime64(3) is used to store timestamps with millisecond precision.
Decimal(18, 2) is used ensures exact precision for monetary values.
UInt64 is used for IDs and versions to support high cardinality.

Engine: 
ReplacingMergeTree engine is used with the _version column to handle late-arriving data and idempotent writes. When a record with the same sorting key is inserted, ClickHouse will eventually keep only the row with the highest _version during partition merges.

Partitioning:
Partitioning by transaction_time balances granularity and manageability, significantly reducing the scan size while modifying data


Part B - Ingestion Pipeline
Ingestion Result (Batch size of 1,000,000)
Total rows inserted: 31094882
Total time: 261.08 seconds
Throughput: 119101.49 rows/sec

Part C - Pre-aggregation Tables + Analytics Outputs

C1: Daily Total Transaction Count
python C1.py [--sort-by {day | total_count}] [--descending]
SQL Query:
SELECT toDate(transaction_time) AS day, COUNT(*) AS total_count
FROM olap_db.transactions
GROUP BY day
ORDER BY {day|total_count} {ASC | DESC};

C2: Daily Total Transfer Amount
python C2.py [--sort-by {day | total_amount}] [--descending]
SQL Query:
SELECT toDate(transaction_time) AS day, SUM(transfer_amount) AS total_amount 
FROM olap_db.transactions 
GROUP BY day 
ORDER BY {day | total_amount} {ASC | DESC};

C3: Daily Count by Currency
python C3.py [--sort-by {day | currency_code | total_count}] [--descending]
SQL Query:
SELECT toDate(transaction_time) AS day, currency_code, COUNT(*) AS total_count 
FROM olap_db.transactions 
GROUP BY day, currency_code 
ORDER BY {day | currency_code | total_count} {ASC | DESC};

C4: Top 20 Usernames by Total Amount (Date Range)
python C4.py [--start-date {YYYY-MM-DD}] [--end-date {YYYY-MM-DD}] [--sort-by {username | total_amount}] [--descending]
SQL Query:
SELECT username, sum(transfer_amount) AS total_amount
FROM olap_db.transactions
WHERE transaction_time BETWEEN '{YYYY-MM-DD} 00:00:00.000' AND '{YYYY-MM-DD} 23:59:59.999'
GROUP BY username
ORDER BY {username | total_amount} {ASC | DESC}
LIMIT 20;

C5 & C6: Pre-aggregation Tables
Run aggregation jobs to generate table:
python aggregate.py

Prerequisites
Python with clickhouse-driver installed.
utils.py file containing the functions run_query, get_client, table_exists
CONFIG.JSON with ClickHouse connection details
ClickHouse service is running.

Querying the Aggregated Tables:
Table agg_username_30min
window_start (DateTime64(3)): The start of the 30‑minute interval, either on the hour (00:00) or half-hour (30:00).
site_id (UInt32): Unique site identifier.
username (String): Unique username.
txn_count (UInt64): Number of transactions in that interval.
total_amount (Decimal(18,2)): Sum of transfer_amount.

Table agg_site_daily
day (Date): The date of the aggregation.
site_id (UInt32): Unique site identifier.
txn_count (UInt64): Total transactions for that site on that day.
total_amount (Decimal(18,2)): Total amount for that site on that day.

creates and populates two pre-aggregated tables:
agg_username_30min – 30-minute window aggregates by site_id and username
agg_site_daily – Daily aggregates by site_id
Perform an incremental refresh (only new data since last run is added) using the ReplacingMergeTree engine to handle duplicates
Print any errors encountered

Setup
1. Prerequisites
* Docker and Docker Compose installed
* Python 3.8+ with clickhouse-driver installed (pip install clickhouse-driver)
* The source data (JSON files) placed in the json_files/ directory

2. Start ClickHouse
docker-compose up -d
2.1 Start ClickHouse server on ports 9000 (native protocol) and 8123 (HTTP)
2.2 Create a database olap_db (if not exists) and a table transactions defined in schema.sql
Connection details (as per docker-compose.yml):
Host: localhost
Port: 9000
Database: olap_db
User: admin
Password: admin

3. Install Python Dependencies
pip install clickhouse-driver

Project Structure
.
├── docker-compose.yml       # Docker setup
├── schema.sql               # Table schema for raw transactions
├── requirements.txt         # Library requirements for Python Environment
├── utils.py                 # Shared utilities (config loading, client creation)
├── ingest.py                # Ingestion script
├── ingestion_tracking.txt   # Ingestion state txt for file tracking
├── aggregate.py             # Pre-aggregation script
├── C1.py                    # Daily total transaction count
├── C2.py                    # Daily total transfer amount
├── C3.py                    # Daily count by currency
├── C4.py                    # Top 20 usernames by amount (date range)
├── Bonus-1.py               # Daily username count by site ID within date range
├── CONFIG.JSON              # Configuration file (see below)
└── json_files/              # Input directory for JSON files

Configuration
{
  "clickhouse": {
    "host": "localhost",
    "port": 9000,
    "user": "admin",
    "password": "admin",
    "database": "olap_db"
  }
}

Data Ingestion
Prepare Data
Place all JSON files (each containing an array of arrays) into the json_files/ directory
Run Ingestion
python ingest.py
Process all .json files in json_files/
Insert rows in batches (default batch size 1,000,000)
Output a summary with:
Total rows inserted
Total duration (wall-clock time)
Throughput (rows/sec)
Processing file1.json.....
Processing file2.json.....
------------------------

