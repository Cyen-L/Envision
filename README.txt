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
==============================================
Setup
Prerequisites
- Python 3.13.12
- Docker Compose Engine
- Visual Studio Code
- Git

1. Clone the repository from GitHub into a specific directory
git clone https://github.com/Cyen-L/Envision.git

2. Download the sample_data.zip
wget https://big-data-interview.s3.ap-southeast-2.amazonaws.com/sample_data.zip

3. Create json_files directory to store dataset
mkdir json_files

4. Extract the dataset into json_files
tar -xf sample_data.zip -C json_files

5. Create and activate a virtual python environment
python -m venv env
source venv/bin/activate

6. Install Python library requirement txt
pip install -r requirements.txt

7. Compose the container with docker-compoase.yml
docker-compose up

8. Ingest dataset from JSON files
python .\ingest.py

9. Build pre-aggregation tables
python .\aggregate.py

10. Querying table from web browser
http://localhost:8123/play

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

Detailed Design
Ingestion Script (ingest.py)
Purpose:
Read JSON files and insert them into the transactions table. It keeps a tracking file (ingestion_tracking.txt) of successfully processed files to avoid re‑processing.
Key Functions:
load_processed_files() / mark_file_processed() – manage the set of already ingested files.
parse_time_ms(value) – converts epoch milliseconds (as int or string) to a Python datetime object (ClickHouse expects DateTime64).
process_file(file_path, client, batch_size) – reads a JSON file, validates each row (must be a list of 8 elements), transforms values to the correct types, and inserts in batches.
main() – discovers .json files in INPUT_DIR, filters out already processed ones, processes each, and prints an ingestion summary (total rows, time, throughput).
Data Flow
JSON file → read → validate → convert → batch → INSERT INTO olap_db.transactions
