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
├── utils.py                 # Shared utilities (config loading, client creation)
├── ingest.py                # Ingestion script
├── aggregate.py             # Pre-aggregation script
├── C1.py                    # Daily total transaction count
├── C2.py                    # Daily total transfer amount
├── C3.py                    # Daily count by currency
├── C4.py                    # Top 20 usernames by amount (date range)
├── config.json              # Configuration file (see below)
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

--- Ingestion Summary ---
Total rows inserted: 5000000
Total time: 42.35 seconds
Throughput: 118036.78 rows/sec
------------------------

Analytics Queries (C1–C4)
C1: Daily Total Transaction Count
python C1.py [--sort-by {day|total_count}] [--descending]

C2: Daily Total Transfer Amount
python C2.py [--sort-by {day|total_amount}] [--descending]

C3: Daily Count by Currency
python C3.py [--sort-by {day,currency_code,total_count}] [--descending]

C4: Top 20 Usernames by Total Amount (Date Range)
python C4.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD [--sort-by {username|total_amount}] [--descending]

Pre-aggregation Tables (C5 & C6)
python aggregate.py
creates and populates two pre-aggregated tables:
agg_username_30min – 30-minute window aggregates by site_id and username
agg_site_daily – Daily aggregates by site_id
Perform an incremental refresh (only new data since last run is added) using the ReplacingMergeTree engine to handle duplicates
Print any errors encountered