#!/usr/bin/env python3
"""
Ingest multiple JSON files into ClickHouse.
Each file contains a JSON array of rows (array of arrays).
Columns: transaction_id, bill_id, site_id, username, item_id,
         currency_code, transfer_amount, transaction_time_ms
"""

# Import necessary libraries
import json
import os
import time
import argparse
from datetime import datetime
from typing import List, Tuple
from clickhouse_driver import Client


def parse_time_ms(value):
    """Convert epoch milliseconds (int or string) to datetime object."""
    if isinstance(value, str):
        value = int(value)
    # ClickHouse DateTime64 expects seconds, but we'll keep milliseconds
    # Using datetime.fromtimestamp with ms precision
    return datetime.fromtimestamp(value / 1000.0)


def process_file(file_path: str, client: Client, batch_size: int = 1000) -> int:
    """Process a single JSON file and insert rows in batches."""
    rows_inserted = 0
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if not isinstance(data, list):
        print(f"Skipping {file_path}: root element is not an array")
        return 0

    batch = []
    for row in data:
        if not isinstance(row, list) or len(row) != 8:
            print(f"Skipping malformed row in {file_path}: {row}")
            continue

        # Convert types
        transaction_id = int(row[0])
        bill_id = int(row[1])
        site_id = int(row[2])
        username = str(row[3])
        item_id = int(row[4])
        currency_code = str(row[5])
        transfer_amount = float(row[6])
        transaction_time = parse_time_ms(row[7])

        batch.append((
            transaction_id, bill_id, site_id, username, item_id,
            currency_code, transfer_amount, transaction_time
        ))

        if len(batch) >= batch_size:
            client.execute(
                "INSERT INTO analytics.transactions VALUES",
                batch
            )
            rows_inserted += len(batch)
            batch = []

    # Insert remaining rows
    if batch:
        client.execute(
            "INSERT INTO analytics.transactions VALUES",
            batch
        )
        rows_inserted += len(batch)

    return rows_inserted


def main():
    parser = argparse.ArgumentParser(description="Ingest JSON files into ClickHouse")
    parser.add_argument("--input-dir", required=True, help="Directory containing *.json files")
    parser.add_argument("--host", default="localhost", help="ClickHouse host")
    parser.add_argument("--port", type=int, default=9000, help="ClickHouse native port")
    parser.add_argument("--database", default="analytics", help="Database name")
    parser.add_argument("--user", default="default", help="Database user")
    parser.add_argument("--password", default="", help="Database password")
    parser.add_argument("--batch-size", type=int, default=1000, help="Rows per insert batch")
    args = parser.parse_args()

    # Connect to ClickHouse
    client = Client(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database
    )

    # Find all JSON files
    json_files = [f for f in os.listdir(args.input_dir) if f.endswith('.json')]
    if not json_files:
        print("No JSON files found.")
        return

    total_rows = 0
    start_time = time.time()

    for file_name in json_files:
        file_path = os.path.join(args.input_dir, file_name)
        print(f"Processing {file_name}...")
        rows = process_file(file_path, client, args.batch_size)
        total_rows += rows
        print(f"  Inserted {rows} rows.")

    end_time = time.time()
    duration = end_time - start_time
    throughput = total_rows / duration if duration > 0 else 0

    print("\n--- Ingestion Summary ---")
    print(f"Total rows inserted: {total_rows}")
    print(f"Total time: {duration:.2f} seconds")
    print(f"Throughput: {throughput:.2f} rows/sec")
    print("------------------------")


if __name__ == "__main__":
    main()