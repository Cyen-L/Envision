# Standard library imports
import json
import os
import time
import argparse
from datetime import datetime
from typing import List, Tuple

# Import from utils.py
from utils import get_client, CONFIG

# Config
INPUT_DIR = 'json_files'
HOST = 'localhost'
PORT = 9000
USER ='admin'
PASSWORD ='admin'
DATABASE ='olap_db'
BATCH_SIZE = 1000000

def parse_time_ms(value):
    """Convert epoch milliseconds (int or string) to datetime object."""

    # Check if the input value is in string instance
    if isinstance(value, str):

        # Convert the value into integer
        value = int(value)
    
    # Convert the value into milliseconds
    output = datetime.fromtimestamp(value / 1000.0)

    return output

def process_file(file_path: str, client: Client, batch_size: int = 10000):
    """Process a single JSON file and insert rows in batches."""
    
    # Initialize variable counting row inserted
    rows_inserted = 0

    # Read JSON file and load into vairable
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Check if data is an instance of list
    if not isinstance(data, list):

        # Stop and process if data is not a list instance
        print(f"Ignoring {file_path}: root element is not an array")

    # Otherwise run the process
    else:

        # Intialize a list for batch input
        batch = []

        # Loop over each 
        for row in data:

            # Check if the row in data is list and whether it contain 8 element (which mapped to the 8 columns)
            if not isinstance(row, list) or len(row) != 8:

                # Print out the respective info if abnormal row found
                print(f"Ignoring malformed row in {file_path}: {row}")
                continue

            # Convert data into respective data types
            transaction_id = int(row[0])
            bill_id = int(row[1])
            site_id = int(row[2])
            username = str(row[3])
            item_id = int(row[4])
            currency_code = str(row[5])
            transfer_amount = float(row[6])

            # Parse the time into specific format with function
            transaction_time = parse_time_ms(row[7])

            # Append the data together into the batch variable
            batch.append((
                transaction_id, 
                bill_id, 
                site_id, 
                username, 
                item_id,
                currency_code, 
                transfer_amount, 
                transaction_time
            ))

            # If the batch size reach the batch size
            if len(batch) >= batch_size:
            
                # Execute the input statement to ingest data into database
                client.execute(
                    "INSERT INTO olap_db.transactions VALUES",
                    batch
                )

                # Add the row number ingested into variable for counting
                rows_inserted += len(batch)

                # Initialize the batach again
                batch = []
        
        # Handle situation where there is remaining batch
        if batch:
            client.execute(
                "INSERT INTO olap_db.transactions VALUES",
                batch
            )
            
            # Add the row number ingested into variable for counting
            rows_inserted += len(batch)
            
    return rows_inserted

def main():

    # Connect to ClickHouse
    client = get_client(CONFIG)

    # Get list of JSON files within data directory
    json_files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.json')]

    # Start looping each file (only when JSON file is found)
    if json_files:

        # Initialize variable for row counting and speed calculation
        total_rows = 0
        start_time = time.time()

        # Loopn over each file name
        for file_name in json_files:
            print(f'Processing {file_name}.....')

            # Construct file path
            file_path = os.path.join(INPUT_DIR, file_name)


            rows = process_file(file_path, client, BATCH_SIZE)

            # Counting function for row
            total_rows += rows
        
        # Get the complete time
        end_time = time.time()

        # Calculate duration taken for ingestion
        duration = end_time - start_time

        # Calculate the throughput
        throughput = total_rows / duration

        print("\n--- Ingestion Summary ---")
        print(f"Total rows inserted: {total_rows}")
        print(f"Total time: {duration:.2f} seconds")
        print(f"Throughput: {throughput:.2f} rows/sec")
        print("------------------------")

    else:
        print("No JSON files found.")

if __name__ == "__main__":
    main()