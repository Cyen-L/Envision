# Standard library imports
import json
import os
import time
from decimal import Decimal
from datetime import datetime
from clickhouse_driver import Client

# Import from utils.py
from utils import get_client, CONFIG, run_query

# Config
INPUT_DIR = 'json_files'
BATCH_SIZE = 1000000
TRACKING_FILE = 'ingestion_tracking.txt'

# Function to load the set of processed files from the tracking file
def load_processed_files():
    """Load the set of processed files from the tracking file."""
    
    # If the tracking file does not exist, return an empty set
    if not os.path.exists(TRACKING_FILE):
        return set()
    
    # Read the tracking file and load the processed files into a set
    with open(TRACKING_FILE, 'r') as f:
        return set(line.strip() for line in f)

# Function to mark a file as processed by adding it to the tracking file
def mark_file_processed(filename):
    """Mark a file as processed by adding it to the tracking file."""
    
    # Append the filename to the tracking file
    with open(TRACKING_FILE, 'a') as f:
        f.write(filename + '\n')

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
            transfer_amount = Decimal(row[6])

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
                transaction_time, 
                int(time.time())
            ))

            # If the batch size reach the batch size
            if len(batch) >= batch_size:

                # Fault-tolerant execution of the query
                try:
                    
                    # Execute the input statement to ingest data into database
                    client.execute(
                        "INSERT INTO olap_db.transactions VALUES",
                        batch
                    )
                
                # Exception handling for query execution
                except Exception as e:
                    print(f"Error fetching transaction counts: {e}")

                # Add the row number ingested into variable for counting
                rows_inserted += len(batch)

                # Initialize the batach again
                batch = []
        
        # Handle situation where there is remaining batch
        if batch:

            # Fault-tolerant execution of the query
            try:

                # Execute the input statement to ingest data into database
                client.execute(
                    "INSERT INTO olap_db.transactions VALUES",
                    batch
                )
            
            # Exception handling for query execution
            except Exception as e:
                print(f"Error fetching transaction counts: {e}")
            
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

        # Load the set of processed files from the tracking file
        processed_files = load_processed_files()

        # Loopn over each file name
        for file_name in json_files:

            # Check if the file has been processed before by looking into the set of processed files
            if file_name in processed_files:
                print(f'Skipping {file_name} (already processed)')
                continue

            print(f'Processing {file_name}.....')

            # Construct file path
            file_path = os.path.join(INPUT_DIR, file_name)

            # Process the file and get the number of rows inserted
            rows = process_file(file_path, client, BATCH_SIZE)

            # Counting function for row
            total_rows += rows

            # Mark the file as processed by adding it to the tracking file
            mark_file_processed(file_name)
        
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