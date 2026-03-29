"""
Script to fetch daily counts by currency from a ClickHouse database.

Usage:
python C3.py [--sort-by {day|currency_code|total_count}] [--descending]

"""

# Standard library imports
import argparse
import sys
from clickhouse_driver import Client

# Import from utils.py
from utils import run_query, get_client, CONFIG

# Initialize allowed columns for sorting
ALLOWED_COLUMNS = ['day', 'currency_code', 'total_count']

# Function to parse the --sort-by argument and validate it
def parse_sort_by(s):
    cols = [c.strip() for c in s.split(',')]
    for col in cols:
        if col not in ALLOWED_COLUMNS:
            raise argparse.ArgumentTypeError(f"Invalid column: {col}. Allowed: {', '.join(ALLOWED_COLUMNS)}")
    return cols


def main():

    # Parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--sort-by', type=parse_sort_by, default=['day', 'currency_code'])
    parser.add_argument('--descending', action = 'store_false')
    args = parser.parse_args()

    # Connect to ClickHouse
    client = get_client(CONFIG)

    # Construct the SQL query
    query = f"""
        SELECT toDate(transaction_time) AS day, currency_code, COUNT(*) AS total_count 
        FROM {CONFIG['clickhouse']['database']}.transactions 
        GROUP BY day, currency_code 
        ORDER BY {', '.join(f"{col} {"ASC" if args.descending else "DESC"}" for col in args.sort_by)};
    """

    # Fault-tolerant execution of the query
    try:

        # Run the query and store in the result
        result = run_query(client, query)
    
    # Exception handling for query execution
    except Exception as e:
        print(f"Error fetching transaction counts: {e}")
    
    # If there is no exception
    else:

        # Print the result in a readable format
        print("\n=== C3: Daily Counts by Currency ===")
        if result:
            for row in result:
                print(f"{row[0]}\t{row[1]}\t{row[2]}")
    
    # Finally block to discounnect the Clickhouse
    finally:
        client.disconnect()

if __name__ == "__main__":
    main()