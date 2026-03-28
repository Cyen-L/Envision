"""
Script to fetch daily transaction counts from a ClickHouse database.

Usage:
python C1.py [--sort-by {day, total_count}] [--descending]

"""

# Standard library imports
import argparse
import sys
from clickhouse_driver import Client

# Import from utils.py
from utils import run_query, get_client, CONFIG

def main():

    # Parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--sort-by', default = 'day')
    parser.add_argument('--descending', action = 'store_false')
    args = parser.parse_args()

    # Test if the input argument is valid
    if args.sort_by not in ('day', 'total_count'):
        print("Error: --sort-by must be 'day' or 'total_count'")
        sys.exit(1)

    # Connect to ClickHouse
    client = get_client(CONFIG)

    # Construct the SQL query
    query = f"""
        SELECT toDate(transaction_time) AS day, COUNT(*) AS total_count
        FROM {CONFIG['clickhouse']['database']}.transactions
        GROUP BY day
        ORDER BY {args.sort_by} {"ASC" if args.descending else "DESC"};
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
        print("\n=== C1: Daily Total Transaction Count ===")
        if result:
            for row in result:
                print(f"{row[0]}\t{row[1]}")
    
    # Finally block to discounnect the Clickhouse
    finally:
        client.disconnect()

if __name__ == "__main__":
    main()