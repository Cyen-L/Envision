"""
Script to fetch top 20 username by total transfer amount within date range from ClickHouse database.

Usage:
python script.py [--sort-by {day, total_amount}] [--descending]

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
    parser.add_argument('--start-date', default = '2025-05-01')
    parser.add_argument('--end-date', default = '2025-06-01')
    parser.add_argument('--sort-by', default = 'username')
    parser.add_argument('--descending', action = 'store_false')
    args = parser.parse_args()

    # Test if the input argument is valid
    if args.sort_by not in ('username', 'total_amount'):
        print("Error: --sort-by must be 'username' or 'total_amount'")
        sys.exit(1)

    # Connect to ClickHouse
    client = get_client(CONFIG)

    # Construct the SQL query
    query = f"""
        SELECT username, sum(transfer_amount) AS total_amount
        FROM {CONFIG['clickhouse']['database']}.transactions
        WHERE transaction_time BETWEEN '{args.start_date} 00:00:00.000' AND '{args.end_date} 23:59:59.999'
        GROUP BY username
        ORDER BY {args.sort_by} {"ASC" if args.descending else "DESC"}
        LIMIT 20;
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
        print("\n=== C4: Top 20 username by total transfer amount within date range ===")
        if result:
            for row in result:
                print(f"{row[0]}\t{row[1]}")
    
    # Finally block to discounnect the Clickhouse
    finally:
        client.disconnect()

if __name__ == "__main__":
    main()