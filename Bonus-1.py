"""
Script to fetch daily username count by site ID within date range from ClickHouse database.

Usage:
python Bonus-1.py [--start-date {YYYY-MM-DD}] [--end-date {YYYY-MM-DD}] [--site-id]

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
    parser.add_argument('--site-id', action = 'store_true')
    args = parser.parse_args()

    # Connect to ClickHouse
    client = get_client(CONFIG)

    # Decide the clause column based on whether site_id is included
    clause_col = "day" + (", site_id" if args.site_id else "")

    # Construct the SQL query
    query = f"""
        SELECT toDate(transaction_time) AS {clause_col}, uniq(username) AS total_count
        FROM {CONFIG['clickhouse']['database']}.transactions
        WHERE transaction_time BETWEEN '{args.start_date} 00:00:00.000' AND '{args.end_date} 23:59:59.999'
        GROUP BY {clause_col}
        ORDER BY {clause_col};
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
        if args.site_id:
            print("\n=== Bonus-1: Daily unique username count by site ID within date range ===")
            for row in result:
                print(f"{row[0]}\t{row[1]}\t{row[2]}")
        else:
            print("\n=== Bonus-1: Daily unique username count within date range ===")
            for row in result:
                print(f"{row[0]}\t{row[1]}")
    
    # Finally block to discounnect the Clickhouse
    finally:
        client.disconnect()

if __name__ == "__main__":
    main()