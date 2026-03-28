#!/usr/bin/env python3
"""
Create pre‑aggregated tables in ClickHouse and optionally populate them
with data from the raw transactions table, using ReplacingMergeTree to
avoid duplicate rows.
"""

# Import necessary libraries
from datetime import datetime, timedelta

# Import from utils.py
from utils import run_query, get_client, table_exists, CONFIG


# Populate the 30‑minute aggregation table with data from the raw transactions.
def aggregate_agg_username_30min():

    # Check if the agg_username_30min table exists
    if not table_exists(client, "agg_username_30min"):
    # Create agg_username_30min table
        query = """
        CREATE TABLE IF NOT EXISTS olap_db.agg_username_30min (
            window_start  DateTime64(3),
            site_id       UInt32,
            username      String,
            txn_count     UInt64,
            total_amount  Decimal(18, 2)
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (window_start, site_id, username)
        """

        # Fault-tolerant execution of the query
        try:

            # Run the query and store in the result
            run_query(client, query)

            # Since table not existed, full aggregation is needed, thus no where clause is needed
            where_clause = ""
        
        # Exception handling for query execution
        except Exception as e:
            print(f"Error fetching transaction counts: {e}")

    else:
        # Get latest window_start from existing table
        max_window_query = f"SELECT max(window_start) FROM {CONFIG['clickhouse']['database']}.agg_username_30min"
        max_window_result = run_query(client, max_window_query)

        # If there is a valid max_window_start
        if max_window_result and max_window_result[0][0] is not None:
            
            # Retrieve the max_window_start value
            max_window_start = max_window_result[0][0]

            # Construct a where clause to only process new transactions
            where_clause = f"WHERE transaction_time >= '{max_window_start}'"
        
        # If the table exists but has no valid max_window_start
        else:

            # Not where clause needed
            where_clause = ""
    
    # Construct the SQL query
    query = f"""
        INSERT INTO {CONFIG['clickhouse']['database']}.agg_username_30min (window_start, site_id, username, txn_count, total_amount)
        SELECT
            toStartOfInterval(transaction_time, INTERVAL 30 MINUTE) AS window_start,
            site_id,
            username,
            count(*) AS txn_count,
            sum(transfer_amount) AS total_amount
        FROM {CONFIG['clickhouse']['database']}.transactions
        {where_clause}
        GROUP BY window_start, site_id, username
        """
    
    # Excep5tion handling for query execution
    try:

        # Run the query
        run_query(client, query)
    
    # Exception handling for query execution
    except Exception as e:
        print(f"Error fetching transaction counts: {e}")

def aggregate_agg_site_daily():

    # Check if the agg_site_daily table exists
    if not table_exists(client, "agg_site_daily"):

        # Create agg_site_daily table
        query = """
        CREATE TABLE IF NOT EXISTS olap_db.agg_site_daily (
            day           Date,
            site_id       UInt32,
            txn_count     UInt64,
            total_amount  Decimal(18, 2)
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (day, site_id)
        """

        # Fault-tolerant execution of the query
        try:

            # Run the query and store in the result
            run_query(client, query)

            # Since table not existed, full aggregation is needed, thus no where clause is needed
            where_clause = ""
    
        # Exception handling for query execution
        except Exception as e:
            print(f"Error fetching transaction counts: {e}")
    
    else:
        # Get latest window_start from existing table
        max_date_query = f"SELECT max(day) FROM {CONFIG['clickhouse']['database']}.agg_site_daily"
        max_date_result = run_query(client, max_date_query)

        # If there is a valid max_window_start
        if max_date_result and max_date_result[0][0] is not None:
            
            # Retrieve the max_window_start value
            max_date_start = max_date_result[0][0]

            # Construct a where clause to only process new transactions
            where_clause = f"WHERE transaction_time >= '{max_date_start}'"
        
        # If the table exists but has no valid max_window_start
        else:

            # Not where clause needed
            where_clause = ""
    
    # Construct the SQL query
    query = f"""
        INSERT INTO {CONFIG['clickhouse']['database']}.agg_site_daily (day, site_id, txn_count, total_amount)
        SELECT
            toDate(transaction_time) AS day,
            site_id,
            count(*) AS txn_count,
            sum(transfer_amount) AS total_amount
        FROM {CONFIG['clickhouse']['database']}.transactions
        {where_clause}
        GROUP BY day, site_id
        HAVING (day, site_id) NOT IN (
            SELECT day, site_id FROM agg_site_daily
        )
        """
    
    # Excep5tion handling for query execution
    try:

        # Run the query
        run_query(client, query)
    
    # Exception handling for query execution
    except Exception as e:
        print(f"Error fetching transaction counts: {e}")

if __name__ == "__main__":

    # Connect to ClickHouse
    client = get_client(CONFIG)

    # Create the pre‑aggregated tables
    aggregate_agg_username_30min()
    aggregate_agg_site_daily()

    # Disconnect the ClickHouse client
    client.disconnect()

    # 3. (Optional) Populate the tables with data from raw transactions
    #    Uncomment the lines below to populate with the entire dataset.
    #    To avoid double‑counting, you can run this script periodically and
    #    limit the range to newly added data (using timestamps / dates).
    #
    # populate_username_30min()   # all data
    # populate_site_daily()       # all data
    #
    # Example: populate only transactions from the last 24 hours
    # now = datetime.now()
    # yesterday = now - timedelta(days=1)
    # populate_username_30min(start_time=yesterday, end_time=now)
    # populate_site_daily(start_date=yesterday.date(), end_date=now.date())
