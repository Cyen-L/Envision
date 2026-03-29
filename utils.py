import os
import sys
import json
from clickhouse_driver import Client

# Utility functions for ClickHouse interactions and configuration management
def run_query(client, query, params=None):
    """Helper to execute a query and print results."""

    # Execute the query and return results, with error handling
    try:
        result = client.execute(query, params=params)
        return result
    
    # Handle exceptions during query execution
    except Exception as e:
        print(f"Error executing query: {e}")
        return None
    
def get_client(CONFIG):
    """Create and return a ClickHouse client."""

    # Attempt to create a ClickHouse client using provided configuration parameters
    try:

        # Create a ClickHouse client using configuration parameters
        client = Client(
            host=CONFIG['clickhouse']['host'],
            port=CONFIG['clickhouse']['port'],
            user=CONFIG['clickhouse']['user'],
            password=CONFIG['clickhouse']['password'],
            database=CONFIG['clickhouse']['database']
        )
        return client
    
    # Handle exceptions during client creation
    except Exception as e:
        print(f"Failed to create ClickHouse client: {e}", file=sys.stderr)
        sys.exit(1)

CONFIG_PATH = os.getenv('APP_CONFIG', 'config.json')
def load_config():
    """Load configuration from JSON file, with fallback to environment variables."""
    try:
        with open(CONFIG_PATH, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"Config file {CONFIG_PATH} not found.", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error parsing config file {CONFIG_PATH}: {e}", file=sys.stderr)
        sys.exit(1)

    return config

# Load config once when the module is imported
CONFIG = load_config()

def table_exists(client, table_name):
    """Check if a table exists in the target database."""
    query = f"""
        SELECT 1 FROM system.tables
        WHERE database = '{CONFIG['clickhouse']['database']}' AND name = '{table_name}'
    """
    result = run_query(client, query)
    return bool(result)