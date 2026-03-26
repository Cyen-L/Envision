import os
import sys
import json
from clickhouse_driver import Client

def run_query(client, query, params=None):
    """Helper to execute a query and print results."""
    try:
        result = client.execute(query, params=params)
        return result
    except Exception as e:
        print(f"Error executing query: {e}")
        return None
    
def get_client(CONFIG):
    """Create and return a ClickHouse client."""
    try:
        client = Client(
            host=CONFIG['clickhouse']['host'],
            port=CONFIG['clickhouse']['port'],
            user=CONFIG['clickhouse']['user'],
            password=CONFIG['clickhouse']['password'],
            database=CONFIG['clickhouse']['database']
        )
        return client
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