"""Database configuration and constants."""
import os
from dotenv import load_dotenv

load_dotenv()

# Global configuration
latest_heartbeat = {"insert_time": None, "id": None, "ts": None}
current_isolation_level = "serializable"  # Track the desired isolation level
refresh_interval = 60  # Default refresh interval in seconds
mz_schema = os.getenv('MZ_SCHEMA', 'public')  # Materialize schema (default: public)

# Track active tasks per source (if needed for concurrency control)
active_tasks = {
    'view': set(),
    'materialized_view': set(),
    'materialize': set()
}

# Rolling window for QPS calculation
WINDOW_SIZE = 1  # 1 second window

# Mappings for stats keys and source names
source_to_stats = {
    "PostgreSQL View": "view",
    "PostgreSQL MV": "materialized_view",
    "Materialize": "materialize"
}

response_mapping = {
    'view': 'view',
    'materialized_view': 'mv',  # Maps to UI's "Cached Table"
    'materialize': 'mz'  # Maps to UI's "Materialize"
}

source_names = {
    'view': 'PostgreSQL View',
    'materialized_view': 'PostgreSQL MV',
    'materialize': 'Materialize'
}

# Traffic control (if you want to toggle query load)
traffic_enabled = {
    "view": True,
    "materialized_view": True,
    "materialize": True
}