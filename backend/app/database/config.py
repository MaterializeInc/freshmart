"""Database configuration and constants."""
import os
from dataclasses import dataclass
from typing import Dict, Set, Any, Optional
from dotenv import load_dotenv

load_dotenv()


@dataclass
class DatabaseConfig:
    """Configuration settings for database operations."""
    
    # PostgreSQL settings
    db_user: str = os.getenv('DB_USER', 'postgres')
    db_password: str = os.getenv('DB_PASSWORD', 'postgres')
    db_name: str = os.getenv('DB_NAME', 'postgres')
    db_host: str = os.getenv('DB_HOST', 'localhost')
    db_port: int = int(os.getenv('DB_PORT', '5432'))
    
    # Materialize settings
    mz_user: str = os.getenv('MZ_USER', 'materialize')
    mz_password: str = os.getenv('MZ_PASSWORD', 'materialize')
    mz_name: str = os.getenv('MZ_NAME', 'materialize')
    mz_host: str = os.getenv('MZ_HOST', 'localhost')
    mz_port: int = int(os.getenv('MZ_PORT', '6875'))
    mz_schema: str = os.getenv('MZ_SCHEMA', 'public')
    
    # Connection pool settings
    pool_min_size: int = 2
    pool_max_size: int = 20
    command_timeout: float = 120.0
    
    # Refresh and timing settings
    refresh_interval: int = 60
    window_size: int = 1  # Rolling window for QPS calculation in seconds
    pool_refresh_interval: int = 60  # Pool refresh interval in seconds
    
    # Concurrency limits
    max_stats_history: int = 100
    container_stats_interval: int = 5
    heartbeat_interval: int = 1
    cart_update_interval: int = 30
    inventory_update_interval: int = 1


@dataclass
class RuntimeState:
    """Runtime state that changes during application execution."""
    latest_heartbeat: Dict[str, Any] = None
    current_isolation_level: str = "serializable"
    refresh_interval: int = 60
    
    def __post_init__(self):
        if self.latest_heartbeat is None:
            self.latest_heartbeat = {"insert_time": None, "id": None, "ts": None}


# Global configuration instance
config = DatabaseConfig()
state = RuntimeState()

# Track active tasks per source
active_tasks: Dict[str, Set[Any]] = {
    'view': set(),
    'materialized_view': set(),
    'materialize': set()
}

# Constants for source mappings
SOURCE_TO_STATS = {
    "PostgreSQL View": "view",
    "PostgreSQL MV": "materialized_view",
    "Materialize": "materialize"
}

RESPONSE_MAPPING = {
    'view': 'view',
    'materialized_view': 'mv',  # Maps to UI's "Cached Table"
    'materialize': 'mz'  # Maps to UI's "Materialize"
}

SOURCE_NAMES = {
    'view': 'PostgreSQL View',
    'materialized_view': 'PostgreSQL MV',
    'materialize': 'Materialize'
}

# Traffic control state
traffic_enabled = {
    "view": True,
    "materialized_view": True,
    "materialize": True
}

# Backward compatibility exports
latest_heartbeat = state.latest_heartbeat
current_isolation_level = state.current_isolation_level
refresh_interval = state.refresh_interval
mz_schema = config.mz_schema
WINDOW_SIZE = config.window_size
source_to_stats = SOURCE_TO_STATS
response_mapping = RESPONSE_MAPPING
source_names = SOURCE_NAMES