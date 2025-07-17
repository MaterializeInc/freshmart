"""Database module - provides a clean API for database operations."""

# Import and expose the main functionality
from .pools import init_pools, postgres_pool, materialize_pool
from .queries import (
    measure_query_time, get_database_size, toggle_promotion,
    get_categories, add_product, get_category_subtotals
)
from .materialize import (
    toggle_view_index, get_view_index_status, get_isolation_level,
    toggle_isolation_level, check_materialize_index_exists,
    get_concurrency_limits
)
from .background import (
    create_heartbeat, refresh_materialized_view, auto_refresh_materialized_view,
    add_to_cart, update_inventory_levels, execute_query, continuous_query_load,
    collect_container_stats
)
from .metrics import (
    get_query_metrics, get_container_stats, get_traffic_state,
    toggle_traffic, configure_refresh_interval, update_freshness_metrics
)
from .config import (
    latest_heartbeat, current_isolation_level, refresh_interval,
    mz_schema, active_tasks, traffic_enabled, source_to_stats,
    response_mapping, source_names
)

# Import new domain-specific classes
from .models import (
    QueryMetrics, ContainerStats, HeartbeatInfo, PromotionResult,
    IndexStatus, IsolationLevelResult, TrafficState, RefreshIntervalResult,
    StatsCalculator
)
from .collectors import (
    QueryMetricsCollector, ContainerStatsCollector, RefreshMetricsCollector
)
from .managers import (
    ProductManager, PromotionManager, MaterializeManager, 
    TrafficManager, DatabaseMetricsManager
)

# Import error handling
from .exceptions import (
    DatabaseError, ConnectionError, PoolExhaustionError, QueryTimeoutError,
    TransactionError, ProductNotFoundError, PromotionError, IndexError,
    MaterializeError, MetricsCollectionError, ConfigurationError,
    RetryableError, TemporaryConnectionError, TemporaryQueryError
)
from .error_handling import (
    handle_database_errors, retry_on_failure, ErrorContext, CircuitBreaker,
    connection_circuit_breaker, query_circuit_breaker
)

# Re-export commonly used items
__all__ = [
    # Pool management
    'init_pools', 'postgres_pool', 'materialize_pool',
    
    # Core queries
    'measure_query_time', 'get_database_size', 'toggle_promotion',
    'get_categories', 'add_product', 'get_category_subtotals',
    
    # Materialize operations
    'toggle_view_index', 'get_view_index_status', 'get_isolation_level',
    'toggle_isolation_level', 'check_materialize_index_exists',
    'get_concurrency_limits',
    
    # Background tasks
    'create_heartbeat', 'refresh_materialized_view', 'auto_refresh_materialized_view',
    'add_to_cart', 'update_inventory_levels', 'execute_query', 'continuous_query_load',
    'collect_container_stats',
    
    # Metrics and monitoring
    'get_query_metrics', 'get_container_stats', 'get_traffic_state',
    'toggle_traffic', 'configure_refresh_interval', 'update_freshness_metrics',
    
    # Configuration
    'latest_heartbeat', 'current_isolation_level', 'refresh_interval',
    'mz_schema', 'active_tasks', 'traffic_enabled', 'source_to_stats',
    'response_mapping', 'source_names',
    
    # Domain models
    'QueryMetrics', 'ContainerStats', 'HeartbeatInfo', 'PromotionResult',
    'IndexStatus', 'IsolationLevelResult', 'TrafficState', 'RefreshIntervalResult',
    'StatsCalculator',
    
    # Collectors
    'QueryMetricsCollector', 'ContainerStatsCollector', 'RefreshMetricsCollector',
    
    # Managers
    'ProductManager', 'PromotionManager', 'MaterializeManager', 
    'TrafficManager', 'DatabaseMetricsManager',
    
    # Error handling
    'DatabaseError', 'ConnectionError', 'PoolExhaustionError', 'QueryTimeoutError',
    'TransactionError', 'ProductNotFoundError', 'PromotionError', 'IndexError',
    'MaterializeError', 'MetricsCollectionError', 'ConfigurationError',
    'RetryableError', 'TemporaryConnectionError', 'TemporaryQueryError',
    'handle_database_errors', 'retry_on_failure', 'ErrorContext', 'CircuitBreaker'
]