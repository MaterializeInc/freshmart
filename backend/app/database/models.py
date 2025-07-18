"""Data models and domain classes for database operations."""
from dataclasses import dataclass
from typing import Dict, Any, Optional
import time


@dataclass
class QueryMetrics:
    """Represents query performance metrics for a data source."""
    qps: float = 0.0
    latency: float = 0.0
    end_to_end_latency: float = 0.0
    price: float = 0.0
    last_updated: float = 0.0
    freshness: Optional[float] = None
    refresh_duration: Optional[float] = None


@dataclass
class ContainerStats:
    """Container resource usage statistics."""
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    last_updated: float = 0.0


@dataclass
class HeartbeatInfo:
    """Heartbeat information for tracking data freshness."""
    insert_time: Optional[float] = None
    id: Optional[int] = None
    ts: Optional[Any] = None


@dataclass
class PromotionResult:
    """Result of a promotion toggle operation."""
    status: str
    updated_at: Optional[Any] = None
    active: Optional[bool] = None
    message: Optional[str] = None


@dataclass
class IndexStatus:
    """Status of database index operations."""
    message: str
    index_exists: bool


@dataclass
class IsolationLevelResult:
    """Result of isolation level operations."""
    status: str
    isolation_level: str


@dataclass
class TrafficState:
    """Current traffic state for all data sources."""
    view: bool = True
    materialized_view: bool = True
    materialize: bool = True
    
    def to_dict(self) -> Dict[str, bool]:
        return {
            "view": self.view,
            "materialized_view": self.materialized_view,
            "materialize": self.materialize
        }


@dataclass 
class RefreshIntervalResult:
    """Result of refresh interval configuration."""
    status: str
    refresh_interval: int


class StatsCalculator:
    """Utility class for calculating statistical metrics."""
    
    @staticmethod
    def calculate_percentile(values: list, percentile: float) -> float:
        """Calculate a specific percentile from a list of values."""
        if not values:
            return 0.0
        sorted_values = sorted(values)
        index = int(len(sorted_values) * percentile)
        return sorted_values[min(index, len(sorted_values) - 1)]
    
    @staticmethod
    def calculate_basic_stats(values: list) -> Dict[str, float]:
        """Calculate basic statistics (max, average, p99) from values."""
        if not values:
            return {"max": 0.0, "average": 0.0, "p99": 0.0}
        
        return {
            "max": max(values),
            "average": sum(values) / len(values),
            "p99": StatsCalculator.calculate_percentile(values, 0.99)
        }