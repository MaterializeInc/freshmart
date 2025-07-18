"""Data collection and metrics management classes."""
import time
import asyncio
import logging
from typing import Dict, List, Any, Tuple
from .models import QueryMetrics, ContainerStats, StatsCalculator
from .config import config, SOURCE_TO_STATS
from .stats import query_stats, stats_lock, calculate_qps

logger = logging.getLogger(__name__)


class QueryMetricsCollector:
    """Manages query performance metrics collection and calculation."""
    
    def __init__(self):
        self.stats_calculator = StatsCalculator()
    
    async def record_query_execution(
        self, 
        source: str, 
        duration: float, 
        result: Any,
        end_to_end_latency: float = None
    ) -> None:
        """Record the execution metrics for a query."""
        stats_key = SOURCE_TO_STATS.get(source)
        if not stats_key or stats_key not in query_stats:
            logger.error(f"Invalid source for metrics recording: {source}")
            return
        
        current_time = time.time()
        stats = query_stats[stats_key]
        
        async with stats_lock:
            # Add basic timing metrics
            stats["counts"].append(1)
            stats["timestamps"].append(current_time)
            stats["latencies"].append(duration)
            
            # Handle end-to-end latency if available
            if end_to_end_latency is not None:
                stats["end_to_end_latencies"].append(end_to_end_latency)
                if len(stats["end_to_end_latencies"]) > config.max_stats_history:
                    stats["end_to_end_latencies"].pop(0)
                stats["current_stats"]["end_to_end_latency"] = end_to_end_latency * 1000
            
            # Clean old data outside the window
            self._clean_old_data(stats, current_time)
            
            # Limit history size
            if len(stats["latencies"]) > config.max_stats_history:
                stats["latencies"].pop(0)
            
            # Update current stats
            stats["current_stats"]["qps"] = calculate_qps(stats_key)
            stats["current_stats"]["latency"] = duration * 1000
            stats["current_stats"]["last_updated"] = current_time
            
            # Update price if available in result
            if result and "adjusted_price" in result and result["adjusted_price"] is not None:
                try:
                    price = float(result["adjusted_price"])
                    stats["current_stats"]["price"] = price
                    logger.debug(f"Updated price for {source} to {price}")
                except (ValueError, TypeError) as e:
                    logger.error(f"Error converting price for {source}: {str(e)}")
    
    def _clean_old_data(self, stats: Dict[str, Any], current_time: float) -> None:
        """Remove data points outside the rolling window."""
        cutoff_time = current_time - config.window_size
        while stats["timestamps"] and stats["timestamps"][0] < cutoff_time:
            if stats["counts"]:
                stats["counts"].pop(0)
            if stats["timestamps"]:
                stats["timestamps"].pop(0)
            if stats["latencies"]:
                stats["latencies"].pop(0)
    
    def get_current_metrics(self, source_key: str) -> QueryMetrics:
        """Get current metrics for a specific source."""
        if source_key not in query_stats:
            return QueryMetrics()
        
        stats = query_stats[source_key]["current_stats"]
        return QueryMetrics(
            qps=stats.get("qps", 0.0),
            latency=stats.get("latency", 0.0),
            end_to_end_latency=stats.get("end_to_end_latency", 0.0),
            price=stats.get("price", 0.0),
            last_updated=stats.get("last_updated", 0.0),
            freshness=stats.get("freshness"),
            refresh_duration=stats.get("refresh_duration")
        )
    
    def calculate_aggregated_stats(self, source_key: str) -> Dict[str, float]:
        """Calculate aggregated statistics for a source."""
        if source_key not in query_stats:
            return {"max": 0.0, "average": 0.0, "p99": 0.0}
        
        latencies = query_stats[source_key]["latencies"]
        # Convert to milliseconds
        latencies_ms = [lat * 1000 for lat in latencies]
        return self.stats_calculator.calculate_basic_stats(latencies_ms)


class ContainerStatsCollector:
    """Manages container resource usage statistics."""
    
    async def record_container_stats(
        self, 
        container_type: str, 
        cpu_usage: float, 
        memory_usage: float
    ) -> None:
        """Record container resource usage statistics."""
        if container_type not in query_stats:
            logger.warning(f"Unknown container type: {container_type}")
            return
        
        current_time = time.time()
        stats = query_stats[container_type]
        
        async with stats_lock:
            stats["cpu_measurements"].append(cpu_usage)
            stats["memory_measurements"].append(memory_usage)
            stats["timestamps"].append(current_time)
            
            # Limit history size
            if len(stats["timestamps"]) > config.max_stats_history:
                stats["cpu_measurements"].pop(0)
                stats["memory_measurements"].pop(0)
                stats["timestamps"].pop(0)
            
            # Update current stats
            stats["current_stats"].update({
                "cpu_usage": cpu_usage,
                "memory_usage": memory_usage,
                "last_updated": current_time
            })
    
    def get_container_stats(self, container_type: str) -> ContainerStats:
        """Get current container statistics."""
        if container_type not in query_stats:
            return ContainerStats()
        
        stats = query_stats[container_type]["current_stats"]
        return ContainerStats(
            cpu_usage=stats.get("cpu_usage", 0.0),
            memory_usage=stats.get("memory_usage", 0.0),
            last_updated=stats.get("last_updated", 0.0)
        )
    
    def calculate_resource_stats(self, container_type: str) -> Tuple[Dict[str, float], Dict[str, float]]:
        """Calculate CPU and memory statistics for a container."""
        if container_type not in query_stats:
            empty_stats = {"max": 0.0, "average": 0.0, "p99": 0.0}
            return empty_stats, empty_stats
        
        stats = query_stats[container_type]
        cpu_stats = StatsCalculator.calculate_basic_stats(stats.get("cpu_measurements", []))
        memory_stats = StatsCalculator.calculate_basic_stats(stats.get("memory_measurements", []))
        
        return cpu_stats, memory_stats


class RefreshMetricsCollector:
    """Manages materialized view refresh metrics."""
    
    async def record_refresh_duration(self, duration: float) -> None:
        """Record a materialized view refresh duration."""
        async with stats_lock:
            stats = query_stats["materialized_view"]
            stats.setdefault("refresh_durations", []).append(duration)
            
            # Limit history size
            if len(stats["refresh_durations"]) > config.max_stats_history:
                stats["refresh_durations"].pop(0)
            
            stats["current_stats"]["refresh_duration"] = duration
    
    def get_refresh_stats(self) -> Dict[str, float]:
        """Get refresh duration statistics."""
        refresh_durations = query_stats["materialized_view"].get("refresh_durations", [])
        if not refresh_durations:
            return {"max": 0.0, "average": 0.0, "p99": 0.0}
        
        return {
            'max': max(refresh_durations),
            'average': sum(refresh_durations) / len(refresh_durations),
            'p99': StatsCalculator.calculate_percentile(refresh_durations, 0.99)
        }