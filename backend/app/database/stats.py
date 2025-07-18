"""Statistics collection and calculation."""
import time
import asyncio
import logging
from typing import Dict, List, Any
from .config import config

logger = logging.getLogger(__name__)

# Query statistics storage
query_stats: Dict[str, Dict[str, Any]] = {
    "view": {
        "counts": [],
        "timestamps": [],
        "latencies": [],
        "end_to_end_latencies": [],
        "current_stats": {
            "qps": 0.0,
            "latency": 0.0,
            "end_to_end_latency": 0.0,
            "price": 0.0,
            "last_updated": 0.0
        }
    },
    "materialized_view": {
        "counts": [],
        "timestamps": [],
        "latencies": [],
        "end_to_end_latencies": [],
        "refresh_durations": [],
        "current_stats": {
            "qps": 0.0,
            "latency": 0.0,
            "end_to_end_latency": 0.0,
            "price": 0.0,
            "last_updated": 0.0,
            "freshness": 0.0,
            "refresh_duration": 0.0
        }
    },
    "materialize": {
        "counts": [],
        "timestamps": [],
        "latencies": [],
        "end_to_end_latencies": [],
        "current_stats": {
            "qps": 0.0,
            "latency": 0.0,
            "end_to_end_latency": 0.0,
            "price": 0.0,
            "last_updated": 0.0,
            "freshness": 0.0
        }
    },
    "cpu": {
        "measurements": [],
        "timestamps": [],
        "current_stats": {
            "usage": 0.0,
            "last_updated": 0.0
        }
    }
}

# Lock for updating stats
stats_lock = asyncio.Lock()


def calculate_qps(source: str) -> float:
    """Calculate queries per second for a given source."""
    stats = query_stats[source]
    current_time = time.time()
    cutoff_time = current_time - config.window_size
    while stats["timestamps"] and stats["timestamps"][0] < cutoff_time:
        stats["counts"].pop(0)
        stats["timestamps"].pop(0)
    if not stats["timestamps"]:
        return 0.0
    total_queries = sum(stats["counts"])
    if len(stats["timestamps"]) >= 2:
        time_span = max(config.window_size, stats["timestamps"][-1] - stats["timestamps"][0])
    else:
        time_span = config.window_size
    qps = total_queries / time_span
    logger.debug(f"QPS calculation for {source}: {total_queries} queries in {time_span:.2f}s = {qps:.2f} QPS")
    return qps


def calculate_stats(latencies: List[float]) -> Dict[str, float]:
    """Calculate statistics (max, average, p99) from a list of latencies.
       Latencies are converted to milliseconds unless they are refresh durations."""
    if not latencies:
        return {"max": 0.0, "average": 0.0, "p99": 0.0}
    values = []
    for val in latencies:
        # Leave refresh durations in seconds; convert others to ms.
        if "refresh_durations" in str(latencies):
            values.append(val)
        else:
            values.append(val * 1000)
    stats = {
        "max": max(values),
        "average": sum(values) / len(values),
        "p99": sorted(values)[int(len(values) * 0.99)] if len(values) >= 100 else max(values)
    }
    unit = "s" if "refresh_durations" in str(latencies) else "ms"
    logger.debug(
        f"Stats calculation for {len(values)} values ({unit}): max={stats['max']:.2f}{unit}, "
        f"avg={stats['average']:.2f}{unit}, p99={stats['p99']:.2f}{unit}"
    )
    return stats