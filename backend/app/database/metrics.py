"""Database metrics and monitoring functionality."""
import time
import asyncio
import asyncpg
import logging
from typing import Dict, Any
from .pools import postgres_pool, materialize_pool
from .config import mz_schema, current_isolation_level, traffic_enabled
from .stats import query_stats, stats_lock, calculate_stats

logger = logging.getLogger(__name__)


async def get_query_metrics(product_id: int) -> Dict[str, Any]:
    """Get comprehensive query metrics for all data sources."""
    current_time = time.time()
    response = {'timestamp': int(current_time * 1000), 'isolation_level': current_isolation_level}
    try:
        async with postgres_pool.acquire() as pg_conn:
            mv_freshness = await pg_conn.fetchrow("""
                SELECT EXTRACT(EPOCH FROM (NOW() - last_refresh)) as age,
                       refresh_duration
                FROM materialized_view_refresh_log
                WHERE view_name = 'mv_dynamic_pricing'
            """)
            pg_heartbeat = await pg_conn.fetchrow("""
                SELECT id, ts, NOW() as current_ts
                FROM heartbeats
                ORDER BY id DESC
                LIMIT 1
            """)
        materialize_lag = 0.0
        try:
            async with materialize_pool.acquire() as mz_conn:
                logger.debug("Fetching Materialize heartbeat...")
                mz_heartbeat = await mz_conn.fetchrow(f'''
                    SELECT id, ts
                    FROM {mz_schema}.heartbeats
                    ORDER BY ts DESC
                    LIMIT 1
                ''')
                logger.debug(f"Materialize heartbeat: {mz_heartbeat}")
                if pg_heartbeat and mz_heartbeat:
                    pg_id = pg_heartbeat['id']
                    mz_id = mz_heartbeat['id']
                    if pg_id > mz_id:
                        lag = (pg_heartbeat['ts'] - mz_heartbeat['ts']).total_seconds()
                        lag += (pg_heartbeat['current_ts'] - pg_heartbeat['ts']).total_seconds()
                        materialize_lag = max(0.0, lag)
        except asyncio.TimeoutError:
            logger.warning("Timeout fetching Materialize metrics")
            query_stats["materialize"]["current_stats"].update({
                "qps": None,
                "latency": None,
                "end_to_end_latency": None,
                "price": None,
                "freshness": None,
                "last_updated": current_time
            })
        except asyncpg.exceptions.UndefinedTableError as e:
            logger.warning(f"Materialize table not found: {str(e)}")
            query_stats["materialize"]["current_stats"].update({
                "qps": None,
                "latency": None,
                "end_to_end_latency": None,
                "price": None,
                "freshness": None,
                "last_updated": current_time
            })
        except Exception as e:
            logger.error(f"Error getting Materialize metrics: {str(e)}", exc_info=True)
            query_stats["materialize"]["current_stats"].update({
                "qps": None,
                "latency": None,
                "end_to_end_latency": None,
                "price": None,
                "freshness": None,
                "last_updated": current_time
            })
        for source in ['view', 'materialized_view', 'materialize']:
            stats = query_stats[source]["current_stats"]
            is_fresh = current_time - stats["last_updated"] <= 2.0
            response.update({
                f"{source}_latency": stats["latency"] if is_fresh else None,
                f"{source}_end_to_end_latency": stats["end_to_end_latency"] if is_fresh else None,
                f"{source}_price": stats["price"] if is_fresh else None,
                f"{source}_qps": stats["qps"] if is_fresh else None,
                f"{source}_stats": calculate_stats(query_stats[source]["latencies"]) if is_fresh else None,
                f"{source}_end_to_end_stats": calculate_stats(
                    query_stats[source]["end_to_end_latencies"]) if is_fresh else None
            })
            if source == 'materialized_view':
                refresh_durations = query_stats[source]["refresh_durations"]
                if refresh_durations and is_fresh:
                    refresh_stats = {
                        'max': max(refresh_durations),
                        'average': sum(refresh_durations) / len(refresh_durations),
                        'p99': sorted(refresh_durations)[int(len(refresh_durations) * 0.99)]
                        if len(refresh_durations) >= 100 else max(refresh_durations)
                    }
                else:
                    refresh_stats = None
                response.update({
                    'materialized_view_freshness': float(mv_freshness['age']) if mv_freshness and is_fresh else None,
                    'materialized_view_refresh_duration': float(
                        mv_freshness['refresh_duration']) if mv_freshness and is_fresh else None,
                    'materialized_view_refresh_stats': refresh_stats
                })
            elif source == 'materialize':
                response.update({'materialize_freshness': materialize_lag if is_fresh else None})
    except Exception as e:
        logger.error(f"Error in get_query_metrics: {str(e)}", exc_info=True)
        raise
    return response


async def get_container_stats() -> Dict[str, Any]:
    """Return CPU and memory usage stats for PostgreSQL and Materialize."""
    current_time = time.time()
    response = {"timestamp": int(current_time * 1000)}
    for container_type in ["postgres_stats", "materialize_stats"]:
        stats = query_stats.get(container_type)
        if not stats:
            response[container_type] = {"cpu_usage": None, "memory_usage": None, "cpu_stats": None,
                                        "memory_stats": None}
            continue
        is_fresh = current_time - stats["current_stats"]["last_updated"] <= 10.0
        if not is_fresh:
            response[container_type] = {"cpu_usage": None, "memory_usage": None, "cpu_stats": None,
                                        "memory_stats": None}
            continue
        cpu_stats = None
        memory_stats = None
        if stats["cpu_measurements"]:
            cpu_stats = {
                "max": max(stats["cpu_measurements"]),
                "average": sum(stats["cpu_measurements"]) / len(stats["cpu_measurements"]),
                "p99": sorted(stats["cpu_measurements"])[int(len(stats["cpu_measurements"]) * 0.99)] if len(
                    stats["cpu_measurements"]) >= 100 else max(stats["cpu_measurements"])
            }
        if stats["memory_measurements"]:
            memory_stats = {
                "max": max(stats["memory_measurements"]),
                "average": sum(stats["memory_measurements"]) / len(stats["memory_measurements"]),
                "p99": sorted(stats["memory_measurements"])[int(len(stats["memory_measurements"]) * 0.99)] if len(
                    stats["memory_measurements"]) >= 100 else max(stats["memory_measurements"])
            }
        response[container_type] = {
            "cpu_usage": stats["current_stats"]["cpu_usage"],
            "memory_usage": stats["current_stats"]["memory_usage"],
            "cpu_stats": cpu_stats,
            "memory_stats": memory_stats
        }
    return response


async def get_traffic_state() -> Dict[str, bool]:
    """Return the current state of traffic toggles for all sources."""
    logger.debug("Getting traffic state")
    state = {
        "view": traffic_enabled["view"],
        "materialized_view": traffic_enabled["materialized_view"],
        "materialize": traffic_enabled["materialize"]
    }
    logger.debug(f"Current traffic state: {state}")
    return state


async def toggle_traffic(source: str) -> bool:
    """Toggle traffic for a specific source."""
    global traffic_enabled
    traffic_enabled[source] = not traffic_enabled[source]
    logger.info(f"Traffic for {source} is now {'enabled' if traffic_enabled[source] else 'disabled'}")
    return traffic_enabled[source]


async def configure_refresh_interval(interval: int) -> Dict[str, Any]:
    """Configure the refresh interval for materialized view."""
    global refresh_interval
    if interval < 1:
        raise ValueError("Interval must be at least 1 second")
    refresh_interval = interval
    return {"status": "success", "refresh_interval": interval}


async def update_freshness_metrics() -> None:
    """Update freshness metrics for materialized view and Materialize."""
    while True:
        try:
            async with postgres_pool.acquire() as conn:
                logger.info(
                    f"Current Freshness Values:\n  - MV: {query_stats['materialized_view']['current_stats']['freshness']:.2f}s"
                    f"\n  - Materialize: {query_stats['materialize']['current_stats']['freshness']:.2f}s")
                try:
                    mv_stats = await conn.fetchrow("""
                        SELECT last_refresh, refresh_duration,
                               EXTRACT(EPOCH FROM (NOW() - last_refresh)) as age,
                               NOW() as current_ts
                        FROM materialized_view_refresh_log
                        WHERE view_name = 'mv_dynamic_pricing'
                    """)
                    if mv_stats:
                        async with stats_lock:
                            stats = query_stats["materialized_view"]
                            duration = float(mv_stats['refresh_duration'])
                            age = float(mv_stats['age'])
                            stats["refresh_durations"].append(duration)
                            if len(stats["refresh_durations"]) > 100:
                                stats["refresh_durations"].pop(0)
                            stats["current_stats"]["freshness"] = age
                            stats["current_stats"]["refresh_duration"] = duration
                            stats["current_stats"]["last_updated"] = time.time()
                            logger.debug(f"Updated MV freshness: {age:.2f}s (duration: {duration:.2f}s)")
                except Exception as e:
                    logger.error(f"Error updating MV freshness: {str(e)}")
            try:
                async with postgres_pool.acquire() as conn:
                    pg_heartbeat = await conn.fetchrow(f"""
                        SELECT id, ts, NOW() as current_ts
                        FROM {mz_schema}.heartbeats
                        ORDER BY ts DESC
                        LIMIT 1
                    """)
                async with materialize_pool.acquire() as conn:
                    mz_heartbeat = await conn.fetchrow(f"""
                        SELECT id, ts
                        FROM {mz_schema}.heartbeats
                        ORDER BY ts DESC
                        LIMIT 1
                    """)
                if pg_heartbeat and mz_heartbeat:
                    pg_id = pg_heartbeat['id']
                    mz_id = mz_heartbeat['id']
                    pg_ts = pg_heartbeat['ts']
                    mz_ts = mz_heartbeat['ts']
                    current_ts = pg_heartbeat['current_ts']
                    async with stats_lock:
                        stats = query_stats["materialize"]["current_stats"]
                        if pg_id > mz_id:
                            freshness = (pg_ts - mz_ts).total_seconds() + (current_ts - pg_ts).total_seconds()
                            stats["freshness"] = max(0.0, freshness)
                        else:
                            stats["freshness"] = 0.0
                        stats["last_updated"] = time.time()
                    logger.debug(
                        f"Updated Materialize freshness: {stats['freshness']:.2f}s (PG ID: {pg_id}, MZ ID: {mz_id})")
            except Exception as e:
                logger.error(f"Error updating Materialize freshness: {str(e)}")
            await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Error in update_freshness_metrics: {str(e)}")
            await asyncio.sleep(1)