"""Background tasks for database operations."""
import time
import asyncio
import random
import logging
import datetime
from typing import Dict
from .pools import postgres_pool, materialize_pool
from .config import (
    latest_heartbeat, refresh_interval, active_tasks, 
    traffic_enabled, source_names, mz_schema, current_isolation_level
)
from .stats import query_stats, stats_lock, calculate_stats
from .queries import measure_query_time
from .materialize import get_concurrency_limits

logger = logging.getLogger(__name__)


async def create_heartbeat():
    """Create heartbeat entries at a fixed interval."""
    while True:
        try:
            async with postgres_pool.acquire() as conn:
                insert_time = time.time()
                async with conn.transaction():
                    result = await conn.fetchrow(
                        "INSERT INTO heartbeats (ts) VALUES (NOW()) RETURNING id, ts;"
                    )
                    await conn.execute(
                        "UPDATE products SET last_update_time = NOW() WHERE product_id = 1;"
                    )
                latest_heartbeat.update({
                    "insert_time": insert_time,
                    "id": result["id"],
                    "ts": result["ts"]
                })
                logger.debug(f"Created heartbeat {result['id']} at {insert_time}")
        except Exception as e:
            logger.error(f"Error creating heartbeat: {str(e)}")
        await asyncio.sleep(1)


async def refresh_materialized_view():
    """Refresh the materialized view with proper lock handling."""
    start_time = time.time()
    try:
        async with postgres_pool.acquire() as conn:
            await conn.execute("SET statement_timeout TO '120s'")
            await conn.execute("""
                SET LOCAL lock_timeout = '120s';
                SET LOCAL statement_timeout = '120s';
                SET LOCAL idle_in_transaction_session_timeout = '120s';
            """)
            await conn.execute("REFRESH MATERIALIZED VIEW mv_dynamic_pricing", timeout=120.0)
            refresh_duration = time.time() - start_time
            logger.debug(f"Materialized view refresh completed in {refresh_duration:.2f} seconds")
            await conn.execute("""
                INSERT INTO materialized_view_refresh_log (view_name, last_refresh, refresh_duration)
                VALUES ('mv_dynamic_pricing', NOW(), $1)
                ON CONFLICT (view_name)
                DO UPDATE SET last_refresh = EXCLUDED.last_refresh, refresh_duration = EXCLUDED.refresh_duration
            """, refresh_duration)
            async with stats_lock:
                stats = query_stats["materialized_view"]
                stats.setdefault("refresh_durations", []).append(refresh_duration)
                if len(stats["refresh_durations"]) > 100:
                    stats["refresh_durations"].pop(0)
                stats["current_stats"]["refresh_duration"] = refresh_duration
            return refresh_duration
    except asyncio.exceptions.TimeoutError as e:
        logger.error(f"Materialized view refresh timed out after {time.time() - start_time:.2f} seconds", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Error refreshing materialized view: {str(e)}", exc_info=True)
        raise


async def auto_refresh_materialized_view():
    """Automatically refresh the materialized view using a fixed interval."""
    global refresh_interval
    while True:
        try:
            if not traffic_enabled["materialized_view"]:
                logger.debug("Materialized view traffic disabled, skipping refresh")
                await asyncio.sleep(1)
                continue

            start_time = time.time()
            logger.debug(f"Starting MV refresh cycle (interval: {refresh_interval}s)")
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    await refresh_materialized_view()
                    break
                except asyncio.TimeoutError:
                    if attempt == max_retries - 1:
                        logger.error("All refresh attempts timed out")
                        break
                    logger.warning(f"Refresh attempt {attempt + 1} timed out, retrying...")
                    await asyncio.sleep(1)
                except Exception as e:
                    logger.error(f"Error in refresh attempt {attempt + 1}: {str(e)}")
                    if attempt == max_retries - 1:
                        break
                    await asyncio.sleep(1)
            elapsed = time.time() - start_time
            wait_time = max(0, refresh_interval - elapsed)
            logger.debug(f"Refresh cycle complete. Waiting {wait_time:.2f}s until next cycle.")
            await asyncio.sleep(wait_time)
        except Exception as e:
            logger.error(f"Error in auto-refresh cycle: {str(e)}", exc_info=True)
            await asyncio.sleep(1)


async def add_to_cart():
    """Automatically adds a new item to a shopping cart at a fixed internal"""
    async def insert_item():
        try:
            async with postgres_pool.acquire() as conn:
                # First, check if product_id 1 exists in the cart
                has_product_one = await conn.fetchval("""
                    SELECT EXISTS(
                        SELECT 1 FROM shopping_cart WHERE product_id = 1
                    )
                """)

                # If product_id 1 doesn't exist, insert it first
                if not has_product_one:
                    await conn.execute("""
                        INSERT INTO shopping_cart (product_id, product_name, category_id, price)
                        SELECT product_id, product_name, category_id, base_price 
                        FROM products
                        WHERE product_id = 1;
                    """)
                    
                    # Check if inventory exists for product 1
                    has_inventory = await conn.fetchval("""
                        SELECT EXISTS(
                            SELECT 1 FROM inventory WHERE product_id = 1
                        )
                    """)
                    
                    if not has_inventory:
                        # Reset the sequence to the maximum value to avoid conflicts
                        await conn.execute("""
                            SELECT setval('inventory_inventory_id_seq', 
                                        COALESCE((SELECT MAX(inventory_id) FROM inventory), 0))
                        """)
                        # Insert new inventory record
                        await conn.execute("""
                            INSERT INTO inventory (inventory_id, product_id, warehouse_id, stock, restock_date)
                            VALUES (nextval('inventory_inventory_id_seq'), 1, 1, 
                                   floor(random() * (25 - 5 + 1) + 5)::int, 
                                   NOW() + interval '30 days')
                        """)
                    else:
                        # Update existing inventory record
                        await conn.execute("""
                            UPDATE inventory 
                            SET stock = floor(random() * (25 - 5 + 1) + 5)::int,
                                restock_date = NOW() + interval '30 days'
                            WHERE product_id = 1
                        """)

                # Then insert a random product
                product = await conn.fetchrow("""
                    WITH selected_product AS (
                        SELECT product_id, product_name, category_id, base_price 
                        FROM products
                        WHERE product_id != 1
                        ORDER BY RANDOM()
                        LIMIT 1
                    )
                    INSERT INTO shopping_cart (product_id, product_name, category_id, price)
                    SELECT product_id, product_name, category_id, base_price 
                    FROM selected_product
                    RETURNING product_id;
                """)
                
                if product:
                    # Check if inventory exists for the random product
                    has_inventory = await conn.fetchval("""
                        SELECT EXISTS(
                            SELECT 1 FROM inventory WHERE product_id = $1
                        )
                    """, product['product_id'])
                    
                    if not has_inventory:
                        # Reset the sequence to the maximum value to avoid conflicts
                        await conn.execute("""
                            SELECT setval('inventory_inventory_id_seq', 
                                        COALESCE((SELECT MAX(inventory_id) FROM inventory), 0))
                        """)
                        # Insert new inventory record
                        await conn.execute("""
                            INSERT INTO inventory (inventory_id, product_id, warehouse_id, stock, restock_date)
                            VALUES (nextval('inventory_inventory_id_seq'), $1, 1, 
                                   floor(random() * (25 - 5 + 1) + 5)::int, 
                                   NOW() + interval '30 days')
                        """, product['product_id'])
                    else:
                        # Update existing inventory record
                        await conn.execute("""
                            UPDATE inventory 
                            SET stock = floor(random() * (25 - 5 + 1) + 5)::int,
                                restock_date = NOW() + interval '30 days'
                            WHERE product_id = $1
                        """, product['product_id'])
        except Exception as e:
            logger.error(f"Error adding item to shopping cart: {str(e)}", exc_info=True)
            raise

    async def delete_item():
        try:
            async with postgres_pool.acquire() as conn:
                await conn.execute("""
                    DELETE FROM shopping_cart
                    WHERE ts IN (
                        SELECT ts 
                        FROM shopping_cart 
                        WHERE product_id != 1
                        ORDER BY RANDOM() 
                        LIMIT 1
                    )
                """)
        except Exception as e:
            logger.error(f"Error removing item from shopping cart: {str(e)}", exc_info=True)
            raise

    # Initialize cart with 10 random items (product 1 will be included)
    for _ in range(10):
        await insert_item()

    while True:
        await insert_item()
        await delete_item()
        await asyncio.sleep(30.0)  # Sleep for 30 seconds


async def update_inventory_levels():
    """Randomly updates inventory levels for items in the shopping cart every second."""
    while True:
        try:
            async with postgres_pool.acquire() as conn:
                # Get current cart items and their inventory levels
                cart_items = await conn.fetch("""
                    SELECT DISTINCT sc.product_id, i.stock 
                    FROM shopping_cart sc
                    JOIN inventory i ON sc.product_id = i.product_id
                """)
                
                for item in cart_items:
                    # Randomly decide to add or subtract (1 for add, -1 for subtract)
                    direction = 1 if random.random() > 0.5 else -1
                    # Random amount between 1-5
                    amount = random.randint(1, 5) * direction
                    
                    # Update inventory ensuring stock doesn't go below 0
                    await conn.execute("""
                        UPDATE inventory 
                        SET stock = GREATEST(0, stock + $1)
                        WHERE product_id = $2
                    """, amount, item['product_id'])
                    
                    logger.debug(f"Updated inventory for product {item['product_id']}: {amount:+d} units")
        except Exception as e:
            logger.error(f"Error updating inventory levels: {str(e)}", exc_info=True)
        await asyncio.sleep(1.0)


async def execute_query(source_key: str, is_materialize: bool, query: str, product_id: int):
    """Execute a query for load testing."""
    try:
        if not traffic_enabled[source_key]:
            return
        source_display = source_names[source_key]
        task = asyncio.create_task(measure_query_time(query, [product_id], is_materialize, source_display))
        active_tasks[source_key].add(task)
        try:
            await task
        finally:
            active_tasks[source_key].remove(task)
        max_concurrent = await get_concurrency_limits()
        if traffic_enabled[source_key] and len(active_tasks[source_key]) < max_concurrent[source_key]:
            asyncio.create_task(execute_query(source_key, is_materialize, query, product_id))
    except Exception as e:
        logger.error(f"Error executing query for {source_key}: {str(e)}", exc_info=True)


async def continuous_query_load():
    """Generate continuous query load across different data sources."""
    product_id = 1
    QUERIES = {
        'view': """
            SELECT product_id, adjusted_price, last_update_time
            FROM dynamic_pricing 
            WHERE product_id = $1
        """,
        'materialized_view': """
            SELECT product_id, adjusted_price, last_update_time
            FROM mv_dynamic_pricing 
            WHERE product_id = $1
        """,
        'materialize': f"""
            SELECT product_id, adjusted_price, last_update_time
            FROM {mz_schema}.dynamic_pricing 
            WHERE product_id = $1
        """
    }
    while True:
        try:
            max_concurrent = await get_concurrency_limits()
            if len(active_tasks['view']) < max_concurrent['view']:
                asyncio.create_task(execute_query('view', False, QUERIES['view'], product_id))
            if len(active_tasks['materialized_view']) < max_concurrent['materialized_view']:
                asyncio.create_task(execute_query('materialized_view', False, QUERIES['materialized_view'], product_id))
            if len(active_tasks['materialize']) < max_concurrent['materialize']:
                asyncio.create_task(execute_query('materialize', True, QUERIES['materialize'], product_id))
            await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Error in continuous query load: {str(e)}", exc_info=True)
            await asyncio.sleep(1)


async def collect_container_stats():
    """Collect Docker container CPU and memory stats every 5 seconds."""
    logger.info("Starting container stats collection...")
    query_stats["postgres_stats"] = {
        "cpu_measurements": [],
        "memory_measurements": [],
        "timestamps": [],
        "current_stats": {"cpu_usage": 0.0, "memory_usage": 0.0, "last_updated": 0.0}
    }
    query_stats["materialize_stats"] = {
        "cpu_measurements": [],
        "memory_measurements": [],
        "timestamps": [],
        "current_stats": {"cpu_usage": 0.0, "memory_usage": 0.0, "last_updated": 0.0}
    }
    while True:
        try:
            current_time = time.time()
            containers = {"postgres_stats": "postgres", "materialize_stats": "materialize"}
            for stats_key, container_name in containers.items():
                logger.debug(f"Collecting Docker stats for {container_name}...")
                process = await asyncio.create_subprocess_exec(
                    'docker', 'stats', container_name, '--no-stream', '--format', '{{.CPUPerc}}\t{{.MemPerc}}',
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                )
                returncode = await process.wait()
                if returncode != 0:
                    logger.error(f"Error getting Docker stats for {container_name}: {process.stderr.read()}")
                else:
                    stats_str = (await process.stdout.read()).decode("utf-8").strip().split('\t')
                    if len(stats_str) == 2:
                        cpu_str = stats_str[0].rstrip('%')
                        mem_str = stats_str[1].rstrip('%')
                        logger.debug(f"Raw stats from {container_name}: CPU={cpu_str}%, MEM={mem_str}%")
                        try:
                            cpu_usage = float(cpu_str)
                            mem_usage = float(mem_str)
                            async with stats_lock:
                                stats = query_stats[stats_key]
                                stats["cpu_measurements"].append(cpu_usage)
                                stats["memory_measurements"].append(mem_usage)
                                stats["timestamps"].append(current_time)
                                if len(stats["timestamps"]) > 100:
                                    stats["cpu_measurements"].pop(0)
                                    stats["memory_measurements"].pop(0)
                                    stats["timestamps"].pop(0)
                                stats["current_stats"].update({
                                    "cpu_usage": cpu_usage,
                                    "memory_usage": mem_usage,
                                    "last_updated": current_time
                                })
                                logger.debug(f"Updated {stats_key} stats: CPU={cpu_usage}%, MEM={mem_usage}%")
                        except ValueError as e:
                            logger.error(f"Error converting stats for {container_name}: {str(e)}")
                    else:
                        logger.error(f"Invalid stats format for {container_name}")
        except Exception as e:
            logger.error(f"Error collecting container stats: {str(e)}")
        await asyncio.sleep(5)