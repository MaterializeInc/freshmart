"""Core database operations and queries."""
import time
import asyncio
import asyncpg
import logging
import datetime
import random
from typing import Dict, Tuple, Any
from .pools import postgres_pool, materialize_pool
from .config import source_to_stats, mz_schema
from .stats import query_stats, stats_lock, calculate_qps

logger = logging.getLogger(__name__)


async def measure_query_time(query: str, params: Tuple, is_materialize: bool, source: str) -> Tuple[float, Any]:
    """Measure the execution time of a database query."""
    start_time = time.time()
    try:
        if is_materialize:
            async with materialize_pool.acquire() as conn:
                result = await conn.fetchrow(query, *params, timeout=120.0)
        else:
            async with postgres_pool.acquire() as conn:
                result = await conn.fetchrow(query, *params, timeout=120.0)
        duration = time.time() - start_time

        stats_key = source_to_stats[source]
        if stats_key not in query_stats:
            logger.error(f"Invalid stats key: {stats_key}")
            return duration, result

        stats = query_stats[stats_key]
        current_time = time.time()
        async with stats_lock:
            stats["counts"].append(1)
            stats["timestamps"].append(current_time)
            stats["latencies"].append(duration)
            if result and "last_update_time" in result:
                current_ts = datetime.datetime.now(datetime.timezone.utc)
                last_update = result["last_update_time"]
                end_to_end_latency = (current_ts - last_update).total_seconds()
                stats["end_to_end_latencies"].append(end_to_end_latency)
                if len(stats["end_to_end_latencies"]) > 100:
                    stats["end_to_end_latencies"].pop(0)
                stats["current_stats"]["end_to_end_latency"] = end_to_end_latency * 1000
            cutoff_time = current_time - 1  # WINDOW_SIZE
            while stats["timestamps"] and stats["timestamps"][0] < cutoff_time:
                if stats["counts"]:
                    stats["counts"].pop(0)
                if stats["timestamps"]:
                    stats["timestamps"].pop(0)
                if stats["latencies"]:
                    stats["latencies"].pop(0)
            if len(stats["latencies"]) > 100:
                stats["latencies"].pop(0)
            stats["current_stats"]["qps"] = calculate_qps(stats_key)
            stats["current_stats"]["latency"] = duration * 1000
            if result and "adjusted_price" in result and result["adjusted_price"] is not None:
                try:
                    price = float(result["adjusted_price"])
                    stats["current_stats"]["price"] = price
                    logger.debug(f"Updated price for {source} to {price}")
                except (ValueError, TypeError) as e:
                    logger.error(f"Error converting price for {source}: {str(e)}")
            else:
                logger.debug(f"No valid price update for {source} - result: {result}")
            stats["current_stats"]["last_updated"] = current_time
        return duration, result
    except asyncio.exceptions.TimeoutError:
        duration = time.time() - start_time
        logger.error(f"{source} query timed out after {duration:.2f} seconds")
        return duration, None
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"{source} query error: {str(e)}", exc_info=True)
        return duration, None


async def get_database_size() -> float:
    """Get the current database size in GB."""
    async with postgres_pool.acquire() as conn:
        logger.debug("Querying database size...")
        size = await conn.fetchval("SELECT pg_database_size(current_database())")
        logger.debug(f"Raw database size (bytes): {size}")
        if size is None:
            logger.error("Database size query returned None")
            return 0.0
        size_gb = size / (1024 * 1024 * 1024)
        logger.debug(f"Database size: {size_gb:.2f} GB")
        return size_gb


async def toggle_promotion(product_id: int):
    """Toggle the promotion status for a product."""
    try:
        async with postgres_pool.acquire() as conn:
            # First check if the product exists
            product_exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM products WHERE product_id = $1)",
                product_id
            )

            if not product_exists:
                return {
                    "status": "error",
                    "message": f"Product with ID {product_id} does not exist"
                }

            # Try to update existing promotion
            result = await conn.fetchrow("""
                UPDATE promotions
                SET active = NOT active,
                    updated_at = NOW()
                WHERE product_id = $1
                RETURNING updated_at, active
            """, product_id)

            # If no existing promotion, create a new one
            if not result:
                # Reset the sequence to the maximum value to avoid conflicts
                await conn.execute("""
                    SELECT setval('promotions_promotion_id_seq', 
                                COALESCE((SELECT MAX(promotion_id) FROM promotions), 0))
                """)

                result = await conn.fetchrow("""
                    INSERT INTO promotions (
                        product_id,
                        promotion_discount,
                        start_date,
                        end_date,
                        active,
                        updated_at
                    )
                    VALUES (
                        $1,
                        10.0,  -- Default 10% discount
                        NOW(),
                        NOW() + interval '30 days',
                        TRUE,
                        NOW()
                    )
                    RETURNING updated_at, active
                """, product_id)

            if not result:
                return {
                    "status": "error",
                    "message": "Failed to create or update promotion"
                }

            return {
                "status": "success",
                "updated_at": result["updated_at"],
                "active": result["active"]
            }
    except Exception as e:
        logger.error(f"Error in toggle_promotion for product_id {product_id}: {str(e)}")
        return {
            "status": "error",
            "message": f"Database error: {str(e)}"
        }


async def get_categories():
    """Get all product categories from the database."""
    try:
        async with postgres_pool.acquire() as conn:
            categories = await conn.fetch("""
                SELECT DISTINCT category_id, category_name 
                FROM categories 
                ORDER BY category_name
            """)
            return [dict(row) for row in categories]
    except Exception as e:
        logger.error(f"Error fetching categories: {str(e)}", exc_info=True)
        raise


async def add_product(product_name: str, category_id: int, price: float):
    """Add a new product to the database and to the shopping cart."""
    try:
        async with postgres_pool.acquire() as conn:
            async with conn.transaction():
                # First get the maximum product_id and add 1
                next_id = await conn.fetchval("""
                    SELECT COALESCE(MAX(product_id), 0) + 1 FROM products
                """)

                # Insert into products table
                result = await conn.fetchrow("""
                    INSERT INTO products (product_id, product_name, category_id, base_price, supplier_id, available, last_update_time)
                    VALUES ($1, $2, $3, $4, 1, true, NOW())
                    RETURNING product_id, product_name, category_id, base_price
                """, next_id, product_name, category_id, price)

                # Reset sequences to match current maximum values
                await conn.execute("""
                    SELECT setval('sales_sale_id_seq', COALESCE((SELECT MAX(sale_id) FROM sales), 0));
                    SELECT setval('promotions_promotion_id_seq', COALESCE((SELECT MAX(promotion_id) FROM promotions), 0));
                """)

                # Add initial sale record for the product
                await conn.execute("""
                    INSERT INTO sales (product_id, sale_date, price, sale_price)
                    VALUES ($1, NOW(), $2, $2)
                """, next_id, price)

                # Initialize inventory with random stock between 5 and 25
                await conn.execute("""
                    INSERT INTO inventory (product_id, warehouse_id, stock, restock_date)
                    VALUES ($1, 1, floor(random() * (25 - 5 + 1) + 5)::int, NOW() + interval '30 days')
                """, next_id)

                # Add the new product to the shopping cart
                await conn.execute("""
                    INSERT INTO shopping_cart (product_id, product_name, category_id, price)
                    VALUES ($1, $2, $3, $4)
                """, next_id, product_name, category_id, price)

                return dict(result)
    except Exception as e:
        logger.error(f"Error adding product: {str(e)}", exc_info=True)
        raise


async def get_category_subtotals():
    """Get subtotals for each category in the shopping cart."""
    try:
        async with materialize_pool.acquire() as conn:
            subtotals = await conn.fetch("""
                SELECT 
                    category_name,
                    item_count,
                    total as subtotal
                FROM category_totals
                ORDER BY category_name ASC
            """)
            return [dict(row) for row in subtotals]
    except Exception as e:
        logger.error(f"Error fetching category subtotals: {str(e)}", exc_info=True)
        raise