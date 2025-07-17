"""Core database operations and queries."""
import time
import asyncio
import asyncpg
import logging
import datetime
import random
from typing import Dict, Tuple, Any, List, Union
from .pools import postgres_pool, materialize_pool
from .config import source_to_stats, mz_schema
from .stats import query_stats, stats_lock, calculate_qps
from .error_handling import handle_database_errors, retry_on_failure, ErrorContext
from .exceptions import ProductNotFoundError, QueryTimeoutError

logger = logging.getLogger(__name__)


@handle_database_errors(operation="measure_query_time")
@retry_on_failure(max_retries=2, retry_delay=0.5)
async def measure_query_time(query: str, params: Tuple[Any, ...], is_materialize: bool, source: str) -> Tuple[float, Any]:
    """Measure the execution time of a database query."""
    start_time = time.time()
    
    async with ErrorContext(f"query_execution_{source}"):
        try:
            if is_materialize:
                async with materialize_pool.acquire() as conn:
                    result = await conn.fetchrow(query, *params, timeout=120.0)
            else:
                async with postgres_pool.acquire() as conn:
                    result = await conn.fetchrow(query, *params, timeout=120.0)
            
            duration = time.time() - start_time
            
            # Update statistics
            await _update_query_stats(source, duration, result)
            
            return duration, result
            
        except asyncio.TimeoutError:
            duration = time.time() - start_time
            raise QueryTimeoutError(
                f"Query timed out after {duration:.2f} seconds",
                query=query,
                timeout=duration,
                operation=f"query_{source}"
            )


async def _update_query_stats(source: str, duration: float, result: Any) -> None:
    """Update query statistics for a completed query."""
    stats_key = source_to_stats.get(source)
    if not stats_key or stats_key not in query_stats:
        logger.warning(f"Invalid stats key for source: {source}")
        return

    stats = query_stats[stats_key]
    current_time = time.time()
    
    async with stats_lock:
        stats["counts"].append(1)
        stats["timestamps"].append(current_time)
        stats["latencies"].append(duration)
        
        # Handle end-to-end latency
        if result and "last_update_time" in result:
            current_ts = datetime.datetime.now(datetime.timezone.utc)
            last_update = result["last_update_time"]
            end_to_end_latency = (current_ts - last_update).total_seconds()
            stats["end_to_end_latencies"].append(end_to_end_latency)
            if len(stats["end_to_end_latencies"]) > 100:
                stats["end_to_end_latencies"].pop(0)
            stats["current_stats"]["end_to_end_latency"] = end_to_end_latency * 1000
        
        # Clean old data
        cutoff_time = current_time - 1  # WINDOW_SIZE
        while stats["timestamps"] and stats["timestamps"][0] < cutoff_time:
            for key in ["counts", "timestamps", "latencies"]:
                if stats[key]:
                    stats[key].pop(0)
        
        # Limit history size
        if len(stats["latencies"]) > 100:
            stats["latencies"].pop(0)
        
        # Update current stats
        stats["current_stats"]["qps"] = calculate_qps(stats_key)
        stats["current_stats"]["latency"] = duration * 1000
        stats["current_stats"]["last_updated"] = current_time
        
        # Update price if available
        if result and "adjusted_price" in result and result["adjusted_price"] is not None:
            try:
                price = float(result["adjusted_price"])
                stats["current_stats"]["price"] = price
                logger.debug(f"Updated price for {source} to {price}")
            except (ValueError, TypeError) as e:
                logger.warning(f"Error converting price for {source}: {str(e)}")
        else:
            logger.debug(f"No valid price update for {source}")


@handle_database_errors(operation="get_database_size")
async def get_database_size() -> float:
    """Get the current database size in GB."""
    async with ErrorContext("get_database_size"):
        async with postgres_pool.acquire() as conn:
            size = await conn.fetchval("SELECT pg_database_size(current_database())")
            if size is None:
                logger.warning("Database size query returned None")
                return 0.0
            size_gb = size / (1024 * 1024 * 1024)
            logger.debug(f"Database size: {size_gb:.2f} GB")
            return size_gb


@handle_database_errors(operation="toggle_promotion", reraise=False, default_return={"status": "error", "message": "Database error occurred"})
async def toggle_promotion(product_id: int) -> Dict[str, Any]:
    """Toggle the promotion status for a product."""
    async with ErrorContext(f"toggle_promotion_product_{product_id}"):
        async with postgres_pool.acquire() as conn:
            # Check if product exists
            product_exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM products WHERE product_id = $1)",
                product_id
            )

            if not product_exists:
                raise ProductNotFoundError(product_id, operation="toggle_promotion")

            # Try to update existing promotion
            result = await conn.fetchrow("""
                UPDATE promotions
                SET active = NOT active, updated_at = NOW()
                WHERE product_id = $1
                RETURNING updated_at, active
            """, product_id)

            # Create new promotion if none exists
            if not result:
                result = await _create_new_promotion(conn, product_id)

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


async def _create_new_promotion(conn, product_id: int):
    """Create a new promotion for a product."""
    # Reset sequence
    await conn.execute("""
        SELECT setval('promotions_promotion_id_seq', 
                    COALESCE((SELECT MAX(promotion_id) FROM promotions), 0))
    """)

    return await conn.fetchrow("""
        INSERT INTO promotions (
            product_id, promotion_discount, start_date, end_date, active, updated_at
        ) VALUES (
            $1, 10.0, NOW(), NOW() + interval '30 days', TRUE, NOW()
        ) RETURNING updated_at, active
    """, product_id)


async def get_categories() -> List[Dict[str, Any]]:
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


async def add_product(product_name: str, category_id: int, price: float) -> Dict[str, Any]:
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


async def get_category_subtotals() -> List[Dict[str, Any]]:
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