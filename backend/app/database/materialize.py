"""Materialize-specific database operations."""
import asyncio
import asyncpg
import logging
from .pools import materialize_pool
from .config import mz_schema, current_isolation_level

logger = logging.getLogger(__name__)


async def toggle_view_index():
    """Toggle the index on the dynamic pricing view."""
    try:
        async with materialize_pool.acquire() as conn:
            if await check_materialize_index_exists():
                await conn.execute(f"DROP INDEX {mz_schema}.dynamic_pricing_product_id_idx")
                return {"message": "Index dropped successfully", "index_exists": False}
            else:
                await conn.execute(
                    f"CREATE INDEX dynamic_pricing_product_id_idx ON {mz_schema}.dynamic_pricing (product_id)")
                return {"message": "Index created successfully", "index_exists": True}
    except Exception as e:
        logger.error(f"Error toggling index: {str(e)}")
        raise Exception(f"Failed to toggle index: {str(e)}")


async def get_view_index_status():
    """Check if the dynamic pricing index exists."""
    async with materialize_pool.acquire() as conn:
        index_exists = await conn.fetchval("""
            SELECT TRUE 
            FROM mz_catalog.mz_indexes
            WHERE name = 'dynamic_pricing_product_id_idx'
        """)
        return index_exists or False


async def get_isolation_level():
    """Get the current transaction isolation level."""
    async with materialize_pool.acquire() as conn:
        level = await conn.fetchval("SHOW transaction_isolation")
        return level.lower()


async def toggle_isolation_level():
    """Toggle between serializable and strict serializable isolation levels."""
    global current_isolation_level
    async with materialize_pool.acquire() as conn:
        new_level = "strict serializable" if current_isolation_level == "serializable" else "serializable"
        await conn.execute(f"SET TRANSACTION_ISOLATION TO '{new_level}'")
        current_isolation_level = new_level
        return {"status": "success", "isolation_level": new_level}


async def check_materialize_index_exists():
    """Check if the Materialize index exists with retry logic."""
    max_retries = 3
    retry_delay = 1.0
    for attempt in range(max_retries):
        try:
            async with materialize_pool.acquire() as conn:
                result = await conn.fetchval("""
                    SELECT TRUE 
                    FROM mz_catalog.mz_indexes
                    WHERE name = 'dynamic_pricing_product_id_idx'
                """)
                return result or False
        except asyncpg.exceptions.ConnectionDoesNotExistError:
            logger.warning(f"Connection lost during index check (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                retry_delay *= 2
            continue
        except Exception as e:
            logger.error(f"Error checking Materialize index: {str(e)}")
            return False
    return False


async def get_concurrency_limits():
    """Get concurrency limits based on whether index exists."""
    has_index = await check_materialize_index_exists()
    return {
        'view': 1,
        'materialized_view': 5,
        'materialize': 5 if has_index else 1
    }