"""Database connection pool management."""
import asyncio
import asyncpg
import logging
from typing import Optional
from .config import config, state

logger = logging.getLogger(__name__)

# Global pools (initialized once)
postgres_pool: Optional[asyncpg.Pool] = None
materialize_pool: Optional[asyncpg.Pool] = None


class MaterializeConnection(asyncpg.Connection):
    """Custom connection class for Materialize that skips PostgreSQL-specific cleanup."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._closed = False
        self._cleanup_lock = asyncio.Lock()

    def _cleanup(self):
        # Skip cleanup if already closed
        if self._closed:
            return

        try:
            loop = asyncio.get_event_loop()

            async def safe_cleanup():
                if self._closed:
                    return

                async with self._cleanup_lock:
                    if not self._closed:
                        try:
                            # Only try to execute ROLLBACK if we have a valid protocol
                            if hasattr(self, '_protocol') and self._protocol is not None:
                                await self.execute("ROLLBACK")
                        except Exception as e:
                            logger.info(f"Ignoring error during cleanup: {str(e)}")
                        finally:
                            self._closed = True

            if loop.is_running():
                logger.info("Cleanup: event loop is running, scheduling cleanup")
                loop.create_task(safe_cleanup())
            else:
                logger.info("Cleanup: event loop not running, running cleanup synchronously")
                loop.run_until_complete(safe_cleanup())
        except Exception as e:
            logger.error(f"Error during Materialize connection cleanup: {str(e)}")
            self._closed = True

    async def close(self, *, timeout: Optional[float] = None) -> None:
        """Override close to handle cleanup properly"""
        if self._closed:
            return

        try:
            async with self._cleanup_lock:
                if not self._closed:
                    try:
                        # Only try to execute ROLLBACK if we have a valid protocol
                        if hasattr(self, '_protocol') and self._protocol is not None:
                            await self.execute("ROLLBACK")
                    except Exception as e:
                        logger.debug(f"Ignoring error during close: {str(e)}")
                    finally:
                        self._closed = True
                        # Call parent close without timeout to avoid additional cleanup
                        await super().close(timeout=None)
        except Exception as e:
            logger.error(f"Error during Materialize connection close: {str(e)}")
            self._closed = True

    async def add_listener(self, channel, callback):
        # Skip listener commands as they're not supported in Materialize
        pass

    async def remove_listener(self, channel, callback):
        # Skip listener commands as they're not supported in Materialize
        pass

    async def reset(self, *, timeout: Optional[float] = None) -> None:
        # Skip reset for Materialize connections
        pass


async def new_postgres_pool() -> asyncpg.Pool:
    """Create a new PostgreSQL connection pool."""
    return await asyncpg.create_pool(
        user=config.db_user,
        password=config.db_password,
        database=config.db_name,
        host=config.db_host,
        port=config.db_port,
        command_timeout=config.command_timeout,
        min_size=config.pool_min_size,
        max_size=config.pool_max_size,
        server_settings={
            'application_name': 'freshmart_pg',
            'statement_timeout': f'{int(config.command_timeout)}s',
            'idle_in_transaction_session_timeout': f'{int(config.command_timeout)}s'
        }
    )


async def new_materialize_pool() -> asyncpg.Pool:
    """Create a new Materialize connection pool."""
    logger.info("Initializing Materialize pool...")
    return await asyncpg.create_pool(
        user=config.mz_user,
        password=config.mz_password,
        database=config.mz_name,
        host=config.mz_host,
        port=config.mz_port,
        command_timeout=config.command_timeout,
        connection_class=MaterializeConnection,
        min_size=config.pool_min_size,
        max_size=config.pool_max_size,
        server_settings={
            'application_name': 'freshmart_mz',
            'statement_timeout': f'{int(config.command_timeout)}s',
            'idle_in_transaction_session_timeout': f'{int(config.command_timeout)}s',
            'statement_logging_sample_rate': '0'
        }
    )


async def refresh_materialize_pool() -> None:
    """Safely refresh the Materialize connection pool with retries."""
    global materialize_pool
    max_retries = 3
    retry_delay = 1.0  # Start with 1 second delay
    
    for attempt in range(max_retries):
        try:
            logger.info("Starting Materialize pool refresh...")
            
            # Create new pool first
            new_pool = await new_materialize_pool()
            
            # Test the new pool with a simple query before switching
            try:
                async with new_pool.acquire() as conn:
                    await conn.fetchval("SELECT 1")
            except Exception as e:
                logger.error(f"New pool validation failed: {str(e)}")
                await new_pool.close()
                raise
            
            if materialize_pool:
                old_pool = materialize_pool
                # Update the global reference first
                materialize_pool = new_pool
                
                try:
                    # Wait a short time for in-flight queries to complete
                    await asyncio.sleep(0.5)
                    
                    # Try to gracefully close the old pool with a timeout
                    try:
                        await asyncio.wait_for(old_pool.close(), timeout=5.0)
                    except asyncio.TimeoutError:
                        logger.warning("Timeout while closing old pool - continuing anyway")
                    except Exception as e:
                        logger.warning(f"Non-critical error closing old pool: {str(e)}")
                except Exception as e:
                    logger.warning(f"Error during old pool cleanup: {str(e)}")
            else:
                # First time initialization
                materialize_pool = new_pool
            
            logger.info("Successfully refreshed Materialize pool")
            return
            
        except Exception as e:
            logger.error(f"Error refreshing Materialize pool (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error("Max retries reached for Materialize pool refresh")
                # Don't raise the error - we want to keep trying in the next interval


async def periodic_pool_refresh() -> None:
    """Periodically refresh the Materialize connection pool."""
    while True:
        try:
            await asyncio.sleep(config.pool_refresh_interval)
            await refresh_materialize_pool()
        except Exception as e:
            logger.error(f"Error in periodic pool refresh: {str(e)}")
            # On error, wait a bit longer before next attempt
            await asyncio.sleep(5)  # Brief pause on error before continuing


async def init_pools() -> None:
    """Initialize the global connection pools for PostgreSQL and Materialize."""
    global postgres_pool, materialize_pool
    postgres_pool = await new_postgres_pool()
    materialize_pool = await new_materialize_pool()
    
    # Start the periodic pool refresh task
    asyncio.create_task(periodic_pool_refresh())