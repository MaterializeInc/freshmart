"""Database operation manager classes for organized functionality."""
import time
import asyncio
import logging
from typing import Dict, Any, List, Optional
from .pools import postgres_pool, materialize_pool
from .config import config, state, traffic_enabled
from .models import PromotionResult, IndexStatus, IsolationLevelResult, TrafficState
from .collectors import QueryMetricsCollector, ContainerStatsCollector

logger = logging.getLogger(__name__)


class ProductManager:
    """Manages product-related database operations."""
    
    def __init__(self):
        self.pool = postgres_pool
    
    async def get_categories(self) -> List[Dict[str, Any]]:
        """Get all product categories from the database."""
        try:
            async with self.pool.acquire() as conn:
                categories = await conn.fetch("""
                    SELECT DISTINCT category_id, category_name 
                    FROM categories 
                    ORDER BY category_name
                """)
                return [dict(row) for row in categories]
        except Exception as e:
            logger.error(f"Error fetching categories: {str(e)}", exc_info=True)
            raise
    
    async def add_product(self, product_name: str, category_id: int, price: float) -> Dict[str, Any]:
        """Add a new product to the database and shopping cart."""
        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    # Get next product ID
                    next_id = await conn.fetchval("""
                        SELECT COALESCE(MAX(product_id), 0) + 1 FROM products
                    """)

                    # Insert product
                    result = await conn.fetchrow("""
                        INSERT INTO products (product_id, product_name, category_id, base_price, supplier_id, available, last_update_time)
                        VALUES ($1, $2, $3, $4, 1, true, NOW())
                        RETURNING product_id, product_name, category_id, base_price
                    """, next_id, product_name, category_id, price)

                    # Initialize related data
                    await self._initialize_product_data(conn, next_id, product_name, category_id, price)
                    
                    return dict(result)
        except Exception as e:
            logger.error(f"Error adding product: {str(e)}", exc_info=True)
            raise
    
    async def _initialize_product_data(self, conn, product_id: int, product_name: str, category_id: int, price: float) -> None:
        """Initialize sales, inventory, and cart data for a new product."""
        # Reset sequences
        await conn.execute("""
            SELECT setval('sales_sale_id_seq', COALESCE((SELECT MAX(sale_id) FROM sales), 0));
            SELECT setval('promotions_promotion_id_seq', COALESCE((SELECT MAX(promotion_id) FROM promotions), 0));
        """)

        # Add initial sale record
        await conn.execute("""
            INSERT INTO sales (product_id, sale_date, price, sale_price)
            VALUES ($1, NOW(), $2, $2)
        """, product_id, price)

        # Initialize inventory
        await conn.execute("""
            INSERT INTO inventory (product_id, warehouse_id, stock, restock_date)
            VALUES ($1, 1, floor(random() * (25 - 5 + 1) + 5)::int, NOW() + interval '30 days')
        """, product_id)

        # Add to shopping cart
        await conn.execute("""
            INSERT INTO shopping_cart (product_id, product_name, category_id, price)
            VALUES ($1, $2, $3, $4)
        """, product_id, product_name, category_id, price)


class PromotionManager:
    """Manages product promotion operations."""
    
    def __init__(self):
        self.pool = postgres_pool
    
    async def toggle_promotion(self, product_id: int) -> PromotionResult:
        """Toggle the promotion status for a product."""
        try:
            async with self.pool.acquire() as conn:
                # Check if product exists
                product_exists = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM products WHERE product_id = $1)",
                    product_id
                )

                if not product_exists:
                    return PromotionResult(
                        status="error",
                        message=f"Product with ID {product_id} does not exist"
                    )

                # Try to update existing promotion
                result = await conn.fetchrow("""
                    UPDATE promotions
                    SET active = NOT active, updated_at = NOW()
                    WHERE product_id = $1
                    RETURNING updated_at, active
                """, product_id)

                # Create new promotion if none exists
                if not result:
                    result = await self._create_new_promotion(conn, product_id)

                if not result:
                    return PromotionResult(
                        status="error",
                        message="Failed to create or update promotion"
                    )

                return PromotionResult(
                    status="success",
                    updated_at=result["updated_at"],
                    active=result["active"]
                )
        except Exception as e:
            logger.error(f"Error in toggle_promotion for product_id {product_id}: {str(e)}")
            return PromotionResult(
                status="error",
                message=f"Database error: {str(e)}"
            )
    
    async def _create_new_promotion(self, conn, product_id: int):
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


class MaterializeManager:
    """Manages Materialize-specific operations."""
    
    def __init__(self):
        self.pool = materialize_pool
    
    async def toggle_index(self, schema: str) -> IndexStatus:
        """Toggle the index on the dynamic pricing view."""
        try:
            async with self.pool.acquire() as conn:
                index_exists = await self._check_index_exists()
                
                if index_exists:
                    await conn.execute(f"DROP INDEX {schema}.dynamic_pricing_product_id_idx")
                    return IndexStatus(
                        message="Index dropped successfully",
                        index_exists=False
                    )
                else:
                    await conn.execute(
                        f"CREATE INDEX dynamic_pricing_product_id_idx ON {schema}.dynamic_pricing (product_id)"
                    )
                    return IndexStatus(
                        message="Index created successfully",
                        index_exists=True
                    )
        except Exception as e:
            logger.error(f"Error toggling index: {str(e)}")
            raise Exception(f"Failed to toggle index: {str(e)}")
    
    async def get_index_status(self) -> bool:
        """Check if the dynamic pricing index exists."""
        async with self.pool.acquire() as conn:
            index_exists = await conn.fetchval("""
                SELECT TRUE 
                FROM mz_catalog.mz_indexes
                WHERE name = 'dynamic_pricing_product_id_idx'
            """)
            return index_exists or False
    
    async def _check_index_exists(self) -> bool:
        """Check if index exists with retry logic."""
        max_retries = 3
        retry_delay = 1.0
        
        for attempt in range(max_retries):
            try:
                async with self.pool.acquire() as conn:
                    result = await conn.fetchval("""
                        SELECT TRUE 
                        FROM mz_catalog.mz_indexes
                        WHERE name = 'dynamic_pricing_product_id_idx'
                    """)
                    return result or False
            except Exception as e:
                logger.warning(f"Index check attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
                continue
        return False
    
    async def toggle_isolation_level(self) -> IsolationLevelResult:
        """Toggle between serializable and strict serializable isolation levels."""
        async with self.pool.acquire() as conn:
            current_level = state.current_isolation_level
            new_level = "strict serializable" if current_level == "serializable" else "serializable"
            
            await conn.execute(f"SET TRANSACTION_ISOLATION TO '{new_level}'")
            state.current_isolation_level = new_level
            
            return IsolationLevelResult(
                status="success",
                isolation_level=new_level
            )
    
    async def get_isolation_level(self) -> str:
        """Get the current transaction isolation level."""
        async with self.pool.acquire() as conn:
            level = await conn.fetchval("SHOW transaction_isolation")
            return level.lower()


class TrafficManager:
    """Manages query traffic control."""
    
    def __init__(self):
        self.traffic_state = TrafficState()
    
    async def toggle_traffic(self, source: str) -> bool:
        """Toggle traffic for a specific source."""
        if source not in traffic_enabled:
            raise ValueError(f"Unknown traffic source: {source}")
        
        traffic_enabled[source] = not traffic_enabled[source]
        logger.info(f"Traffic for {source} is now {'enabled' if traffic_enabled[source] else 'disabled'}")
        
        # Update internal state
        setattr(self.traffic_state, source, traffic_enabled[source])
        
        return traffic_enabled[source]
    
    async def get_traffic_state(self) -> TrafficState:
        """Get the current traffic state for all sources."""
        # Update state from global traffic_enabled
        self.traffic_state.view = traffic_enabled["view"]
        self.traffic_state.materialized_view = traffic_enabled["materialized_view"]
        self.traffic_state.materialize = traffic_enabled["materialize"]
        
        return self.traffic_state
    
    def is_traffic_enabled(self, source: str) -> bool:
        """Check if traffic is enabled for a specific source."""
        return traffic_enabled.get(source, False)


class DatabaseMetricsManager:
    """Centralized manager for all database metrics and monitoring."""
    
    def __init__(self):
        self.query_collector = QueryMetricsCollector()
        self.container_collector = ContainerStatsCollector()
        self.product_manager = ProductManager()
        self.promotion_manager = PromotionManager()
        self.materialize_manager = MaterializeManager()
        self.traffic_manager = TrafficManager()
    
    async def get_database_size(self) -> float:
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
    
    async def configure_refresh_interval(self, interval: int) -> Dict[str, Any]:
        """Configure the refresh interval for materialized view."""
        if interval < 1:
            raise ValueError("Interval must be at least 1 second")
        
        state.refresh_interval = interval
        return {"status": "success", "refresh_interval": interval}