"""Custom exceptions for database operations."""


class DatabaseError(Exception):
    """Base exception for database-related errors."""
    
    def __init__(self, message: str, operation: str = None, details: dict = None):
        super().__init__(message)
        self.message = message
        self.operation = operation
        self.details = details or {}
    
    def __str__(self):
        base_msg = self.message
        if self.operation:
            base_msg = f"{self.operation}: {base_msg}"
        return base_msg


class ConnectionError(DatabaseError):
    """Raised when database connection fails."""
    pass


class PoolExhaustionError(DatabaseError):
    """Raised when connection pool is exhausted."""
    pass


class QueryTimeoutError(DatabaseError):
    """Raised when a query times out."""
    
    def __init__(self, message: str, query: str = None, timeout: float = None, **kwargs):
        super().__init__(message, **kwargs)
        self.query = query
        self.timeout = timeout


class TransactionError(DatabaseError):
    """Raised when transaction operations fail."""
    pass


class ProductNotFoundError(DatabaseError):
    """Raised when a product is not found."""
    
    def __init__(self, product_id: int, **kwargs):
        message = f"Product with ID {product_id} does not exist"
        super().__init__(message, **kwargs)
        self.product_id = product_id


class PromotionError(DatabaseError):
    """Raised when promotion operations fail."""
    pass


class IndexError(DatabaseError):
    """Raised when index operations fail."""
    pass


class MaterializeError(DatabaseError):
    """Raised when Materialize-specific operations fail."""
    pass


class MetricsCollectionError(DatabaseError):
    """Raised when metrics collection fails."""
    pass


class ConfigurationError(DatabaseError):
    """Raised when configuration is invalid."""
    pass


class RetryableError(DatabaseError):
    """Base class for errors that can be retried."""
    
    def __init__(self, message: str, retry_count: int = 0, max_retries: int = 3, **kwargs):
        super().__init__(message, **kwargs)
        self.retry_count = retry_count
        self.max_retries = max_retries
    
    def can_retry(self) -> bool:
        """Check if the operation can be retried."""
        return self.retry_count < self.max_retries
    
    def increment_retry(self) -> 'RetryableError':
        """Create a new instance with incremented retry count."""
        self.retry_count += 1
        return self


class TemporaryConnectionError(RetryableError):
    """Temporary connection error that can be retried."""
    pass


class TemporaryQueryError(RetryableError):
    """Temporary query error that can be retried."""
    pass