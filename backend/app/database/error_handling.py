"""Error handling utilities and decorators for database operations."""
import asyncio
import functools
import logging
from typing import Callable, Any, Type, Union, Optional, Dict
import asyncpg
from .exceptions import (
    DatabaseError, ConnectionError, QueryTimeoutError, 
    TransactionError, RetryableError, TemporaryConnectionError,
    TemporaryQueryError, PoolExhaustionError
)

logger = logging.getLogger(__name__)


def map_asyncpg_exception(exc: Exception, operation: str = None) -> DatabaseError:
    """Map asyncpg exceptions to custom database exceptions."""
    if isinstance(exc, asyncpg.exceptions.ConnectionDoesNotExistError):
        return TemporaryConnectionError(
            "Database connection lost", 
            operation=operation,
            details={"original_error": str(exc)}
        )
    elif isinstance(exc, asyncpg.exceptions.ConnectionFailureError):
        return ConnectionError(
            "Failed to connect to database",
            operation=operation, 
            details={"original_error": str(exc)}
        )
    elif isinstance(exc, asyncpg.exceptions.TooManyConnectionsError):
        return PoolExhaustionError(
            "Too many database connections",
            operation=operation,
            details={"original_error": str(exc)}
        )
    elif isinstance(exc, asyncpg.exceptions.QueryCancelledError):
        return QueryTimeoutError(
            "Query was cancelled",
            operation=operation,
            details={"original_error": str(exc)}
        )
    elif isinstance(exc, asyncpg.exceptions.InFailedSQLTransactionError):
        return TransactionError(
            "Transaction is in failed state",
            operation=operation,
            details={"original_error": str(exc)}
        )
    elif isinstance(exc, asyncpg.exceptions.PostgreSQLError):
        return DatabaseError(
            f"PostgreSQL error: {exc.message}",
            operation=operation,
            details={
                "sqlstate": exc.sqlstate,
                "original_error": str(exc)
            }
        )
    else:
        return DatabaseError(
            f"Unexpected database error: {str(exc)}",
            operation=operation,
            details={"original_error": str(exc)}
        )


def handle_database_errors(
    operation: str = None,
    reraise: bool = True,
    default_return: Any = None
) -> Callable:
    """Decorator to handle database errors consistently."""
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except DatabaseError:
                # Already a custom database error, just log and reraise
                logger.error(f"Database error in {operation or func.__name__}", exc_info=True)
                if reraise:
                    raise
                return default_return
            except asyncio.TimeoutError as e:
                error = QueryTimeoutError(
                    "Database operation timed out",
                    operation=operation or func.__name__,
                    timeout=getattr(e, 'timeout', None)
                )
                logger.error(str(error), exc_info=True)
                if reraise:
                    raise error
                return default_return
            except Exception as e:
                # Map to custom exception
                error = map_asyncpg_exception(e, operation or func.__name__)
                logger.error(str(error), exc_info=True)
                if reraise:
                    raise error
                return default_return
        
        return wrapper
    return decorator


def retry_on_failure(
    max_retries: int = 3,
    retry_delay: float = 1.0,
    exponential_backoff: bool = True,
    retry_exceptions: tuple = (TemporaryConnectionError, TemporaryQueryError)
) -> Callable:
    """Decorator to retry operations on specific failures."""
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = retry_delay
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except retry_exceptions as e:
                    last_exception = e
                    if attempt == max_retries:
                        logger.error(
                            f"Max retries ({max_retries}) reached for {func.__name__}",
                            exc_info=True
                        )
                        break
                    
                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries + 1} failed for {func.__name__}: {str(e)}. "
                        f"Retrying in {current_delay}s..."
                    )
                    
                    await asyncio.sleep(current_delay)
                    
                    if exponential_backoff:
                        current_delay *= 2
                except Exception as e:
                    # Non-retryable exception, raise immediately
                    logger.error(f"Non-retryable error in {func.__name__}: {str(e)}", exc_info=True)
                    raise
            
            # If we get here, all retries failed
            raise last_exception
        
        return wrapper
    return decorator


class ErrorContext:
    """Context manager for enhanced error logging and handling."""
    
    def __init__(
        self, 
        operation: str,
        log_entry: bool = True,
        log_success: bool = False,
        extra_context: Dict[str, Any] = None
    ):
        self.operation = operation
        self.log_entry = log_entry
        self.log_success = log_success
        self.extra_context = extra_context or {}
        self.start_time = None
    
    async def __aenter__(self):
        if self.log_entry:
            logger.debug(f"Starting operation: {self.operation}")
            self.start_time = asyncio.get_event_loop().time()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            duration = asyncio.get_event_loop().time() - self.start_time
            
        if exc_type is None:
            if self.log_success:
                msg = f"Operation completed: {self.operation}"
                if self.start_time:
                    msg += f" (took {duration:.3f}s)"
                logger.info(msg)
        else:
            # Log the error with context
            error_msg = f"Operation failed: {self.operation}"
            if self.start_time:
                error_msg += f" (failed after {duration:.3f}s)"
            
            logger.error(
                error_msg,
                extra={
                    "operation": self.operation,
                    "error_type": exc_type.__name__,
                    "duration": duration if self.start_time else None,
                    **self.extra_context
                },
                exc_info=True
            )
        
        return False  # Don't suppress exceptions


class CircuitBreaker:
    """Simple circuit breaker for database operations."""
    
    def __init__(
        self, 
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: Type[Exception] = DatabaseError
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
    
    async def call(self, func: Callable, *args, **kwargs):
        """Execute function with circuit breaker protection."""
        if self.state == "OPEN":
            if self._should_attempt_reset():
                self.state = "HALF_OPEN"
                logger.info("Circuit breaker attempting reset")
            else:
                raise DatabaseError(
                    f"Circuit breaker is OPEN for {func.__name__}",
                    operation="circuit_breaker"
                )
        
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception as e:
            self._on_failure()
            raise
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset."""
        if self.last_failure_time is None:
            return True
        return (asyncio.get_event_loop().time() - self.last_failure_time) >= self.recovery_timeout
    
    def _on_success(self):
        """Handle successful operation."""
        self.failure_count = 0
        if self.state == "HALF_OPEN":
            self.state = "CLOSED"
            logger.info("Circuit breaker reset to CLOSED")
    
    def _on_failure(self):
        """Handle failed operation."""
        self.failure_count += 1
        self.last_failure_time = asyncio.get_event_loop().time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
            logger.warning(
                f"Circuit breaker opened after {self.failure_count} failures"
            )


# Global circuit breakers for different operations
connection_circuit_breaker = CircuitBreaker(
    failure_threshold=3,
    recovery_timeout=30.0,
    expected_exception=(ConnectionError, PoolExhaustionError)
)

query_circuit_breaker = CircuitBreaker(
    failure_threshold=5,
    recovery_timeout=60.0,
    expected_exception=QueryTimeoutError
)