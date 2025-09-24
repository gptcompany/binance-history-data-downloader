#!/usr/bin/env python3
"""
Enhanced Error Handling System for Binance History Data Downloader

This module provides comprehensive error categorization, smart retry logic,
and circuit breaker patterns for robust data downloading operations.
"""

import logging
import time
import asyncio
from enum import Enum
from typing import Dict, Optional, Callable, Any, List
from dataclasses import dataclass
from datetime import datetime, timedelta
import aiohttp


class ErrorCategory(Enum):
    """Comprehensive error categorization for download operations"""

    # Network-related errors (retryable with backoff)
    NETWORK_TIMEOUT = "network_timeout"
    CONNECTION_ERROR = "connection_error"
    DNS_RESOLUTION = "dns_resolution"
    SSL_ERROR = "ssl_error"

    # API-related errors
    API_RATE_LIMIT = "api_rate_limit"  # 429, needs adaptive backoff
    API_SERVER_ERROR = "api_server_error"  # 5xx, retryable
    API_CLIENT_ERROR = "api_client_error"  # 4xx (non-404), usually not retryable
    API_NOT_FOUND = "api_not_found"  # 404, expected for missing data
    API_UNAUTHORIZED = "api_unauthorized"  # 401, 403, not retryable

    # Data integrity errors
    DATA_CHECKSUM_MISMATCH = "data_checksum_mismatch"  # Re-download required
    DATA_CORRUPT_ZIP = "data_corrupt_zip"  # Re-download required
    DATA_INVALID_CSV = "data_invalid_csv"  # Re-download required
    DATA_SIZE_MISMATCH = "data_size_mismatch"  # Re-download required

    # File system errors
    FS_PERMISSION_DENIED = "fs_permission_denied"  # Not retryable
    FS_DISK_FULL = "fs_disk_full"  # Not retryable immediately
    FS_PATH_TOO_LONG = "fs_path_too_long"  # Not retryable
    FS_IO_ERROR = "fs_io_error"  # May be retryable

    # Resource errors
    MEMORY_ERROR = "memory_error"  # May need different strategy
    SEMAPHORE_TIMEOUT = "semaphore_timeout"  # Retryable with delay

    # Unknown/unhandled errors
    UNKNOWN_ERROR = "unknown_error"


@dataclass
class ErrorInfo:
    """Structured information about an error occurrence"""
    category: ErrorCategory
    message: str
    timestamp: datetime
    context: Dict[str, Any]
    retry_count: int = 0
    is_retryable: bool = True
    suggested_delay: float = 1.0


class CircuitBreakerState(Enum):
    """Circuit breaker states for handling persistent failures"""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class CircuitBreakerStats:
    """Statistics for circuit breaker decision making"""
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[datetime] = None
    state: CircuitBreakerState = CircuitBreakerState.CLOSED
    failure_threshold: int = 5
    recovery_timeout: int = 30  # seconds
    half_open_max_calls: int = 3


class ErrorClassifier:
    """Classifies exceptions into structured error categories"""

    @staticmethod
    def classify_exception(exception: Exception, context: Dict[str, Any] = None) -> ErrorInfo:
        """Classify an exception into structured error information"""
        context = context or {}
        timestamp = datetime.now()

        # Network and connection errors
        if isinstance(exception, (aiohttp.ClientConnectionError, ConnectionRefusedError)):
            return ErrorInfo(
                category=ErrorCategory.CONNECTION_ERROR,
                message=str(exception),
                timestamp=timestamp,
                context=context,
                is_retryable=True,
                suggested_delay=2.0
            )

        if isinstance(exception, (asyncio.TimeoutError, aiohttp.ServerTimeoutError)):
            return ErrorInfo(
                category=ErrorCategory.NETWORK_TIMEOUT,
                message=str(exception),
                timestamp=timestamp,
                context=context,
                is_retryable=True,
                suggested_delay=5.0
            )

        if isinstance(exception, aiohttp.ClientResponseError):
            status_code = exception.status

            if status_code == 429:
                # Rate limiting - use longer backoff
                return ErrorInfo(
                    category=ErrorCategory.API_RATE_LIMIT,
                    message=f"Rate limited: {exception}",
                    timestamp=timestamp,
                    context={**context, "status_code": status_code},
                    is_retryable=True,
                    suggested_delay=30.0
                )

            elif status_code == 404:
                # Not found - expected for missing historical data
                return ErrorInfo(
                    category=ErrorCategory.API_NOT_FOUND,
                    message=f"Resource not found: {exception}",
                    timestamp=timestamp,
                    context={**context, "status_code": status_code},
                    is_retryable=False,
                    suggested_delay=0.0
                )

            elif 500 <= status_code < 600:
                # Server errors - retryable
                return ErrorInfo(
                    category=ErrorCategory.API_SERVER_ERROR,
                    message=f"Server error: {exception}",
                    timestamp=timestamp,
                    context={**context, "status_code": status_code},
                    is_retryable=True,
                    suggested_delay=10.0
                )

            elif 400 <= status_code < 500:
                # Client errors (except 404, 429) - usually not retryable
                return ErrorInfo(
                    category=ErrorCategory.API_CLIENT_ERROR,
                    message=f"Client error: {exception}",
                    timestamp=timestamp,
                    context={**context, "status_code": status_code},
                    is_retryable=False,
                    suggested_delay=0.0
                )

        # File system errors
        if isinstance(exception, PermissionError):
            return ErrorInfo(
                category=ErrorCategory.FS_PERMISSION_DENIED,
                message=str(exception),
                timestamp=timestamp,
                context=context,
                is_retryable=False,
                suggested_delay=0.0
            )

        if isinstance(exception, OSError):
            error_msg = str(exception).lower()
            if "no space left" in error_msg or "disk full" in error_msg:
                return ErrorInfo(
                    category=ErrorCategory.FS_DISK_FULL,
                    message=str(exception),
                    timestamp=timestamp,
                    context=context,
                    is_retryable=False,
                    suggested_delay=0.0
                )
            else:
                return ErrorInfo(
                    category=ErrorCategory.FS_IO_ERROR,
                    message=str(exception),
                    timestamp=timestamp,
                    context=context,
                    is_retryable=True,
                    suggested_delay=1.0
                )

        # Memory errors
        if isinstance(exception, MemoryError):
            return ErrorInfo(
                category=ErrorCategory.MEMORY_ERROR,
                message=str(exception),
                timestamp=timestamp,
                context=context,
                is_retryable=True,
                suggested_delay=5.0
            )

        # Default: unknown error
        return ErrorInfo(
            category=ErrorCategory.UNKNOWN_ERROR,
            message=str(exception),
            timestamp=timestamp,
            context=context,
            is_retryable=True,
            suggested_delay=1.0
        )


class SmartRetryHandler:
    """Advanced retry handling with exponential backoff and jitter"""

    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 300.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.logger = logging.getLogger("downloader.retry")

    def calculate_delay(self, error_info: ErrorInfo, attempt: int) -> float:
        """Calculate smart delay based on error type and attempt number"""
        if not error_info.is_retryable:
            return 0.0

        # Use error-specific suggested delay as base
        base = error_info.suggested_delay

        # Exponential backoff with jitter
        exponential_delay = base * (2 ** attempt)

        # Add jitter (±25% random variation)
        import random
        jitter = random.uniform(-0.25, 0.25) * exponential_delay
        delay = exponential_delay + jitter

        # Cap at maximum delay
        return min(delay, self.max_delay)

    async def retry_with_backoff(
        self,
        operation: Callable,
        operation_args: tuple = (),
        operation_kwargs: dict = None,
        context: Dict[str, Any] = None
    ) -> Any:
        """Execute operation with smart retry logic"""
        operation_kwargs = operation_kwargs or {}
        context = context or {}

        last_error_info = None

        for attempt in range(self.max_retries + 1):  # +1 for initial attempt
            try:
                result = await operation(*operation_args, **operation_kwargs)

                if attempt > 0:
                    self.logger.info(f"Operation succeeded after {attempt} retries: {context.get('operation', 'unknown')}")

                return result

            except Exception as e:
                error_info = ErrorClassifier.classify_exception(e, context)
                error_info.retry_count = attempt
                last_error_info = error_info

                if not error_info.is_retryable:
                    self.logger.error(f"Non-retryable error in {context.get('operation', 'unknown')}: {error_info.message}")
                    raise e

                if attempt >= self.max_retries:
                    self.logger.error(f"Max retries ({self.max_retries}) exceeded for {context.get('operation', 'unknown')}: {error_info.message}")
                    raise e

                delay = self.calculate_delay(error_info, attempt)

                self.logger.warning(
                    f"Attempt {attempt + 1}/{self.max_retries + 1} failed for {context.get('operation', 'unknown')}: "
                    f"{error_info.category.value} - {error_info.message}. Retrying in {delay:.2f}s..."
                )

                if delay > 0:
                    await asyncio.sleep(delay)

        # Should not reach here due to raise in loop
        raise last_error_info


class CircuitBreaker:
    """Circuit breaker implementation for handling persistent endpoint failures"""

    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 30):
        self.stats = CircuitBreakerStats(
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout
        )
        self.logger = logging.getLogger("downloader.circuit_breaker")

    def should_allow_request(self) -> bool:
        """Determine if a request should be allowed based on circuit state"""
        now = datetime.now()

        if self.stats.state == CircuitBreakerState.CLOSED:
            return True

        elif self.stats.state == CircuitBreakerState.OPEN:
            # Check if recovery timeout has passed
            if (self.stats.last_failure_time and
                now - self.stats.last_failure_time > timedelta(seconds=self.stats.recovery_timeout)):
                self.logger.info("Circuit breaker transitioning to HALF_OPEN for testing")
                self.stats.state = CircuitBreakerState.HALF_OPEN
                self.stats.success_count = 0
                return True
            return False

        elif self.stats.state == CircuitBreakerState.HALF_OPEN:
            # Allow limited requests for testing recovery
            return self.stats.success_count < self.stats.half_open_max_calls

        return False

    def record_success(self):
        """Record a successful operation"""
        if self.stats.state == CircuitBreakerState.HALF_OPEN:
            self.stats.success_count += 1

            if self.stats.success_count >= self.stats.half_open_max_calls:
                self.logger.info("Circuit breaker recovery successful, transitioning to CLOSED")
                self.stats.state = CircuitBreakerState.CLOSED
                self.stats.failure_count = 0

        elif self.stats.state == CircuitBreakerState.CLOSED:
            # Reset failure count on success
            if self.stats.failure_count > 0:
                self.stats.failure_count = max(0, self.stats.failure_count - 1)

    def record_failure(self):
        """Record a failed operation"""
        self.stats.last_failure_time = datetime.now()

        if self.stats.state == CircuitBreakerState.HALF_OPEN:
            # Failed during recovery testing, back to OPEN
            self.logger.warning("Circuit breaker recovery failed, returning to OPEN state")
            self.stats.state = CircuitBreakerState.OPEN

        elif self.stats.state == CircuitBreakerState.CLOSED:
            self.stats.failure_count += 1

            if self.stats.failure_count >= self.stats.failure_threshold:
                self.logger.error(f"Circuit breaker OPEN after {self.stats.failure_count} failures")
                self.stats.state = CircuitBreakerState.OPEN


class EnhancedErrorHandler:
    """Main error handling facade combining all error management components"""

    def __init__(self, max_retries: int = 3, circuit_breaker_threshold: int = 5):
        self.retry_handler = SmartRetryHandler(max_retries=max_retries)
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.circuit_breaker_threshold = circuit_breaker_threshold
        self.logger = logging.getLogger("downloader.error_handler")

    def get_circuit_breaker(self, endpoint_key: str) -> CircuitBreaker:
        """Get or create circuit breaker for specific endpoint"""
        if endpoint_key not in self.circuit_breakers:
            self.circuit_breakers[endpoint_key] = CircuitBreaker(
                failure_threshold=self.circuit_breaker_threshold
            )
        return self.circuit_breakers[endpoint_key]

    async def execute_with_protection(
        self,
        operation: Callable,
        endpoint_key: str,
        operation_args: tuple = (),
        operation_kwargs: dict = None,
        context: Dict[str, Any] = None
    ) -> Any:
        """Execute operation with full error protection (retry + circuit breaker)"""
        circuit_breaker = self.get_circuit_breaker(endpoint_key)
        context = context or {}
        context['endpoint'] = endpoint_key

        # Check circuit breaker
        if not circuit_breaker.should_allow_request():
            self.logger.warning(f"Circuit breaker OPEN for {endpoint_key}, rejecting request")
            raise Exception(f"Circuit breaker is OPEN for endpoint {endpoint_key}")

        try:
            # Execute with retry logic
            result = await self.retry_handler.retry_with_backoff(
                operation=operation,
                operation_args=operation_args,
                operation_kwargs=operation_kwargs,
                context=context
            )

            # Record success
            circuit_breaker.record_success()
            return result

        except Exception as e:
            # Record failure and re-raise
            circuit_breaker.record_failure()
            raise e


# Convenience functions for common use cases
async def safe_download_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    destination_path,
    max_retries: int = 3,
    endpoint_key: str = "default"
) -> bool:
    """Wrapper for safe downloading with comprehensive error handling"""
    error_handler = EnhancedErrorHandler(max_retries=max_retries)

    async def download_operation():
        # Import the original download logic here
        # This would be refactored from the original download_file function
        async with session.get(url, timeout=60) as response:
            response.raise_for_status()

            with open(destination_path, 'wb') as f:
                async for chunk in response.content.iter_chunked(8192):
                    f.write(chunk)
            return True

    try:
        return await error_handler.execute_with_protection(
            operation=download_operation,
            endpoint_key=endpoint_key,
            context={"operation": "download", "url": url, "destination": str(destination_path)}
        )
    except Exception as e:
        logging.getLogger("downloader").error(f"Failed to download {url}: {e}")
        return False


# Error statistics tracking
class ErrorStatistics:
    """Track error statistics for monitoring and alerting"""

    def __init__(self):
        self.error_counts: Dict[ErrorCategory, int] = {}
        self.endpoint_stats: Dict[str, Dict[ErrorCategory, int]] = {}
        self.start_time = datetime.now()

    def record_error(self, error_info: ErrorInfo, endpoint: str = "unknown"):
        """Record an error occurrence for statistics"""
        # Global error count
        self.error_counts[error_info.category] = self.error_counts.get(error_info.category, 0) + 1

        # Per-endpoint error count
        if endpoint not in self.endpoint_stats:
            self.endpoint_stats[endpoint] = {}
        endpoint_errors = self.endpoint_stats[endpoint]
        endpoint_errors[error_info.category] = endpoint_errors.get(error_info.category, 0) + 1

    def get_error_summary(self) -> Dict[str, Any]:
        """Get comprehensive error summary for reporting"""
        runtime = datetime.now() - self.start_time

        return {
            "runtime_seconds": runtime.total_seconds(),
            "total_errors": sum(self.error_counts.values()),
            "error_categories": {cat.value: count for cat, count in self.error_counts.items()},
            "endpoint_breakdown": {
                endpoint: {cat.value: count for cat, count in errors.items()}
                for endpoint, errors in self.endpoint_stats.items()
            }
        }


# Global error statistics instance
error_statistics = ErrorStatistics()