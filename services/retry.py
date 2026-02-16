"""Retry utilities with exponential backoff."""
from __future__ import annotations

import functools
import logging
import random
import time
from collections.abc import Callable
from typing import Any, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Default exceptions to retry on
DEFAULT_RETRY_EXCEPTIONS: tuple[type[Exception], ...] = (
    ConnectionError,
    TimeoutError,
    OSError,
)


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retry_on: tuple[type[Exception], ...] | None = None,
    on_retry: Callable[[Exception, int, float], None] | None = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator that retries a function with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts (0 = no retries)
        base_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        exponential_base: Base for exponential backoff calculation
        jitter: Whether to add random jitter to prevent thundering herd
        retry_on: Tuple of exception types to retry on (default: connection errors)
        on_retry: Optional callback called on each retry with (exception, attempt, delay)

    Returns:
        Decorated function that will retry on specified exceptions
    """
    exceptions_to_catch = retry_on or DEFAULT_RETRY_EXCEPTIONS

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception: Exception | None = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions_to_catch as e:
                    last_exception = e

                    if attempt >= max_retries:
                        logger.error(
                            f"[retry] {func.__name__} failed after {max_retries + 1} attempts: {e}"
                        )
                        raise

                    # Calculate delay with exponential backoff
                    delay = min(base_delay * (exponential_base**attempt), max_delay)

                    # Add jitter (±25% of delay)
                    if jitter:
                        delay = delay * (0.75 + random.random() * 0.5)

                    logger.warning(
                        f"[retry] {func.__name__} attempt {attempt + 1}/{max_retries + 1} "
                        f"failed: {e}. Retrying in {delay:.2f}s"
                    )

                    if on_retry:
                        on_retry(e, attempt + 1, delay)

                    time.sleep(delay)

            # Should not reach here, but satisfy type checker
            if last_exception:
                raise last_exception
            raise RuntimeError("Unexpected retry loop exit")

        return wrapper

    return decorator


def retry_with_backoff_async(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retry_on: tuple[type[Exception], ...] | None = None,
    on_retry: Callable[[Exception, int, float], None] | None = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Async version of retry_with_backoff decorator.
    """
    import asyncio

    exceptions_to_catch = retry_on or DEFAULT_RETRY_EXCEPTIONS

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception: Exception | None = None

            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions_to_catch as e:
                    last_exception = e

                    if attempt >= max_retries:
                        logger.error(
                            f"[retry] {func.__name__} failed after {max_retries + 1} attempts: {e}"
                        )
                        raise

                    delay = min(base_delay * (exponential_base**attempt), max_delay)
                    if jitter:
                        delay = delay * (0.75 + random.random() * 0.5)

                    logger.warning(
                        f"[retry] {func.__name__} attempt {attempt + 1}/{max_retries + 1} "
                        f"failed: {e}. Retrying in {delay:.2f}s"
                    )

                    if on_retry:
                        on_retry(e, attempt + 1, delay)

                    await asyncio.sleep(delay)

            if last_exception:
                raise last_exception
            raise RuntimeError("Unexpected retry loop exit")

        return wrapper

    return decorator


# Pre-configured retry decorators for common use cases
llm_retry = retry_with_backoff(
    max_retries=3,
    base_delay=2.0,
    max_delay=30.0,
    retry_on=(ConnectionError, TimeoutError, OSError, Exception),  # Broad for LLM calls
)

db_retry = retry_with_backoff(
    max_retries=2,
    base_delay=0.5,
    max_delay=5.0,
    retry_on=(OSError,),  # SQLite lock errors
)

http_retry = retry_with_backoff(
    max_retries=3,
    base_delay=1.0,
    max_delay=15.0,
    retry_on=(ConnectionError, TimeoutError),
)
