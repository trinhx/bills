import time
import requests
import logging
import random
from typing import Callable, Any
from functools import wraps

logger = logging.getLogger(__name__)

class RateLimiter:
    """Proactive rate limiter to enforce max_requests per time_window."""
    def __init__(self, max_requests: int, time_window: float):
        self.max_requests = max_requests
        self.time_window = time_window
        self.timestamps: list[float] = []

    def wait(self):
        """Blocks until a request can be made without violating the rate limit."""
        now = time.time()
        # Remove timestamps older than the time window
        self.timestamps = [t for t in self.timestamps if now - t < self.time_window]
        
        if len(self.timestamps) >= self.max_requests:
            # Sleep until the oldest timestamp falls out of the window
            sleep_time = self.time_window - (now - self.timestamps[0])
            if sleep_time > 0:
                logger.debug(f"Proactive rate limiting: sleeping for {sleep_time:.2f}s")
                time.sleep(sleep_time)
            
            # Recalculate after sleep
            now = time.time()
            self.timestamps = [t for t in self.timestamps if now - t < self.time_window]
            
        self.timestamps.append(now)

class NetworkException(Exception):
    def __init__(self, message: str, status_code: int = 500, retry_after: int = 0):
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after

def with_retry(max_attempts: int = 5, base_delay: float = 1.0, max_delay: float = 60.0):
    """
    Decorator for reactive retry/backoff.
    Caches exceptions as NetworkException so we can extract retry_after cleanly
    or bubble them up for the orchestrator to log in cache_failures.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            attempts = 0
            while attempts < max_attempts:
                attempts += 1
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.RequestException as e:
                    status_code = getattr(e.response, 'status_code', 500) if e.response else 500
                    
                    # Determine retryability
                    is_retryable = (status_code == 429 or status_code >= 500)
                    if not is_retryable:
                        raise NetworkException(str(e), status_code=status_code) from e
                    
                    retry_after = 0
                    if e.response and 'Retry-After' in e.response.headers:
                        try:
                            retry_after = int(e.response.headers['Retry-After'])
                        except ValueError:
                            pass # sometimes it's an HTTP date, we just fallback
                    
                    # If it's a 429 and we still have no retry_after, enforce a default 60s cooldown
                    if status_code == 429 and retry_after == 0:
                        retry_after = 60
                            
                    if attempts == max_attempts:
                        raise NetworkException(str(e), status_code=status_code, retry_after=retry_after) from e
                        
                    # Calculate delay
                    if retry_after > 0:
                        delay = retry_after
                    else:
                        delay = min(max_delay, base_delay * (2 ** (attempts - 1)))
                        delay += random.uniform(0, 0.1 * delay) # jitter
                        
                    logger.warning(f"Request failed ({status_code}). Retrying in {delay:.2f}s (Attempt {attempts}/{max_attempts})")
                    time.sleep(delay)
        return wrapper
    return decorator
