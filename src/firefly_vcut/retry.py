import asyncio
import time
from typing import Callable, TypeVar, Any, Optional
import requests
import aiohttp

T = TypeVar('T')

class RetryConfig:
    """Configuration for retry behavior with exponential backoff."""
    
    def __init__(
        self,
        max_retries: int = 3,
        initial_backoff: float = 5.0,
        exponent: float = 2.0,
        max_backoff: Optional[float] = None,
        retry_on_status_codes: Optional[list[int]] = None
    ):
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.exponent = exponent
        self.max_backoff = max_backoff
        self.retry_on_status_codes = retry_on_status_codes or [500, 502, 503, 504, 520, 521, 522, 523, 524]

def should_retry_response(response: requests.Response, config: RetryConfig) -> bool:
    """Check if a response should trigger a retry based on status code."""
    return response.status_code in config.retry_on_status_codes

def should_retry_aiohttp_response(response: aiohttp.ClientResponse, config: RetryConfig) -> bool:
    """Check if an aiohttp response should trigger a retry based on status code."""
    return response.status in config.retry_on_status_codes

def retry_with_backoff(
    func: Callable[[], T],
    config: RetryConfig,
    *args,
    **kwargs
) -> T:
    """
    Retry a synchronous function with exponential backoff.
    
    Args:
        func: The function to retry
        config: Retry configuration
        *args: Arguments to pass to the function
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        The result of the function call
        
    Raises:
        Exception: The last exception that occurred after all retries
    """
    last_exception = None
    backoff = config.initial_backoff
    
    for attempt in range(config.max_retries + 1):
        try:
            result = func(*args, **kwargs)
            
            # If it's a requests.Response, check if we should retry based on status code
            if isinstance(result, requests.Response) and should_retry_response(result, config):
                if attempt < config.max_retries:
                    print(f"HTTP {result.status_code} received, retrying in {backoff:.1f}s (attempt {attempt + 1}/{config.max_retries})")
                    time.sleep(backoff)
                    backoff = min(backoff * config.exponent, config.max_backoff or float('inf'))
                    continue
                else:
                    print(f"Max retries ({config.max_retries}) reached for HTTP {result.status_code}")
                    return result
            
            return result
            
        except Exception as e:
            last_exception = e
            if attempt < config.max_retries:
                print(f"Exception occurred: {e}, retrying in {backoff:.1f}s (attempt {attempt + 1}/{config.max_retries})")
                time.sleep(backoff)
                backoff = min(backoff * config.exponent, config.max_backoff or float('inf'))
            else:
                print(f"Max retries ({config.max_retries}) reached, last exception: {e}")
                raise last_exception
    
    raise last_exception

async def retry_with_backoff_async(
    func: Callable[[], Any],
    config: RetryConfig,
    *args,
    **kwargs
) -> T:
    """
    Retry an asynchronous function with exponential backoff.
    
    Args:
        func: The async function to retry
        config: Retry configuration
        *args: Arguments to pass to the function
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        The result of the function call
        
    Raises:
        Exception: The last exception that occurred after all retries
    """
    last_exception = None
    backoff = config.initial_backoff
    
    for attempt in range(config.max_retries + 1):
        try:
            result = await func(*args, **kwargs)
            
            # If it's an aiohttp.ClientResponse, check if we should retry based on status code
            if isinstance(result, aiohttp.ClientResponse) and should_retry_aiohttp_response(result, config):
                if attempt < config.max_retries:
                    print(f"HTTP {result.status} received, retrying in {backoff:.1f}s (attempt {attempt + 1}/{config.max_retries})")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * config.exponent, config.max_backoff or float('inf'))
                    continue
                else:
                    print(f"Max retries ({config.max_retries}) reached for HTTP {result.status}")
                    return result
            
            return result
            
        except Exception as e:
            last_exception = e
            if attempt < config.max_retries:
                print(f"Exception occurred: {e}, retrying in {backoff:.1f}s (attempt {attempt + 1}/{config.max_retries})")
                await asyncio.sleep(backoff)
                backoff = min(backoff * config.exponent, config.max_backoff or float('inf'))
            else:
                print(f"Max retries ({config.max_retries}) reached, last exception: {e}")
                raise last_exception
    
    raise last_exception