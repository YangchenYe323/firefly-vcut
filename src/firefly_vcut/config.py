"""
Configuration module for firefly-vcut application.
"""

import os
from .retry import RetryConfig

# Retry configuration for HTTP requests
# These can be overridden by environment variables
DEFAULT_HTTP_RETRY_CONFIG = RetryConfig(
    max_retries=int(os.getenv("HTTP_MAX_RETRIES", "3")),
    initial_backoff=float(os.getenv("HTTP_INITIAL_BACKOFF", "5.0")),
    exponent=float(os.getenv("HTTP_EXPONENT", "2.0")),
    max_backoff=float(os.getenv("HTTP_MAX_BACKOFF", "60.0")) if os.getenv("HTTP_MAX_BACKOFF") else None,
)

# Bilibili API specific retry configuration
BILIBILI_RETRY_CONFIG = RetryConfig(
    max_retries=int(os.getenv("BILIBILI_MAX_RETRIES", "3")),
    initial_backoff=float(os.getenv("BILIBILI_INITIAL_BACKOFF", "5.0")),
    exponent=float(os.getenv("BILIBILI_EXPONENT", "2.0")),
    max_backoff=float(os.getenv("BILIBILI_MAX_BACKOFF", "60.0")) if os.getenv("BILIBILI_MAX_BACKOFF") else None,
)

# Live streaming specific retry configuration
STREAMING_RETRY_CONFIG = RetryConfig(
    max_retries=int(os.getenv("STREAMING_MAX_RETRIES", "3")),
    initial_backoff=float(os.getenv("STREAMING_INITIAL_BACKOFF", "5.0")),
    exponent=float(os.getenv("STREAMING_EXPONENT", "2.0")),
    max_backoff=float(os.getenv("STREAMING_MAX_BACKOFF", "60.0")) if os.getenv("STREAMING_MAX_BACKOFF") else None,
) 