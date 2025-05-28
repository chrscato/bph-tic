"""Module for logging and retry utilities."""

import logging
import sys
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

def setup_logging(level: str = "INFO"):
    """Configure structured logging.
    
    Args:
        level: Logging level (INFO, DEBUG, etc.)
    """
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.stdlib.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Configure root logger
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level.upper()
    )

def get_logger(name: str):
    """Get a structured logger instance.
    
    Args:
        name: Logger name
        
    Returns:
        Structured logger instance
    """
    return structlog.get_logger(name)

def with_retry(func):
    """Decorator to add retry logic to a function.
    
    Args:
        func: Function to wrap
        
    Returns:
        Wrapped function with retry logic
    """
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True
    )
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper
