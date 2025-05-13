# Structured logging module for ActivityStore
# Provides a configurable logging system with metadata support

import functools
import logging
from typing import Any, Callable, Dict, Optional

# Setup default logger
DEFAULT_LOGGER = logging.getLogger("activity_store")


class StructuredLogger:
    """
    Structured logger that supports metadata and different logging backends.
    
    This logger wraps Python's standard logging but adds support for
    metadata in a structured format and can be configured to use
    different logging backends.
    """
    
    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        default_metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize a structured logger.
        
        Args:
            logger: Logger instance to use (defaults to activity_store logger)
            default_metadata: Default metadata to include with all log messages
        """
        self.logger = logger or DEFAULT_LOGGER
        self.default_metadata = default_metadata or {}
    
    def _log(
        self,
        level: int,
        msg: str,
        metadata: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs
    ) -> None:
        """
        Log a message with metadata.
        
        Args:
            level: Logging level
            msg: Message to log
            metadata: Additional metadata to include
            *args: Additional positional arguments
            **kwargs: Additional keyword arguments
        """
        # Combine default and message-specific metadata
        combined_metadata = {**self.default_metadata}
        if metadata:
            combined_metadata.update(metadata)
        
        # Include metadata in the log record
        extra = kwargs.get("extra", {})
        extra["metadata"] = combined_metadata
        kwargs["extra"] = extra
        
        self.logger.log(level, msg, *args, **kwargs)
    
    def debug(
        self,
        msg: str,
        metadata: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs
    ) -> None:
        """Log a debug message with metadata."""
        self._log(logging.DEBUG, msg, metadata, *args, **kwargs)
    
    def info(
        self,
        msg: str,
        metadata: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs
    ) -> None:
        """Log an info message with metadata."""
        self._log(logging.INFO, msg, metadata, *args, **kwargs)
    
    def warning(
        self,
        msg: str,
        metadata: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs
    ) -> None:
        """Log a warning message with metadata."""
        self._log(logging.WARNING, msg, metadata, *args, **kwargs)
    
    def error(
        self,
        msg: str,
        metadata: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs
    ) -> None:
        """Log an error message with metadata."""
        self._log(logging.ERROR, msg, metadata, *args, **kwargs)
    
    def critical(
        self,
        msg: str,
        metadata: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs
    ) -> None:
        """Log a critical message with metadata."""
        self._log(logging.CRITICAL, msg, metadata, *args, **kwargs)


def get_logger(
    name: Optional[str] = None,
    default_metadata: Optional[Dict[str, Any]] = None
) -> StructuredLogger:
    """
    Get a structured logger.
    
    Args:
        name: Logger name (appended to 'activity_store' namespace)
        default_metadata: Default metadata to include with all log messages
        
    Returns:
        StructuredLogger instance
    """
    if name:
        logger_name = f"activity_store.{name}"
    else:
        logger_name = "activity_store"
    
    return StructuredLogger(
        logger=logging.getLogger(logger_name),
        default_metadata=default_metadata
    )


def with_logging(
    func: Optional[Callable] = None,
    *,
    logger: Optional[StructuredLogger] = None,
    level: int = logging.DEBUG
) -> Callable:
    """
    Decorator that adds logging to a function.
    
    Args:
        func: Function to decorate
        logger: Logger to use
        level: Logging level
        
    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal logger
            if logger is None:
                logger = get_logger(func.__module__)
            
            # Log function call
            logger._log(
                level,
                f"Calling {func.__name__}",
                metadata={
                    "function": func.__name__,
                    "args": args,
                    "kwargs": kwargs
                }
            )
            
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                # Log exception
                logger.error(
                    f"Exception in {func.__name__}: {str(e)}",
                    metadata={
                        "function": func.__name__,
                        "exception": str(e),
                        "exception_type": type(e).__name__
                    }
                )
                raise
        
        return wrapper
    
    if func is None:
        return decorator
    return decorator(func)