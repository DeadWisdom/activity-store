import asyncio
import io
import logging
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

from activity_store.logging import StructuredLogger


def create_test_ld_object(id: str = "https://example.com/test", 
                         type_: str = "Note", 
                         **kwargs) -> Dict[str, Any]:
    """
    Create a test LD-object with the given parameters.
    
    Args:
        id: Object ID
        type_: Object type
        **kwargs: Additional properties
    
    Returns:
        An LD-object dictionary
    """
    obj = {
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": id,
        "type": type_,
    }
    obj.update(kwargs)
    return obj


@contextmanager
def capture_logs():
    """
    Context manager to capture logs during tests.
    
    Yields:
        A list that will contain the captured log records
    """
    captured_logs = []
    handler = logging.StreamHandler(io.StringIO())
    
    # Save the log records
    handler.emit = lambda record: captured_logs.append(record)
    
    # Add handler to the logger
    logger = logging.getLogger("activity_store")
    level = logger.getEffectiveLevel()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    
    try:
        yield captured_logs
    finally:
        # Clean up
        logger.removeHandler(handler)
        logger.setLevel(level)


def get_metadata_from_logs(logs: List[logging.LogRecord], 
                          message_contains: Optional[str] = None) -> Dict[str, Any]:
    """
    Extract metadata from captured logs.
    
    Args:
        logs: List of captured log records
        message_contains: Optional substring to filter logs by message content
    
    Returns:
        Dictionary of metadata from matching log records
    """
    matching_logs = logs
    if message_contains:
        matching_logs = [
            log for log in logs 
            if hasattr(log, 'msg') and message_contains in str(log.msg)
        ]
    
    metadata = {}
    for log in matching_logs:
        if hasattr(log, 'metadata'):
            metadata.update(log.metadata)
    
    return metadata


async def run_concurrently(*coroutines):
    """
    Run multiple coroutines concurrently.
    
    Args:
        *coroutines: Coroutines to run
    
    Returns:
        List of results from the coroutines
    """
    return await asyncio.gather(*coroutines)