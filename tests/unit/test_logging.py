import logging

from activity_store.logging import StructuredLogger, get_logger, with_logging
from tests.utils import capture_logs, get_metadata_from_logs


class TestLogging:
    """Test the structured logging functionality."""
    
    def test_structured_logger_creation(self):
        """Test creating a StructuredLogger instance."""
        # Default creation
        logger = StructuredLogger()
        assert logger.logger.name == "activity_store"
        assert logger.default_metadata == {}
        
        # With custom logger and metadata
        python_logger = logging.getLogger("custom")
        custom_metadata = {"service": "test"}
        logger = StructuredLogger(logger=python_logger, default_metadata=custom_metadata)
        
        assert logger.logger.name == "custom"
        assert logger.default_metadata == custom_metadata
    
    def test_structured_logger_methods(self):
        """Test that each logging method works properly."""
        # Create a StringIO to capture log output
        with capture_logs() as captured:
            logger = get_logger("test", default_metadata={"service": "test-service"})
            
            # Test each log level
            logger.debug("Debug message", metadata={"level": "debug"})
            logger.info("Info message", metadata={"level": "info"})
            logger.warning("Warning message", metadata={"level": "warning"})
            logger.error("Error message", metadata={"level": "error"})
            logger.critical("Critical message", metadata={"level": "critical"})
        
        # Check that we captured the expected number of logs
        assert len(captured) == 5
        
        # Check log levels
        assert captured[0].levelno == logging.DEBUG
        assert captured[1].levelno == logging.INFO
        assert captured[2].levelno == logging.WARNING
        assert captured[3].levelno == logging.ERROR
        assert captured[4].levelno == logging.CRITICAL
        
        # Check messages
        assert "Debug message" in str(captured[0].msg)
        assert "Info message" in str(captured[1].msg)
        assert "Warning message" in str(captured[2].msg)
        assert "Error message" in str(captured[3].msg)
        assert "Critical message" in str(captured[4].msg)
    
    def test_metadata_inclusion(self):
        """Test that metadata is properly included in logs."""
        with capture_logs() as captured:
            logger = get_logger("test", default_metadata={"service": "test-service"})
            
            # Log with additional metadata
            logger.info(
                "Message with metadata",
                metadata={"request_id": "123", "user_id": "user123"}
            )
        
        # Extract metadata from logs
        metadata = get_metadata_from_logs(captured)
        
        # Check default metadata is included
        assert metadata["service"] == "test-service"
        
        # Check specific metadata for this log
        assert metadata["request_id"] == "123"
        assert metadata["user_id"] == "user123"
    
    def test_default_metadata_merging(self):
        """Test that default metadata is merged with log-specific metadata."""
        with capture_logs() as captured:
            # Create logger with default metadata
            logger = get_logger("test", default_metadata={
                "service": "test-service",
                "environment": "testing"
            })
            
            # Log with additional metadata, including an override
            logger.info(
                "Message with metadata",
                metadata={
                    "request_id": "123",
                    "service": "override-service"  # This should override the default
                }
            )
        
        # Extract metadata from logs
        metadata = get_metadata_from_logs(captured)
        
        # Check that override worked
        assert metadata["service"] == "override-service"
        
        # Other default metadata should still be there
        assert metadata["environment"] == "testing"
        
        # And specific metadata for this log
        assert metadata["request_id"] == "123"
    
    def test_get_logger(self):
        """Test the get_logger function."""
        # Default get_logger
        logger1 = get_logger()
        assert logger1.logger.name == "activity_store"
        
        # With name
        logger2 = get_logger("test")
        assert logger2.logger.name == "activity_store.test"
        
        # With nested name
        logger3 = get_logger("test.nested")
        assert logger3.logger.name == "activity_store.test.nested"
        
        # With default metadata
        metadata = {"service": "test"}
        logger4 = get_logger(default_metadata=metadata)
        assert logger4.default_metadata == metadata
    
    def test_with_logging_decorator(self):
        """Test the with_logging decorator."""
        with capture_logs() as captured:
            # Define a decorated function
            @with_logging
            def test_func(a, b, c=None):
                if c is None:
                    raise ValueError("c cannot be None")
                return a + b + c
            
            # Call function and handle exception
            try:
                test_func(1, 2)
            except ValueError:
                pass
            
            # Call function successfully
            result = test_func(1, 2, 3)
            assert result == 6
        
        # Check that we have function call logs
        assert len(captured) >= 2
        
        # Get the first log (function call)
        call_log = captured[0]
        assert "Calling test_func" in str(call_log.msg)
        assert call_log.levelno == logging.DEBUG
        
        # Extract metadata
        call_metadata = getattr(call_log, "metadata", {})
        assert call_metadata.get("function") == "test_func"
        
        # Check for exception log
        exception_logs = [log for log in captured if "Exception in test_func" in str(log.msg)]
        assert len(exception_logs) == 1
        exception_log = exception_logs[0]
        assert exception_log.levelno == logging.ERROR
        
        # Extract exception metadata
        exception_metadata = getattr(exception_log, "metadata", {})
        assert exception_metadata.get("exception_type") == "ValueError"
    
    def test_with_logging_custom_logger(self):
        """Test the with_logging decorator with a custom logger."""
        custom_logger = get_logger("custom")
        
        with capture_logs() as captured:
            # Define a decorated function with custom logger
            @with_logging(logger=custom_logger, level=logging.INFO)
            def test_func(a, b):
                return a + b
            
            # Call function
            result = test_func(1, 2)
            assert result == 3
        
        # Check that we have a log
        assert len(captured) >= 1
        
        # Get the log
        log = captured[0]
        assert "Calling test_func" in str(log.msg)
        assert log.levelno == logging.INFO  # Custom level
        
        # Extract metadata
        metadata = getattr(log, "metadata", {})
        assert metadata.get("function") == "test_func"
    
    def test_async_with_logging(self):
        """Test the with_logging decorator on async functions."""
        with capture_logs() as captured:
            # Define a decorated async function
            @with_logging
            async def async_test_func(a, b):
                return a + b
            
            # Run the async function
            import asyncio
            result = asyncio.run(async_test_func(1, 2))
            assert result == 3
        
        # Check that we have a log
        assert len(captured) >= 1
        
        # Get the log
        log = captured[0]
        assert "Calling async_test_func" in str(log.msg)
        
        # Extract metadata
        metadata = getattr(log, "metadata", {})
        assert metadata.get("function") == "async_test_func"