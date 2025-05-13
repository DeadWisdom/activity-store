# Custom exceptions for the Activity Store package
# Provides specific error types for different failure scenarios

class ActivityStoreError(Exception):
    """Base exception for all ActivityStore errors."""
    pass


class ObjectNotFound(ActivityStoreError, KeyError):
    """Raised when an object with a specific ID cannot be found in the store."""
    pass


class InvalidLDObject(ActivityStoreError, ValueError):
    """Raised when an object does not conform to the LD-object specification."""
    pass