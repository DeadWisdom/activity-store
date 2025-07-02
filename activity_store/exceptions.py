# Custom exceptions for the Activity Store package
# Provides specific error types for different failure scenarios


class ActivityStoreError(Exception):
    """Base exception for all ActivityStore errors."""

    pass


class InvalidLDObject(ActivityStoreError, ValueError):
    """Raised when an object does not conform to the LD-object specification."""

    pass
