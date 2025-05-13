import pytest

from activity_store.exceptions import ActivityStoreError, InvalidLDObject, ObjectNotFound


class TestExceptions:
    """Test custom exceptions defined in the activity_store.exceptions module."""
    
    def test_activity_store_error_inheritance(self):
        """Test that ActivityStoreError inherits from Exception."""
        assert issubclass(ActivityStoreError, Exception)
        
        # Test instance creation and message
        error = ActivityStoreError("Test error message")
        assert str(error) == "Test error message"
    
    def test_object_not_found_inheritance(self):
        """Test that ObjectNotFound inherits from ActivityStoreError and KeyError."""
        assert issubclass(ObjectNotFound, ActivityStoreError)
        assert issubclass(ObjectNotFound, KeyError)
        
        # Test instance creation - KeyError formats strings with quotes
        error = ObjectNotFound("Object with ID 'test' not found")
        # Note: KeyError adds quotes to the string representation
        assert "Object with ID 'test' not found" in str(error)
        
        # Test that it can be caught as both parent types
        with pytest.raises(ActivityStoreError):
            raise ObjectNotFound("Test")
            
        with pytest.raises(KeyError):
            raise ObjectNotFound("Test")
    
    def test_invalid_ld_object_inheritance(self):
        """Test that InvalidLDObject inherits from ActivityStoreError and ValueError."""
        assert issubclass(InvalidLDObject, ActivityStoreError)
        assert issubclass(InvalidLDObject, ValueError)
        
        # Test instance creation and message
        error = InvalidLDObject("Invalid LD object: missing required field 'id'")
        assert str(error) == "Invalid LD object: missing required field 'id'"
        
        # Test that it can be caught as both parent types
        with pytest.raises(ActivityStoreError):
            raise InvalidLDObject("Test")
            
        with pytest.raises(ValueError):
            raise InvalidLDObject("Test")
    
    def test_exception_in_try_except(self):
        """Test that exceptions can be caught in try/except blocks."""
        # Test ActivityStoreError
        try:
            raise ActivityStoreError("Test error")
        except ActivityStoreError as e:
            assert str(e) == "Test error"
        
        # Test ObjectNotFound - KeyError formats strings with quotes
        try:
            raise ObjectNotFound("Test object not found")
        except ObjectNotFound as e:
            assert "Test object not found" in str(e)
        
        # Test that ObjectNotFound can be caught as either ActivityStoreError or KeyError
        try:
            raise ObjectNotFound("Test object not found")
        except (ActivityStoreError, KeyError) as e:
            assert "Test object not found" in str(e)
        
        # Test InvalidLDObject
        try:
            raise InvalidLDObject("Test invalid object")
        except InvalidLDObject as e:
            assert str(e) == "Test invalid object"
        
        # Test that InvalidLDObject can be caught as either ActivityStoreError or ValueError
        try:
            raise InvalidLDObject("Test invalid object")
        except (ActivityStoreError, ValueError) as e:
            assert str(e) == "Test invalid object"