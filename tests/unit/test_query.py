import pytest
from pydantic import ValidationError

from activity_store.query import Query


class TestQuery:
    """Test the Query model in activity_store.query module."""
    
    def test_query_initialization_defaults(self):
        """Test that Query can be initialized with default values."""
        query = Query()
        
        # Check default values
        assert query.text is None
        assert query.keywords is None
        assert query.sort is None
        assert query.size == 10  # Default size should be 10
        assert query.after is None
        assert query.collection is None
        assert query.type is None
    
    def test_query_initialization_with_values(self):
        """Test that Query can be initialized with specific values."""
        query = Query(
            text="test query",
            keywords=["keyword1", "keyword2"],
            sort="published:desc",
            size=20,
            after="some_token",
            collection="notes",
            type="Note"
        )
        
        # Check values
        assert query.text == "test query"
        assert query.keywords == ["keyword1", "keyword2"]
        assert query.sort == "published:desc"
        assert query.size == 20
        assert query.after == "some_token"
        assert query.collection == "notes"
        assert query.type == "Note"
    
    def test_query_type_can_be_string_or_list(self):
        """Test that Query.type can be either a string or a list of strings."""
        # String type
        query1 = Query(type="Note")
        assert query1.type == "Note"
        
        # List of strings
        query2 = Query(type=["Note", "Article"])
        assert query2.type == ["Note", "Article"]
    
    def test_query_to_dict(self):
        """Test that Query.to_dict converts the query to a dictionary without None values."""
        # With some None values
        query = Query(
            text="test query",
            keywords=None,
            sort="published:desc",
            size=20,
            after=None,
            collection="notes",
            type=None
        )
        
        expected = {
            "text": "test query",
            "sort": "published:desc",
            "size": 20,
            "collection": "notes"
        }
        
        assert query.to_dict() == expected
        
        # With all values
        query = Query(
            text="test query",
            keywords=["keyword1", "keyword2"],
            sort="published:desc",
            size=20,
            after="some_token",
            collection="notes",
            type="Note"
        )
        
        expected = {
            "text": "test query",
            "keywords": ["keyword1", "keyword2"],
            "sort": "published:desc",
            "size": 20,
            "after": "some_token",
            "collection": "notes",
            "type": "Note"
        }
        
        assert query.to_dict() == expected
        
        # With no values (just defaults)
        query = Query()
        expected = {"size": 10}
        assert query.to_dict() == expected
    
    def test_query_update(self):
        """Test that Query objects can be updated with new values."""
        query = Query(text="initial query", size=10)
        
        # Update values using model_dump instead of dict
        new_query = Query(
            **{**query.model_dump(), "text": "updated query", "collection": "notes"}
        )
        
        assert new_query.text == "updated query"
        assert new_query.size == 10  # Preserved
        assert new_query.collection == "notes"  # Added
    
    def test_query_validation(self):
        """Test that Query validates input values."""
        # Size must be a positive integer
        with pytest.raises(ValidationError):
            Query(size=0)
            
        with pytest.raises(ValidationError):
            Query(size=-1)
        
        # Keywords must be a list of strings
        with pytest.raises(ValidationError):
            Query(keywords=123)  # Not a list