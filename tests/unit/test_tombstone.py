import pytest
from datetime import datetime
import re

from activity_store.store import ActivityStore
from tests.utils import create_test_ld_object


class TestTombstone:
    """Test the tombstone conversion functionality."""
    
    @pytest.fixture
    def sample_object(self):
        """Create a sample Activity Streams object."""
        return create_test_ld_object(
            id="https://example.com/objects/123",
            type_="Note",
            content="This is a test note",
            published="2023-01-01T00:00:00Z"
        )
    
    @pytest.mark.asyncio
    async def test_convert_to_tombstone(self, activity_store, sample_object):
        """Test converting an object to a Tombstone."""
        # Convert to tombstone
        tombstone = await activity_store.convert_to_tombstone(sample_object)
        
        # Check basic tombstone properties
        assert tombstone["id"] == sample_object["id"]
        assert tombstone["type"] == "Tombstone"
        assert tombstone["formerType"] == sample_object["type"]
        assert "@context" in tombstone
        
        # Check deleted timestamp format (ISO 8601)
        iso8601_pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$'
        assert re.match(iso8601_pattern, tombstone["deleted"])
        
        # Retrieve the tombstone from the store
        retrieved = await activity_store.dereference(sample_object["id"])
        
        # Should be the same as the returned tombstone
        assert retrieved["id"] == tombstone["id"]
        assert retrieved["type"] == "Tombstone"
        assert retrieved["formerType"] == tombstone["formerType"]
        assert retrieved["deleted"] == tombstone["deleted"]
    
    @pytest.mark.asyncio
    async def test_convert_multiple_types_to_tombstone(self, activity_store):
        """Test converting an object with multiple types to a Tombstone."""
        # Object with multiple types
        obj = create_test_ld_object(
            id="https://example.com/objects/multi-type",
            type_=["Note", "Article"],
            content="Object with multiple types"
        )
        
        # Convert to tombstone
        tombstone = await activity_store.convert_to_tombstone(obj)
        
        # Check that formerType preserves the multiple types
        assert tombstone["formerType"] == ["Note", "Article"]
    
    @pytest.mark.asyncio
    async def test_convert_with_context_preservation(self, activity_store):
        """Test that context is preserved when converting to a Tombstone."""
        # Object with custom context
        obj = {
            "@context": [
                "https://www.w3.org/ns/activitystreams",
                {"custom": "http://example.org/custom#"}
            ],
            "id": "https://example.com/objects/custom-context",
            "type": "Note",
            "custom:property": "custom value"
        }
        
        # Convert to tombstone
        tombstone = await activity_store.convert_to_tombstone(obj)
        
        # Check that context is preserved
        assert tombstone["@context"] == obj["@context"]