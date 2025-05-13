import datetime
import pytest
from unittest.mock import patch, MagicMock

from activity_store.ld import (
    expand_property, compact_property, normalize, any_none, with_prefixes
)


class TestLDFunctions:
    """Test the LD-handling functions in activity_store.ld module."""
    
    def test_with_prefixes_no_prefixes(self):
        """Test that with_prefixes returns the original context if no prefixes provided."""
        context = "https://www.w3.org/ns/activitystreams"
        result = with_prefixes(context, None)
        assert result == context
        
        context = ["https://www.w3.org/ns/activitystreams"]
        result = with_prefixes(context, None)
        assert result == context
    
    def test_with_prefixes_adds_prefixes(self):
        """Test that with_prefixes correctly adds prefixes to context."""
        # String context
        context = "https://www.w3.org/ns/activitystreams"
        prefixes = {"schema": "http://schema.org/"}
        result = with_prefixes(context, prefixes)
        assert result == [context, prefixes]
        
        # List context
        context = ["https://www.w3.org/ns/activitystreams"]
        result = with_prefixes(context, prefixes)
        assert result == context + [prefixes]
    
    def test_any_none_simple_values(self):
        """Test any_none with simple values."""
        assert any_none(None) is True
        assert any_none("not none") is False
        assert any_none(123) is False
        assert any_none({}) is False
        assert any_none([]) is False
    
    def test_any_none_nested_dict(self):
        """Test any_none with nested dictionaries."""
        # No None values
        doc = {
            "id": "test",
            "name": "Test Object",
            "nested": {"key": "value"}
        }
        assert any_none(doc) is False
        
        # With None value
        doc = {
            "id": "test",
            "name": None,
            "nested": {"key": "value"}
        }
        assert any_none(doc) is True
        
        # With nested None value
        doc = {
            "id": "test",
            "name": "Test Object",
            "nested": {"key": None}
        }
        assert any_none(doc) is True
    
    def test_any_none_nested_list(self):
        """Test any_none with nested lists."""
        # No None values
        doc = {
            "id": "test",
            "items": ["item1", "item2", {"key": "value"}]
        }
        assert any_none(doc) is False
        
        # With None in list
        doc = {
            "id": "test",
            "items": ["item1", None, "item2"]
        }
        assert any_none(doc) is True
        
        # With None in nested dict in list
        doc = {
            "id": "test",
            "items": ["item1", {"key": None}, "item2"]
        }
        assert any_none(doc) is True
    
    def test_expand_property_single_property(self):
        """Test expand_property with a single property name."""
        # Create a test document with a mix of single values and lists
        doc = {
            "id": "test",
            "type": "Note",
            "name": "Test Note",
            "tag": "test-tag",
            "attachment": [{"url": "attachment1"}, {"url": "attachment2"}],
            "nested": {
                "tag": "nested-tag",
                "items": [{"tag": "item-tag"}]
            }
        }
        
        # Expand the 'tag' property
        expand_property(doc, "tag")
        
        # Check that all 'tag' properties are lists now
        assert doc["tag"] == ["test-tag"]
        assert doc["nested"]["tag"] == ["nested-tag"]
        assert doc["nested"]["items"][0]["tag"] == ["item-tag"]
        
        # Other properties should be unchanged
        assert doc["id"] == "test"
        assert doc["type"] == "Note"
        assert doc["name"] == "Test Note"
        assert isinstance(doc["attachment"], list)
    
    def test_expand_property_multiple_properties(self):
        """Test expand_property with multiple property names."""
        # Create a test document
        doc = {
            "id": "test",
            "type": "Note",
            "name": "Test Note",
            "tag": "test-tag",
            "attachment": {"url": "attachment-url"}
        }
        
        # Expand multiple properties
        expand_property(doc, ["type", "tag", "attachment"])
        
        # Check that specified properties are lists now
        assert doc["type"] == ["Note"]
        assert doc["tag"] == ["test-tag"]
        assert doc["attachment"] == [{"url": "attachment-url"}]
        
        # Other properties should be unchanged
        assert doc["id"] == "test"
        assert doc["name"] == "Test Note"
    
    def test_compact_property_single_property(self):
        """Test compact_property with a single property name."""
        # Create a test document with list properties
        doc = {
            "id": "test",
            "type": ["Note"],
            "name": "Test Note",
            "tag": ["test-tag", "another-tag"],
            "nested": {
                "tag": ["nested-tag"],
                "items": [{"tag": ["item-tag"]}]
            }
        }
        
        # Compact the 'tag' property
        compact_property(doc, "tag")
        
        # Check that all 'tag' properties take the first value
        assert doc["tag"] == "test-tag"  # Takes first from list
        assert doc["nested"]["tag"] == "nested-tag"
        assert doc["nested"]["items"][0]["tag"] == "item-tag"
        
        # Other properties should be unchanged
        assert doc["id"] == "test"
        assert doc["type"] == ["Note"]
        assert doc["name"] == "Test Note"
    
    def test_compact_property_multiple_properties(self):
        """Test compact_property with multiple property names."""
        # Create a test document
        doc = {
            "id": "test",
            "type": ["Note", "Article"],
            "name": "Test Note",
            "tag": ["test-tag", "another-tag"],
            "attachment": [{"url": "attachment-url"}]
        }
        
        # Compact multiple properties
        compact_property(doc, ["type", "tag", "attachment"])
        
        # Check that specified properties are compacted now
        assert doc["type"] == "Note"
        assert doc["tag"] == "test-tag"
        assert doc["attachment"] == {"url": "attachment-url"}
        
        # Other properties should be unchanged
        assert doc["id"] == "test"
        assert doc["name"] == "Test Note"
    
    @patch('activity_store.ld.jsonld.compact')
    def test_normalize(self, mock_compact):
        """Test normalize function."""
        # Setup mock
        mock_result = {
            "@graph": [{
                "id": "test",
                "type": "Note",
                "name": "Test Note"
            }]
        }
        mock_compact.return_value = mock_result
        
        # Call normalize
        doc = {"id": "test", "type": "Note"}
        result = normalize(doc)
        
        # Verify correct options passed to jsonld.compact
        mock_compact.assert_called_once()
        args = mock_compact.call_args[0]
        assert args[0] == doc  # First arg is the document
        assert args[1] == "https://www.w3.org/ns/activitystreams"  # Default context
        
        options = mock_compact.call_args[0][2]
        assert options["compactArrays"] is False
        
        # Verify graph handling
        assert result == {
            "id": "test",
            "type": ["Note"],
            "name": "Test Note"
        }
    
    @patch('activity_store.ld.jsonld.compact')
    def test_normalize_with_custom_context(self, mock_compact):
        """Test normalize with custom context."""
        # Setup mock
        mock_result = {
            "id": "test",
            "type": "Note",
            "name": "Test Note"
        }
        mock_compact.return_value = mock_result
        
        # Custom context
        custom_context = {
            "@context": {
                "@vocab": "http://example.org/",
                "name": "http://schema.org/name"
            }
        }
        
        # Call normalize with custom context
        doc = {"id": "test", "type": "Note"}
        result = normalize(doc, context=custom_context)
        
        # Verify correct context passed
        args = mock_compact.call_args[0]
        assert args[1] == custom_context
    
    @patch('activity_store.ld.jsonld.compact')
    def test_normalize_with_compact_keys(self, mock_compact):
        """Test normalize with compact_keys parameter."""
        # Setup mock with properties that should be compacted
        mock_result = {
            "id": "test",
            "type": ["Note"],
            "name": "Test Note",
            "attachment": [{"url": "test-url"}],
            "tag": ["tag1", "tag2"]
        }
        mock_compact.return_value = mock_result
        
        # Call normalize with compact_keys
        doc = {"id": "test", "type": "Note"}
        result = normalize(doc, compact_keys=["attachment", "tag"])
        
        # Verify type is still expanded (should happen by default)
        assert result["type"] == ["Note"]
        
        # Verify specified keys are compacted
        assert result["attachment"] == {"url": "test-url"}
        assert result["tag"] == "tag1"