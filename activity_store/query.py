# Query model for ActivityStore querying functionality
# Provides a structured format for querying operations

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field


class Query(BaseModel):
    """
    Structured query object for ActivityStore queries.
    
    This model provides a way to specify search criteria for querying
    ActivityStream objects stored in the ActivityStore.
    """
    
    text: Optional[str] = Field(
        default=None,
        description="Free text search across all object content"
    )
    
    keywords: Optional[List[str]] = Field(
        default=None,
        description="List of keywords to match against"
    )
    
    sort: Optional[str] = Field(
        default=None,
        description="Field to sort by, with optional direction (e.g., 'published:desc')"
    )
    
    size: int = Field(
        default=10,
        description="Maximum number of results to return"
    )
    
    after: Optional[str] = Field(
        default=None,
        description="Pagination token for results after this point"
    )
    
    collection: Optional[str] = Field(
        default=None,
        description="Collection to query within"
    )
    
    type: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Object type(s) to match"
    )
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert query to a dictionary representation.
        
        Filters out None values for cleaner representation.
        
        Returns:
            Dictionary with query parameters
        """
        return {k: v for k, v in self.dict().items() if v is not None}