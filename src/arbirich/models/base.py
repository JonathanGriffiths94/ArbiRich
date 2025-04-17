"""
Base models and utilities for the ArbiRich application.
"""

import json
import uuid
from datetime import datetime
from typing import Any, Dict, Optional, Type, TypeVar

from pydantic import BaseModel as PydanticBaseModel
from pydantic import ConfigDict, Field

# Type variable for model classes
T = TypeVar("T", bound="BaseModel")


class BaseModel(PydanticBaseModel):
    """Base model with common functionality for all models."""

    model_config = ConfigDict(
        from_attributes=True,  # Allow loading from ORM objects
        validate_assignment=True,  # Validate on attribute assignment
        arbitrary_types_allowed=True,  # Allow arbitrary types
        populate_by_name=True,  # Allow population by field name
    )

    def to_dict(self) -> Dict[str, Any]:
        """Convert model to a dictionary."""
        return self.model_dump()

    def to_json(self) -> str:
        """Convert model to a JSON string."""
        return json.dumps(self.to_dict())

    def to_db_dict(self) -> Dict[str, Any]:
        """
        Convert model to a dictionary suitable for database insertion.
        This method should be overridden by subclasses if they need
        specific database conversion logic.
        """
        data = self.to_dict()

        # Convert datetime objects to ISO format strings
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.isoformat()
            elif isinstance(value, uuid.UUID):
                data[key] = str(value)
            elif isinstance(value, dict):
                data[key] = json.dumps(value)

        return data

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        """Create a model instance from a dictionary."""
        return cls(**data)

    @classmethod
    def from_json(cls: Type[T], json_string: str) -> T:
        """Create a model instance from a JSON string."""
        data = json.loads(json_string)
        return cls.from_dict(data)


class TimestampedModel(BaseModel):
    """Base model with created_at and updated_at timestamps."""

    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = Field(default_factory=datetime.utcnow)


class IdentifiableModel(BaseModel):
    """Base model with a unique identifier."""

    id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))


class StatusAwareModel(BaseModel):
    """Base model with an active/inactive status."""

    is_active: bool = False
