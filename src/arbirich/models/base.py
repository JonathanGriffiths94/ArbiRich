"""
Base models and utilities for the ArbiRich application.
"""

import json
import uuid
from datetime import datetime
from typing import Any, Dict, Optional, Type, TypeVar

from pydantic import BaseModel as PydanticBaseModel
from pydantic import ConfigDict
from sqlalchemy import DateTime, Integer
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

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


class IdentifiableModel(BaseModel):
    """Base model with a unique identifier."""

    id: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)


class UUIDModel(BaseModel):
    """Base model with a UUID identifier."""

    id: Mapped[uuid.UUID] = mapped_column(UUID, primary_key=True, default=uuid.uuid4)


class TimestampedModel(BaseModel):
    """Base model with created_at and updated_at timestamps."""

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class StatusAwareModel(BaseModel):
    """Base model with an active/inactive status."""

    status: Mapped[str]
    status_reason: Mapped[Optional[str]] = mapped_column(nullable=True)
    status_changed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)


class TradeOpportunityModel(UUIDModel, TimestampedModel):
    """Base model for trade opportunities."""

    pass


class TradeExecutionModel(UUIDModel, TimestampedModel):
    """Base model for trade executions."""

    pass


class TradeExecutionResultModel(UUIDModel, TimestampedModel):
    """Base model for trade execution results."""

    pass
