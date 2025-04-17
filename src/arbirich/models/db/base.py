"""
Base SQLAlchemy models and database connection utilities.
"""

import logging
from typing import Any, Dict

from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

# Create a metadata object with naming conventions for constraints
convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}
metadata = MetaData(naming_convention=convention)

# Create a base class for all SQLAlchemy models
Base = declarative_base(metadata=metadata)


class DatabaseManager:
    """Manage database connections and sessions."""

    def __init__(self, database_uri: str):
        """Initialize the database manager."""
        self.database_uri = database_uri
        self.engine = None
        self.async_session_factory = None

    async def initialize(self) -> None:
        """Initialize the database engine and session factory."""
        self.engine = create_async_engine(
            self.database_uri,
            echo=False,
            future=True,
        )
        self.async_session_factory = sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)

    async def create_tables(self) -> None:
        """Create all tables in the database."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def get_session(self) -> AsyncSession:
        """Get a new database session."""
        if not self.async_session_factory:
            await self.initialize()
        return self.async_session_factory()


# Convenience functions to convert between Pydantic and SQLAlchemy models


def pydantic_to_sqlalchemy(pydantic_model: Any, sqlalchemy_model: Any) -> Any:
    """Convert a Pydantic model to a SQLAlchemy model."""
    data = pydantic_model.model_dump(exclude_unset=True)
    # Filter out any fields that don't exist in the SQLAlchemy model
    filtered_data = {k: v for k, v in data.items() if hasattr(sqlalchemy_model, k) and v is not None}
    return sqlalchemy_model(**filtered_data)


def sqlalchemy_to_dict(sqlalchemy_model: Any) -> Dict[str, Any]:
    """Convert a SQLAlchemy model to a dictionary."""
    return {c.name: getattr(sqlalchemy_model, c.name) for c in sqlalchemy_model.__table__.columns}
