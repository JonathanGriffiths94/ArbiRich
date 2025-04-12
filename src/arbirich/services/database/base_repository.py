import logging
from typing import Generic, Type, TypeVar

import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.arbirich.config.config import DATABASE_URL
from src.arbirich.models.schema import metadata

# Create engine
engine = sa.create_engine(DATABASE_URL)
metadata.create_all(engine)

logger = logging.getLogger(__name__)

# Type variable for generic repository pattern
T = TypeVar("T")


class BaseRepository(Generic[T]):
    """Base repository with common database operations"""

    def __init__(self, model_class: Type[T], engine: Engine = engine):
        self.engine = engine
        self.connection = None
        self._session = None
        self.Session = sessionmaker(bind=engine)
        self.model_class = model_class
        self.logger = logger

    def __enter__(self):
        self.connection = self.engine.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()
            self.connection = None
        if self._session:
            self._session.close()
            self._session = None

    @property
    def session(self) -> Session:
        """Get a SQLAlchemy session."""
        if self._session is None:
            self._session = self.Session()
        return self._session

    def close(self):
        """Close the database connection and release resources."""
        try:
            if hasattr(self, "session") and self._session:
                self._session.close()
            if hasattr(self, "engine"):
                self.engine.dispose()
            logger.debug("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")
