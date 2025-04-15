from abc import ABC, abstractmethod
from typing import Generic, List, Optional, TypeVar

T = TypeVar("T")


class Repository(ABC, Generic[T]):
    """Abstract base class for repository pattern implementation."""

    @abstractmethod
    def save(self, entity: T, **kwargs) -> bool:
        """Save an entity to the repository."""
        pass

    @abstractmethod
    def get_by_id(self, id: str, **kwargs) -> Optional[T]:
        """Get an entity by its ID."""
        pass

    @abstractmethod
    def get_recent(self, count: int = 10, **kwargs) -> List[T]:
        """Get most recent entities."""
        pass

    @abstractmethod
    def delete(self, id: str, **kwargs) -> bool:
        """Delete an entity by its ID."""
        pass

    @abstractmethod
    def publish(self, entity: T, **kwargs) -> int:
        """Publish an entity to relevant channels."""
        pass
