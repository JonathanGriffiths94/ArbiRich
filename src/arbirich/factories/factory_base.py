import logging
from typing import Callable, Dict, Generic, Optional, Type, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class FactoryBase(Generic[T]):
    """Base class for factory pattern implementation with type safety."""

    def __init__(self, factory_name: str):
        """
        Initialize a new factory.

        Parameters:
            factory_name: The name of this factory (for logging)
        """
        self.factory_name = factory_name
        self._registry: Dict[str, Type[T]] = {}
        logger.info(f"Initialized {factory_name} factory")

    def register(self, name: str, cls: Type[T]) -> None:
        """
        Register a class with a specific name.

        Parameters:
            name: The name to register the class under
            cls: The class to register
        """
        self._registry[name] = cls
        logger.info(f"Registered {cls.__name__} as '{name}' in {self.factory_name} factory")

    def get_instance(self, name: str) -> Optional[Type[T]]:
        """
        Get a class by name.

        Parameters:
            name: The name to look up

        Returns:
            The registered class or None if not found
        """
        if name in self._registry:
            return self._registry[name]

        logger.warning(f"No {self.factory_name} registered with name '{name}'")
        return None

    def create_decorator(self) -> Callable[[str], Callable[[Type[T]], Type[T]]]:
        """
        Create a decorator for registering classes with this factory.

        Returns:
            A decorator function
        """

        def decorator(name: str) -> Callable[[Type[T]], Type[T]]:
            def wrapper(cls: Type[T]) -> Type[T]:
                self.register(name, cls)
                return cls

            return wrapper

        return decorator
