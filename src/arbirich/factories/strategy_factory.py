import importlib
import inspect
import logging
import pkgutil
from typing import Dict, Type

from src.arbirich.config import STRATEGIES
from src.arbirich.strategies.base_strategy import ArbitrageStrategy

logger = logging.getLogger(__name__)


class StrategyFactory:
    """
    Factory class for creating and managing arbitrage strategy instances.
    Implements the Singleton pattern to ensure only one factory exists.
    """

    _instance = None

    # Map strategy types to their implementation classes
    _strategy_classes: Dict[str, Type[ArbitrageStrategy]] = {}

    # Cache of initialized strategy instances
    _strategy_instances: Dict[str, ArbitrageStrategy] = {}

    def __new__(cls):
        """Ensure only one instance of the factory exists (Singleton pattern)"""
        if cls._instance is None:
            cls._instance = super(StrategyFactory, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Initialize the factory if it hasn't been initialized yet"""
        if not getattr(self, "_initialized", False):
            self._discover_strategies()
            self._initialized = True

    def register_strategy(self, strategy_type: str, strategy_class: Type[ArbitrageStrategy]) -> None:
        """
        Register a new strategy class with the factory.

        Parameters:
            strategy_type: The type identifier for the strategy
            strategy_class: The strategy class to register
        """
        if strategy_type in self._strategy_classes:
            logger.warning(f"Overriding existing strategy type '{strategy_type}'")

        self._strategy_classes[strategy_type] = strategy_class
        logger.debug(f"Registered strategy type '{strategy_type}' with class {strategy_class.__name__}")

    def get_strategy(self, strategy_name: str) -> ArbitrageStrategy:
        """
        Get a strategy instance by name.

        Parameters:
            strategy_name: The name of the strategy

        Returns:
            An ArbitrageStrategy instance
        """
        # Return cached instance if available
        if strategy_name in self._strategy_instances:
            return self._strategy_instances[strategy_name]

        # Get strategy config from settings
        if strategy_name not in STRATEGIES:
            raise ValueError(f"Strategy '{strategy_name}' not found in configuration")

        config = STRATEGIES[strategy_name]

        # Determine strategy type from config or default to "basic"
        strategy_type = config.get("type", "basic")

        # Get the strategy class
        if strategy_type not in self._strategy_classes:
            logger.warning(f"Unknown strategy type '{strategy_type}', falling back to basic")
            strategy_type = "basic"
            # If basic isn't available, raise an error
            if "basic" not in self._strategy_classes:
                raise ValueError("No basic fallback strategy available")

        strategy_class = self._strategy_classes[strategy_type]

        # Create strategy instance
        strategy = strategy_class(strategy_name, config)

        # Cache the instance
        self._strategy_instances[strategy_name] = strategy

        logger.info(f"Created strategy instance '{strategy_name}' of type '{strategy_type}'")
        return strategy

    def get_available_strategy_types(self) -> list:
        """Return a list of all registered strategy types"""
        return list(self._strategy_classes.keys())

    def clear_cache(self) -> None:
        """Clear the strategy instance cache"""
        self._strategy_instances.clear()
        logger.debug("Strategy instance cache cleared")

    def _discover_strategies(self) -> None:
        """
        Discover and register all strategy implementations.
        """
        logger.info("Discovering strategy implementations...")

        # Get the module path for the strategies package
        import src.arbirich.strategies as strategies_pkg

        for _, name, is_pkg in pkgutil.iter_modules(strategies_pkg.__path__):
            if not is_pkg and name not in ("base_strategy", "strategy_factory"):
                try:
                    # Import the module
                    module = importlib.import_module(f"src.arbirich.strategies.{name}")
                    logger.debug(f"Imported strategy module: {name}")

                    # Find all ArbitrageStrategy subclasses in the module
                    for cls_name, cls in inspect.getmembers(module, inspect.isclass):
                        if (
                            issubclass(cls, ArbitrageStrategy)
                            and cls != ArbitrageStrategy
                            and hasattr(cls, "STRATEGY_TYPE")
                        ):
                            # Register the strategy using its STRATEGY_TYPE class attribute
                            self.register_strategy(cls.STRATEGY_TYPE, cls)
                            logger.info(f"Auto-registered strategy: {cls_name} as '{cls.STRATEGY_TYPE}'")

                except Exception as e:
                    logger.error(f"Error importing strategy module {name}: {e}")


# Create global factory instance for module-level access
_factory = StrategyFactory()


# Module-level functions that delegate to the singleton factory
def get_strategy(strategy_name: str) -> ArbitrageStrategy:
    """Get a strategy instance by name"""
    return _factory.get_strategy(strategy_name)


def register_strategy(strategy_type: str, strategy_class: Type[ArbitrageStrategy]) -> None:
    """Register a strategy class with the factory"""
    _factory.register_strategy(strategy_type, strategy_class)


def get_available_strategy_types() -> list:
    """Get a list of all available strategy types"""
    return _factory.get_available_strategy_types()


def clear_strategy_cache() -> None:
    """Clear the strategy instance cache"""
    _factory.clear_cache()
