import importlib
import logging
import pkgutil
from typing import Dict

from src.arbirich.config.config import STRATEGIES
from src.arbirich.services.strategies.base_strategy import ArbitrageStrategy
from src.arbirich.services.strategies.basic_arbitrage_strategy import BasicArbitrageStrategy
from src.arbirich.services.strategies.high_frequency_strategy import HighFrequencyStrategy
from src.arbirich.services.strategies.mid_price_arbitrage_strategy import MidPriceArbitrageStrategy

logger = logging.getLogger(__name__)

# Map strategy types to their implementation classes
STRATEGY_CLASSES = {
    "basic": BasicArbitrageStrategy,
    "mid_price": MidPriceArbitrageStrategy,
    "hft": HighFrequencyStrategy,
}

# Cache of initialized strategy instances
_strategy_instances: Dict[str, ArbitrageStrategy] = {}


def get_strategy(strategy_name: str) -> ArbitrageStrategy:
    """
    Get a strategy instance by name.

    Parameters:
        strategy_name: The name of the strategy

    Returns:
        An ArbitrageStrategy instance
    """
    # Return cached instance if available
    if strategy_name in _strategy_instances:
        return _strategy_instances[strategy_name]

    # Get strategy config from settings
    if strategy_name not in STRATEGIES:
        raise ValueError(f"Strategy '{strategy_name}' not found in configuration")

    config = STRATEGIES[strategy_name]

    # Determine strategy type from config or default to "basic"
    strategy_type = config.get("type", "basic")

    # Get the strategy class
    if strategy_type not in STRATEGY_CLASSES:
        logger.warning(f"Unknown strategy type '{strategy_type}', falling back to basic")
        strategy_type = "basic"

    strategy_class = STRATEGY_CLASSES[strategy_type]

    # Create strategy instance
    strategy = strategy_class(strategy_name, config)

    # Cache the instance
    _strategy_instances[strategy_name] = strategy

    logger.info(f"Created strategy instance '{strategy_name}' of type '{strategy_type}'")
    return strategy


def discover_strategies() -> None:
    """
    Discover and register all strategy implementations.
    """
    logger.info("Discovering strategy implementations...")

    # Get the module path for the strategies package
    import src.arbirich.services.strategies as strategies_pkg

    for _, name, is_pkg in pkgutil.iter_modules(strategies_pkg.__path__):
        if not is_pkg and name not in ("base_strategy", "strategy_factory"):
            try:
                # Import the module
                module = importlib.import_module(f"src.arbirich.services.strategies.{name}")
                logger.debug(f"Imported strategy module: {name}")

                # Strategy classes should register themselves or be registered here
            except Exception as e:
                logger.error(f"Error importing strategy module {name}: {e}")
