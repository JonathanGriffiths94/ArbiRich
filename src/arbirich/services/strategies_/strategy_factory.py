import importlib
import logging
import pkgutil
from typing import Dict

from src.arbirich.config.config import STRATEGIES
from src.arbirich.core.trading.strategy.base import ArbitrageStrategy
from src.arbirich.core.trading.strategy.types.basic import BasicArbitrage
from src.arbirich.core.trading.strategy.types.mid_price import MidPriceArbitrage
from src.arbirich.core.trading.strategy.types.volume_adjusted import VolumeAdjustedArbitrage

logger = logging.getLogger(__name__)

# Map strategy types to their implementation classes - Updated to use new classes
STRATEGY_CLASSES = {
    "basic": BasicArbitrage,
    "mid_price": MidPriceArbitrage,
    "volume_adjusted": VolumeAdjustedArbitrage,
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

    # Create strategy instance with appropriate constructor based on the new implementation
    if strategy_type in ["basic", "mid_price", "volume_adjusted"]:
        # New implementations use strategy_id, name, and config parameters
        strategy = strategy_class(
            strategy_id=str(strategy_name),  # Use strategy name as ID
            strategy_name=strategy_name,
            config=config,
        )
    else:
        # Fallback for any legacy implementations
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
    import src.arbirich.services.strategies_ as strategies_pkg

    for _, name, is_pkg in pkgutil.iter_modules(strategies_pkg.__path__):
        if not is_pkg and name not in ("base_strategy", "strategy_factory"):
            try:
                # Import the module
                module = importlib.import_module(f"src.arbirich.services.strategies.{name}")
                logger.debug(f"Imported strategy module: {name}")

                # Strategy classes should register themselves or be registered here
            except Exception as e:
                logger.error(f"Error importing strategy module {name}: {e}")
