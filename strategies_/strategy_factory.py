import importlib
import logging
import pkgutil
from typing import Dict

from arbirich.core.trading.strategy.types.vwap import VWAPArbitrage
from src.arbirich.config.config import STRATEGIES
from src.arbirich.core.trading.strategy.base import ArbitrageStrategy
from src.arbirich.core.trading.strategy.types.basic import BasicArbitrage
from src.arbirich.core.trading.strategy.types.mid_price import MidPriceArbitrage

logger = logging.getLogger(__name__)

# Map strategy types to their implementation classes - Updated to use new classes
STRATEGY_CLASSES = {
    "basic": BasicArbitrage,
    "mid_price": MidPriceArbitrage,
    "volume_adjusted": VWAPArbitrage,
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

    strategy_type = config.get("type")

    try:
        if strategy_type not in STRATEGY_CLASSES:
            raise ValueError(f"Unknown strategy type '{strategy_type}' not found in configuration")

        strategy_class = STRATEGY_CLASSES[strategy_type]

        if strategy_type in ["basic", "mid_price", "volume_adjusted"]:
            strategy = strategy_class(
                strategy_id=str(strategy_name),  # Use strategy name as ID
                strategy_name=strategy_name,
                config=config,
            )
    except Exception as e:
        raise ValueError(f"Error creating strategy instance for '{strategy_name}': {e}")

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
    import src.arbirich.core.trading.strategy as strategies_pkg

    for _, name, is_pkg in pkgutil.iter_modules(strategies_pkg.__path__):
        if not is_pkg and name not in ("base", "strategy_factory"):
            try:
                # Import the module
                importlib.import_module(f"src.arbirich.core.trading.strategy..{name}")
                logger.debug(f"Imported strategy module: {name}")
            except Exception as e:
                logger.error(f"Error importing strategy module {name}: {e}")
