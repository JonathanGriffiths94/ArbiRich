import logging
from typing import Dict, Optional

from arbirich.core.trading.strategy.types.vwap import VWAPArbitrage
from src.arbirich.core.trading.strategy.base import ArbitrageStrategy
from src.arbirich.core.trading.strategy.types.basic import BasicArbitrage
from src.arbirich.core.trading.strategy.types.mid_price import MidPriceArbitrage

logger = logging.getLogger(__name__)

# Map strategy types to their implementation classes
STRATEGY_CLASSES = {
    "basic": BasicArbitrage,
    "mid_price": MidPriceArbitrage,
    "volume_adjusted": VWAPArbitrage,
}

# Cache of initialized strategy instances
_strategy_instances = {}


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
        logger.debug(f"Returning cached strategy instance for '{strategy_name}'")
        return _strategy_instances[strategy_name]

    # Get strategy config from settings
    from src.arbirich.config.config import STRATEGIES

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

    # Create strategy instance - handle both new and old constructor styles
    try:
        # First try new constructor style with separate parameters
        strategy = strategy_class(
            strategy_id=str(strategy_name),
            strategy_name=strategy_name,
            config=config,
        )
    except TypeError:
        # Fall back to old constructor style if needed
        logger.debug(f"Using legacy constructor for strategy type '{strategy_type}'")
        strategy = strategy_class(strategy_name, config)

    # Cache the instance
    _strategy_instances[strategy_name] = strategy

    logger.info(f"Created strategy instance '{strategy_name}' of type '{strategy_type}'")
    return strategy


def create_strategy(
    strategy_type: str, strategy_id: str, strategy_name: str, config: Dict
) -> Optional[ArbitrageStrategy]:
    """
    Create a new strategy instance of the specified type.

    Parameters:
        strategy_type: The type of strategy to create
        strategy_id: Unique identifier for the strategy
        strategy_name: Name of the strategy
        config: Strategy configuration parameters

    Returns:
        A new strategy instance or None if the type is not recognized
    """
    if strategy_type not in STRATEGY_CLASSES:
        logger.error(f"Unknown strategy type: {strategy_type}")
        return None

    strategy_class = STRATEGY_CLASSES[strategy_type]

    try:
        # First try new constructor style
        strategy = strategy_class(strategy_id=strategy_id, strategy_name=strategy_name, config=config)
    except TypeError:
        # Fall back to old constructor style if needed
        logger.debug(f"Using legacy constructor for strategy type '{strategy_type}'")
        # For older constructors, we'll assume the first parameter is name and second is config
        strategy = strategy_class(strategy_name, config)

    logger.info(f"Created new strategy: {strategy_name} (type: {strategy_type}, id: {strategy_id})")
    return strategy


def clear_cache():
    """Clear the strategy instance cache."""
    global _strategy_instances
    _strategy_instances = {}
    logger.info("Strategy instance cache cleared")
