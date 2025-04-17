import logging
from typing import Any, Dict

from src.arbirich.config.config import get_strategy_config
from src.arbirich.core.trading.strategy.types.basic import BasicArbitrage
from src.arbirich.core.trading.strategy.types.liquidity_adjusted import LiquidityAdjustedArbitrage
from src.arbirich.core.trading.strategy.types.mid_price import MidPriceArbitrage
from src.arbirich.core.trading.strategy.types.vwap import VWAPArbitrage
from src.arbirich.models.enums import StrategyType

logger = logging.getLogger(__name__)

STRATEGY_TYPES = {
    StrategyType.BASIC: BasicArbitrage,
    StrategyType.MID_PRICE: MidPriceArbitrage,
    StrategyType.VWAP: VWAPArbitrage,
    StrategyType.LIQUIDITY_ADJUSTED: LiquidityAdjustedArbitrage,
}

_strategy_instances = {}


def get_strategy(strategy_name: str) -> Any:
    """
    Get a strategy instance by name.

    Args:
        strategy_name: The name of the strategy

    Returns:
        Strategy instance

    Raises:
        ValueError: If strategy configuration is not found or has invalid values
    """
    # Check if we already have an instance
    if strategy_name in _strategy_instances:
        logger.debug(f"Returning cached strategy instance for: {strategy_name}")
        return _strategy_instances[strategy_name]

    # Get strategy configuration
    config = get_strategy_config(strategy_name)
    if not config:
        raise ValueError(f"Strategy configuration not found for: {strategy_name}")

    # Get strategy type
    strategy_type_str = config.get("type")
    if not strategy_type_str:
        raise ValueError(f"Strategy type not specified for: {strategy_name}")

    # Convert string to enum value
    strategy_type = StrategyType(strategy_type_str)

    # Get strategy class
    strategy_class = STRATEGY_TYPES.get(strategy_type)
    if not strategy_class:
        raise ValueError(f"Strategy class not found for type: {strategy_type}")

    # Create strategy instance
    logger.info(f"Creating new strategy instance: {strategy_name} (type: {strategy_type.value})")

    # Initialize with the proper parameters
    threshold = config.get("threshold", 0.001)

    # Log the configuration being used
    logger.info(f"Strategy config: name={strategy_name}, threshold={threshold}")

    # Create the strategy instance with the correct parameters
    strategy = strategy_class(strategy_id=strategy_name, strategy_name=strategy_name, config=config)

    # Cache the instance
    _strategy_instances[strategy_name] = strategy

    # Log success
    logger.info(f"Successfully created strategy: {strategy_name}")
    return strategy


def get_all_strategies() -> Dict[str, Any]:
    """
    Get all available strategy instances.

    Returns:
        Dictionary of strategy instances by name

    Raises:
        Exception: If any strategies fail to initialize
    """
    from src.arbirich.config.config import get_all_strategy_names

    strategy_names = get_all_strategy_names()
    strategies = {}
    failed_strategies = []

    for name in strategy_names:
        try:
            strategies[name] = get_strategy(name)
        except Exception as e:
            logger.error(f"Failed to initialize strategy {name}: {e}")
            failed_strategies.append((name, str(e)))

    if failed_strategies:
        raise Exception(f"Failed to initialize {len(failed_strategies)} strategies: {failed_strategies}")

    return strategies


def clear_strategy_cache():
    """Clear the strategy instance cache"""
    global _strategy_instances
    _strategy_instances = {}
    logger.info("Strategy cache cleared")
