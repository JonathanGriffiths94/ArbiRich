import logging
from typing import Any, Dict, List, Optional, Tuple

from src.arbirich.config.config import STRATEGIES
from src.arbirich.services.strategies.strategy_configs import (
    get_all_strategy_names,
    get_strategy_config,
)

logger = logging.getLogger(__name__)


class StrategyManager:
    """Utility class to manage strategy configurations and mapping"""

    @staticmethod
    def get_strategy_config(strategy_name: str) -> Optional[Dict[str, Any]]:
        """
        Get the configuration for a specific strategy

        Parameters:
            strategy_name: The name of the strategy to get the configuration for

        Returns:
            The strategy configuration or None if not found
        """
        if strategy_name in STRATEGIES:
            return STRATEGIES[strategy_name]

        # If not in active strategies, try to get it from ALL_STRATEGIES
        config = get_strategy_config(strategy_name)
        if config:
            return config

        logger.warning(f"Strategy '{strategy_name}' not found in configured strategies")
        return None

    @staticmethod
    def get_all_strategy_names() -> List[str]:
        """Get a list of all configured strategy names"""
        return get_all_strategy_names()

    @staticmethod
    def get_threshold(strategy_name: str) -> float:
        """Get the threshold value for a specific strategy"""
        config = StrategyManager.get_strategy_config(strategy_name)
        if config:
            return config.get("threshold", 0.001)

        logger.warning(f"Strategy {strategy_name} not found in configuration. Using default threshold.")
        return 0.001  # 0.1% default threshold

    @staticmethod
    def get_exchanges_for_strategy(strategy_name: str) -> List[str]:
        """Get the list of exchanges used by a specific strategy"""
        config = StrategyManager.get_strategy_config(strategy_name)
        if config and "exchanges" in config:
            return config["exchanges"]

        logger.warning(f"No exchanges found for strategy '{strategy_name}', using all exchanges")
        from src.arbirich.config.config import EXCHANGES

        return EXCHANGES  # Default to all exchanges if not specified

    @staticmethod
    def get_pairs_for_strategy(strategy_name: str) -> List[Tuple[str, str]]:
        """Get the list of trading pairs used by a specific strategy"""
        config = StrategyManager.get_strategy_config(strategy_name)
        if config and "pairs" in config:
            return config["pairs"]

        logger.warning(f"No pairs found for strategy '{strategy_name}', using default pairs")
        return [("BTC", "USDT")]  # Default pair if not specified

    @staticmethod
    def get_exchange_channels(strategy_name: str) -> Dict[str, str]:
        """
        Get a dictionary of exchange to channel mappings for a strategy.
        This is useful for configuring what data sources each strategy subscribes to.
        """
        exchanges = StrategyManager.get_exchanges_for_strategy(strategy_name)
        # Create a mapping of exchange -> channel name
        # Currently all exchanges use the same 'order_book' channel
        return {exchange: "order_book" for exchange in exchanges}

    @staticmethod
    def enable_debug_for_strategy(strategy_name: str) -> bool:
        """
        Check if debug mode should be enabled for a strategy.

        Parameters:
            strategy_name: The name of the strategy

        Returns:
            True if debug should be enabled, False otherwise
        """
        config = StrategyManager.get_strategy_config(strategy_name)
        if config:
            return config.get("debug", False)
        return False

    @staticmethod
    def get_opportunity_channel(strategy_name: str) -> str:
        """
        Get the Redis channel name to publish trade opportunities for a strategy.

        Parameters:
            strategy_name: The name of the strategy

        Returns:
            Redis channel name
        """
        return f"trade_opportunities_{strategy_name}"

    @staticmethod
    def get_strategy_type(strategy_name: str) -> str:
        """
        Get the type of the strategy (basic, mid_price, etc.)

        Parameters:
            strategy_name: The name of the strategy

        Returns:
            Strategy type as a string
        """
        config = StrategyManager.get_strategy_config(strategy_name)
        if config:
            return config.get("type", "basic")
        return "basic"  # Default to basic if not specified
