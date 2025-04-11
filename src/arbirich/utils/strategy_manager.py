import logging
from typing import Dict, List, Tuple

from src.arbirich.config.config import STRATEGIES, get_strategy_config
from src.arbirich.constants import ORDER_BOOK_CHANNEL

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class StrategyManager:
    """
    Manager for strategy configuration and operations.
    Provides static methods for accessing strategy-specific settings.
    """

    @staticmethod
    def get_exchange_channels(strategy_name: str) -> Dict[str, List[str]]:
        """
        Get the exchanges and channels for a specific strategy.

        Args:
            strategy_name: Name of the strategy

        Returns:
            Dictionary mapping exchange names to channel lists
        """
        logger.info(f"Getting exchange channels for strategy: {strategy_name}")

        # Get strategy configuration
        strategy_config = get_strategy_config(strategy_name)
        if not strategy_config:
            logger.warning(f"No configuration found for strategy: {strategy_name}")
            return {}

        # Get exchanges from strategy configuration
        exchanges = strategy_config.get("exchanges", [])
        if not exchanges:
            logger.warning(f"No exchanges configured for strategy: {strategy_name}")
            return {}

        # Get pairs for this strategy
        pairs = strategy_config.get("pairs", [])
        if not pairs:
            logger.warning(f"No pairs configured for strategy: {strategy_name}")
            return {}

        # Create exchange to channels mapping
        exchange_channels = {}
        for exchange in exchanges:
            # Format channel names to match Redis publishing format: "order_book:{exchange}:{pair}"
            channels = [f"{ORDER_BOOK_CHANNEL}:{exchange}:{base}-{quote}" for base, quote in pairs]
            exchange_channels[exchange] = channels

        logger.info(f"Exchange channels for {strategy_name}: {exchange_channels}")
        return exchange_channels

    @staticmethod
    def get_pairs_for_strategy(strategy_name: str) -> List[Tuple[str, str]]:
        """
        Get the trading pairs for a specific strategy.

        Args:
            strategy_name: Name of the strategy

        Returns:
            List of trading pairs (as tuples of base and quote currencies)
        """
        logger.info(f"Getting pairs for strategy: {strategy_name}")

        # Get strategy configuration
        strategy_config = get_strategy_config(strategy_name)
        if not strategy_config:
            logger.warning(f"No configuration found for strategy: {strategy_name}")
            return []

        # Get pairs from strategy configuration
        pairs = strategy_config.get("pairs", [])

        logger.info(f"Pairs for strategy {strategy_name}: {pairs}")
        return pairs

    @staticmethod
    def get_threshold(strategy_name: str) -> float:
        """
        Get the minimum spread threshold for a specific strategy.

        Args:
            strategy_name: Name of the strategy

        Returns:
            Minimum spread threshold as a float
        """
        logger.info(f"Getting threshold for strategy: {strategy_name}")

        # Get strategy configuration
        strategy_config = get_strategy_config(strategy_name)
        if not strategy_config:
            logger.warning(f"No configuration found for strategy: {strategy_name}")
            return 0.001  # Default threshold

        # Get threshold from strategy configuration
        threshold = strategy_config.get("min_spread", 0.001)

        logger.info(f"Threshold for strategy {strategy_name}: {threshold}")
        return threshold

    @staticmethod
    def get_active_strategies() -> List[str]:
        """
        Get a list of active strategy names.

        Returns:
            List of active strategy names
        """
        logger.info("Getting active strategies")

        # For now, all strategies in STRATEGIES are considered active
        active_strategies = list(STRATEGIES.keys())

        logger.info(f"Active strategies: {active_strategies}")
        return active_strategies
