import logging
from typing import Any, Dict, List, Optional, Tuple

from src.arbirich.config import STRATEGIES

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
        logger.warning(f"Strategy '{strategy_name}' not found in configured strategies")
        return None

    @staticmethod
    def get_all_strategy_names() -> List[str]:
        """Get a list of all configured strategy names"""
        return list(STRATEGIES.keys())

    @staticmethod
    def get_threshold(strategy_name: str) -> float:
        """Get the threshold value for a specific strategy"""
        config = StrategyManager.get_strategy_config(strategy_name)
        if config and "threshold" in config:
            return config["threshold"]
        logger.warning(f"Using default threshold 0.01 for strategy '{strategy_name}'")
        return 0.01  # Default threshold if not found

    @staticmethod
    def get_exchanges_for_strategy(strategy_name: str) -> List[str]:
        """Get the list of exchanges used by a specific strategy"""
        config = StrategyManager.get_strategy_config(strategy_name)
        if config and "exchanges" in config:
            return config["exchanges"]
        logger.warning(f"No exchanges found for strategy '{strategy_name}', using all exchanges")
        from src.arbirich.config import EXCHANGES

        return EXCHANGES  # Default to all exchanges if not specified

    @staticmethod
    def get_pairs_for_strategy(strategy_name: str) -> List[Tuple[str, str]]:
        """Get the list of trading pairs used by a specific strategy"""
        config = StrategyManager.get_strategy_config(strategy_name)
        if config and "pairs" in config:
            return config["pairs"]
        logger.warning(f"No pairs found for strategy '{strategy_name}', using all pairs")
        from src.arbirich.config import PAIRS

        return PAIRS  # Default to all pairs if not specified

    @staticmethod
    def get_exchange_channels(strategy_name: str) -> Dict[str, str]:
        """
        Get a dictionary of exchange to channel mappings for a strategy.
        This is useful for configuring what data sources each strategy subscribes to.
        """
        exchanges = StrategyManager.get_exchanges_for_strategy(strategy_name)
        return {exchange: "order_book" for exchange in exchanges}
