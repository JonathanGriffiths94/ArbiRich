import logging
from typing import Any, Dict, Optional

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
    def get_all_strategy_names() -> list[str]:
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
