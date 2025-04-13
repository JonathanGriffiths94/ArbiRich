import abc
import logging
from typing import Dict, Optional

from src.arbirich.core.config.validator import ConfigValidator
from src.arbirich.models.models import OrderBookState, TradeOpportunity

logger = logging.getLogger(__name__)


class ArbitrageStrategy(abc.ABC):
    """
    Abstract base class for all arbitrage strategies.
    Each strategy implements its own arbitrage detection logic.
    """

    def __init__(self, name: str, config: Dict):
        """
        Initialize the strategy.

        Parameters:
            name: The strategy name
            config: Configuration parameters for the strategy
        """
        self.name = name

        # Validate the config
        errors = ConfigValidator.validate_strategy_config(config.get("type", "basic"), config)
        if errors:
            logger.error(f"Invalid configuration for strategy {name}: {errors}")
            raise ValueError(f"Invalid strategy configuration: {errors}")

        # Store both raw config and validated config
        self.raw_config = config

        # Get validated strategy config
        strategy_type = config.get("type", "basic")
        self.config = ConfigValidator.get_validated_strategy_config(strategy_type, config)

        # Set common attributes - access either through Pydantic model or fallback to dict
        if hasattr(self.config, "threshold"):
            self.threshold = self.config.threshold
        else:
            self.threshold = config.get("threshold", 0.001)  # Default 0.1%

        if hasattr(self.config, "pairs"):
            self.pairs = self.config.pairs
        else:
            self.pairs = config.get("pairs", [])

        if hasattr(self.config, "exchanges"):
            self.exchanges = self.config.exchanges
        else:
            self.exchanges = config.get("exchanges", [])

        logger.info(f"Initialized strategy '{name}' with threshold {self.threshold:.4%}")

    @abc.abstractmethod
    def detect_arbitrage(self, asset: str, state: OrderBookState) -> Optional[TradeOpportunity]:
        """
        Detect arbitrage opportunities for a given asset across exchanges.

        Parameters:
            asset: The asset symbol (e.g., 'BTC-USDT')
            state: Current order book state across exchanges

        Returns:
            TradeOpportunity object if an opportunity is found, None otherwise
        """
        pass

    def get_threshold(self) -> float:
        """Get the threshold value for this strategy"""
        return self.threshold

    def get_pairs(self) -> list:
        """Get the trading pairs for this strategy"""
        return self.pairs

    def get_exchanges(self) -> list:
        """Get the exchanges for this strategy"""
        return self.exchanges
