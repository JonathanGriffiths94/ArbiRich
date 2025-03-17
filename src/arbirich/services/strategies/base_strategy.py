import abc
import logging
from typing import Optional

from src.arbirich.models.models import OrderBookState, TradeOpportunity

logger = logging.getLogger(__name__)


class ArbitrageStrategy(abc.ABC):
    """
    Abstract base class for all arbitrage strategies.
    Each strategy implements its own arbitrage detection logic.
    """

    def __init__(self, name: str, config: dict):
        """
        Initialize the strategy.

        Parameters:
            name: The strategy name
            config: Configuration parameters for the strategy
        """
        self.name = name
        self.config = config
        self.threshold = config.get("threshold", 0.001)  # Default 0.1%
        self.pairs = config.get("pairs", [])
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
