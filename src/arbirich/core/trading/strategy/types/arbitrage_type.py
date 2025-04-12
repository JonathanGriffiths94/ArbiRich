from abc import ABC, abstractmethod
from typing import Any, Dict, List


class ArbitrageType(ABC):
    """Base class for different arbitrage detection and analysis strategies"""

    def __init__(self, config: Dict):
        self.config = config

    @abstractmethod
    def detect_opportunities(self, order_books: Dict) -> List[Any]:
        """
        Detect arbitrage opportunities from order book data

        Args:
            order_books: Dictionary of order books by exchange and trading pair

        Returns:
            List of opportunity objects (format depends on implementation)
        """
        pass

    @abstractmethod
    def validate_opportunity(self, opportunity: Any) -> bool:
        """
        Validate if an opportunity is actionable

        Args:
            opportunity: The opportunity object to validate

        Returns:
            True if the opportunity is valid, False otherwise
        """
        pass

    @abstractmethod
    def calculate_spread(self, opportunity: Any) -> float:
        """
        Calculate the expected profit or spread for an opportunity

        Args:
            opportunity: The opportunity object

        Returns:
            The spread as a decimal (e.g., 0.01 for 1%)
        """
        pass
