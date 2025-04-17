"""
Base class for exchange connectors.
"""

import abc
import logging
from typing import Any, Callable, Dict

from src.arbirich.models import OrderBookUpdate, TradeRequest


class BaseExchangeConnector(abc.ABC):
    """
    Abstract base class for exchange connectors.

    This defines the interface that all exchange connectors must implement.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the exchange connector with configuration.

        Args:
            config: Exchange-specific configuration
        """
        self.api_key = config.get("api_key")
        self.api_secret = config.get("api_secret")
        self.logger = logging.getLogger("arbirich.connectors.base")

    @abc.abstractmethod
    async def initialize(self) -> bool:
        """
        Initialize the exchange connector.

        Returns:
            bool: True if initialization was successful
        """
        pass

    @abc.abstractmethod
    async def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Get current ticker information for a symbol.

        Args:
            symbol: Trading pair symbol

        Returns:
            Dict containing ticker information
        """
        pass

    @abc.abstractmethod
    async def get_order_book(self, symbol: str, depth: int = 20) -> OrderBookUpdate:
        """
        Get order book for a symbol.

        Args:
            symbol: Trading pair symbol
            depth: Depth of the order book to retrieve

        Returns:
            OrderBookUpdate containing order book data with bids and asks
        """
        pass

    @abc.abstractmethod
    async def get_account_balance(self) -> Dict[str, float]:
        """
        Get account balances for all currencies.

        Returns:
            Dict mapping currency symbols to their available balances
        """
        pass

    @abc.abstractmethod
    async def create_order(self, trade_request: TradeRequest) -> Dict[str, Any]:
        """
        Create an order on the exchange.

        Args:
            trade_request: Trade request model containing order parameters

        Returns:
            Dict containing order information
        """
        pass

    @abc.abstractmethod
    async def cancel_order(self, symbol: str, order_id: str) -> Dict[str, Any]:
        """
        Cancel an existing order.

        Args:
            symbol: Trading pair symbol
            order_id: ID of the order to cancel

        Returns:
            Dict containing cancellation information
        """
        pass

    @abc.abstractmethod
    async def get_order_status(self, symbol: str, order_id: str) -> Dict[str, Any]:
        """
        Get status of an order.

        Args:
            symbol: Trading pair symbol
            order_id: ID of the order to check

        Returns:
            Dict containing order status
        """
        pass

    @abc.abstractmethod
    async def subscribe_to_ticker(self, symbol: str, callback: Callable) -> Any:
        """
        Subscribe to ticker updates for a symbol.

        Args:
            symbol: Trading pair symbol
            callback: Callback function to call with ticker updates

        Returns:
            Subscription identifier
        """
        pass

    @abc.abstractmethod
    async def subscribe_to_order_book(self, symbol: str, callback: Callable[[OrderBookUpdate], Any]) -> Any:
        """
        Subscribe to order book updates for a symbol.

        Args:
            symbol: Trading pair symbol
            callback: Callback function to call with order book updates

        Returns:
            Subscription identifier
        """
        pass

    @abc.abstractmethod
    async def close(self) -> None:
        """
        Close connections and clean up resources.
        """
        pass
