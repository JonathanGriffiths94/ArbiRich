import abc
from typing import Any, Dict


class ExchangeClient(abc.ABC):
    """Abstract base class for exchange API clients"""

    def __init__(self, exchange_name: str, config: Dict):
        self.exchange_name = exchange_name
        self.config = config

    @abc.abstractmethod
    async def place_market_order(self, symbol: str, side: str, quantity: float) -> Dict[str, Any]:
        """Place a market order on the exchange"""
        pass

    @abc.abstractmethod
    async def place_limit_order(self, symbol: str, side: str, quantity: float, price: float) -> Dict[str, Any]:
        """Place a limit order on the exchange"""
        pass

    @abc.abstractmethod
    async def cancel_order(self, symbol: str, order_id: str) -> Dict[str, Any]:
        """Cancel an existing order"""
        pass

    @abc.abstractmethod
    async def get_order_status(self, symbol: str, order_id: str) -> Dict[str, Any]:
        """Get the status of an existing order"""
        pass
