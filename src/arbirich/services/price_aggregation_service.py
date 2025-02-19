import asyncio
import logging
from typing import Dict

logger = logging.getLogger(__name__)


class PriceService:
    def __init__(self):
        """
        Stores only the latest prices from exchanges.
        #TODO:
        # Add timestamp to price data to track when it was last updated.
        # Add a time-to-live (TTL) for prices to prevent stale data.
        """
        self.latest_prices: Dict[str, float] = {}
        self.lock = asyncio.Lock()  # Prevents race conditions

    async def update_price(self, exchange_name: str, price: float):
        """
        Update the most recent price for an exchange.
        """
        async with self.lock:
            self.latest_prices[exchange_name] = price

    def get_latest_price(self, exchange_name: str):
        """
        Fetch the last known price for a specific exchange.
        """
        return self.latest_prices.get(exchange_name, None)

    def get_latest_prices(self) -> Dict[str, float]:
        """
        Returns the latest prices for all exchanges.
        """
        return self.latest_prices
