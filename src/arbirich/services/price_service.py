import logging
from typing import Dict

logger = logging.getLogger(__name__)


class PriceService:
    def __init__(self):
        """
        Stores only the latest prices from exchanges.
        """
        self.latest_prices: Dict[str, float] = {}

    async def update_price(self, exchange_name: str, price: float):
        """
        Update the most recent price for an exchange.
        """
        self.latest_prices[exchange_name] = price
        logger.info(f"Updated {exchange_name} price: {price}")

    def get_latest_price(self, exchange_name: str):
        """
        Fetch the last known price for a specific exchange.
        """
        return self.latest_prices.get(exchange_name, None)
