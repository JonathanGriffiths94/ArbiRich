import logging
import time
from typing import Dict

from src.arbirich.services.price_service import PriceService

logger = logging.getLogger(__name__)


class ArbitrageService:
    def __init__(self, price_service: PriceService, threshold: float = 0.5):
        """
        Initializes the ArbitrageService.

        :param price_service: Instance of PriceService to fetch latest prices.
        :param threshold: The minimum price difference (%) to log an arbitrage opportunity.
        """
        self.price_service = price_service
        self.threshold = threshold
        self.logged_opportunities = {}  # Stores detected opportunities with timestamps

    def detect_opportunity(self):
        """
        Checks for arbitrage opportunities by comparing prices across 3 exchanges.
        """
        prices: Dict[str, float] = self.price_service.get_latest_prices()
        exchanges = list(prices.keys())

        if len(exchanges) < 2:
            return  # No arbitrage possible with <2 exchanges

        current_time = time.time()
        expired_keys = [
            key
            for key, timestamp in self.logged_opportunities.items()
            if current_time - timestamp > 600
        ]  # Remove after 10 minutes

        # Remove expired keys
        for key in expired_keys:
            del self.logged_opportunities[key]

        # Compare each exchange with every other exchange (for 3 exchanges)
        for i in range(len(exchanges)):
            for j in range(i + 1, len(exchanges)):
                exchange1, exchange2 = exchanges[i], exchanges[j]
                price1, price2 = prices[exchange1], prices[exchange2]

                # Ensure both prices are valid
                if price1 is None or price2 is None:
                    continue

                # Calculate percentage difference
                spread = abs(price1 - price2) / min(price1, price2) * 100

                if spread >= self.threshold:
                    opportunity_key = (exchange1, exchange2)
                    reverse_key = (exchange2, exchange1)

                    # Ensure we only log a new opportunity once
                    if (
                        opportunity_key not in self.logged_opportunities
                        and reverse_key not in self.logged_opportunities
                    ):
                        self.logged_opportunities[opportunity_key] = current_time
                        self.logged_opportunities[reverse_key] = current_time

                        if price1 < price2:
                            logger.info(
                                f"🔥 Arbitrage Opportunity! Buy on {exchange1} (${price1:.2f}) "
                                f"and sell on {exchange2} (${price2:.2f}) | Spread: {spread:.2f}%"
                            )
                        else:
                            logger.info(
                                f"🔥 Arbitrage Opportunity! Buy on {exchange2} (${price2:.2f}) "
                                f"and sell on {exchange1} (${price1:.2f}) | Spread: {spread:.2f}%"
                            )
