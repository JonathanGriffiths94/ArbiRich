import asyncio
import logging
import time
from typing import Dict

logger = logging.getLogger(__name__)


class ArbitrageService:
    def __init__(
        self,
        price_service,
        threshold: float = 0.5,
        alert_producer=None,
        alert_topic="arbitrage_alerts",
    ):
        """
        Initializes the ArbitrageService.

        :param price_service: Instance of PriceService to fetch latest prices.
        :param threshold: The minimum percentage price difference to log an arbitrage opportunity.
        :param alert_producer: Optional Kafka producer to publish arbitrage alerts.
        :param alert_topic: Kafka topic to which alerts will be published.
        """
        self.price_service = price_service
        self.threshold = threshold
        self.logged_opportunities = {}  # Stores detected opportunities with timestamps
        self.alert_producer = alert_producer
        self.alert_topic = alert_topic

    def detect_opportunity(self):
        """
        Checks for arbitrage opportunities by comparing prices across exchanges.
        If an opportunity is found, it logs the event and, if an alert producer is provided,
        publishes an alert to the Kafka topic.
        """
        prices: Dict[str, float] = self.price_service.get_latest_prices()
        exchanges = list(prices.keys())
        if len(exchanges) < 2:
            return  # No arbitrage possible with fewer than 2 exchanges

        current_time = time.time()
        # Remove stale opportunities (older than 10 minutes)
        expired_keys = [
            key
            for key, timestamp in self.logged_opportunities.items()
            if current_time - timestamp > 600
        ]
        for key in expired_keys:
            del self.logged_opportunities[key]

        # Compare each exchange with every other exchange
        for i in range(len(exchanges)):
            for j in range(i + 1, len(exchanges)):
                exchange1, exchange2 = exchanges[i], exchanges[j]
                price1, price2 = prices[exchange1], prices[exchange2]
                if price1 is None or price2 is None:
                    continue

                spread = abs(price1 - price2) / min(price1, price2) * 100
                if spread >= self.threshold:
                    opportunity_key = (exchange1, exchange2)
                    reverse_key = (exchange2, exchange1)
                    if (
                        opportunity_key not in self.logged_opportunities
                        and reverse_key not in self.logged_opportunities
                    ):
                        self.logged_opportunities[opportunity_key] = current_time
                        self.logged_opportunities[reverse_key] = current_time

                        if price1 < price2:
                            message = (
                                f"🔥 Arbitrage Opportunity! Buy on {exchange1} (${price1:.2f}) "
                                f"and sell on {exchange2} (${price2:.2f}) | Spread: {spread:.2f}%"
                            )
                            buy_exchange = exchange1
                            sell_exchange = exchange2
                            buy_price = price1
                            sell_price = price2
                        else:
                            message = (
                                f"🔥 Arbitrage Opportunity! Buy on {exchange2} (${price2:.2f}) "
                                f"and sell on {exchange1} (${price1:.2f}) | Spread: {spread:.2f}%"
                            )
                            buy_exchange = exchange2
                            sell_exchange = exchange1
                            buy_price = price2
                            sell_price = price1

                        logger.info(message)

                        # Publish alert to Kafka if an alert producer is available
                        if self.alert_producer:
                            alert_data = {
                                "buy_exchange": buy_exchange,
                                "sell_exchange": sell_exchange,
                                "buy_price": buy_price,
                                "sell_price": sell_price,
                                "spread": spread,
                                "message": message,
                            }
                            # Schedule asynchronous production of the alert message.
                            asyncio.create_task(
                                self.alert_producer.send_and_wait(
                                    self.alert_topic, alert_data
                                )
                            )
