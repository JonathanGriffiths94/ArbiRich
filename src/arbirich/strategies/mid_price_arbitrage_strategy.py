import logging
import time
import uuid
from typing import Dict, Optional

from src.arbirich.models.models import OrderBookState, TradeOpportunity
from src.arbirich.strategies.base_strategy import ArbitrageStrategy

logger = logging.getLogger(__name__)


class MidPriceArbitrageStrategy(ArbitrageStrategy):
    """
    Mid-price arbitrage strategy that compares the mid-prices between exchanges.
    This can be effective for more liquid markets where the spread is tight.
    """

    def __init__(self, name: str, config: dict):
        super().__init__(name, config)
        # Additional parameters specific to mid-price strategy
        self.min_depth = config.get("min_depth", 3)  # Minimum depth to calculate mid-price

    def calculate_mid_price(self, order_book) -> Optional[float]:
        """Calculate weighted mid-price based on order book depth"""
        if not order_book.bids or not order_book.asks:
            return None

        try:
            # Get top N bids and asks
            top_bids = sorted(order_book.bids, key=lambda x: -float(x.price))[: self.min_depth]
            top_asks = sorted(order_book.asks, key=lambda x: float(x.price))[: self.min_depth]

            if not top_bids or not top_asks:
                return None

            # Calculate weighted prices
            bid_weight_sum = sum(float(b.quantity) for b in top_bids)
            ask_weight_sum = sum(float(a.quantity) for a in top_asks)

            if bid_weight_sum == 0 or ask_weight_sum == 0:
                return None

            weighted_bid = sum(float(b.price) * float(b.quantity) / bid_weight_sum for b in top_bids)
            weighted_ask = sum(float(a.price) * float(a.quantity) / ask_weight_sum for a in top_asks)

            # Return the mid-price
            return (weighted_bid + weighted_ask) / 2

        except Exception as e:
            logger.error(f"Error calculating mid price: {e}")
            return None

    def detect_arbitrage(self, asset: str, state: OrderBookState) -> Optional[TradeOpportunity]:
        """
        Detect arbitrage based on mid-price differences between exchanges.

        Parameters:
            asset: The asset symbol (e.g., 'BTC-USDT')
            state: Current order book state across exchanges

        Returns:
            TradeOpportunity object if an opportunity is found, None otherwise
        """
        logger.debug(f"Checking mid-price arbitrage for {asset} with threshold {self.threshold:.6f}")

        if asset not in state.symbols:
            return None

        if len(state.symbols[asset]) < 2:
            # Need at least 2 exchanges to compare
            return None

        # Calculate mid-prices for each exchange
        mid_prices: Dict[str, float] = {}

        for exchange, order_book in state.symbols[asset].items():
            mid_price = self.calculate_mid_price(order_book)
            if mid_price:
                mid_prices[exchange] = mid_price

        # Need at least 2 valid mid-prices
        if len(mid_prices) < 2:
            return None

        # Find lowest and highest mid-prices
        lowest_exchange = min(mid_prices.items(), key=lambda x: x[1])
        highest_exchange = max(mid_prices.items(), key=lambda x: x[1])

        # Calculate spread based on mid-prices
        low_price = lowest_exchange[1]
        high_price = highest_exchange[1]
        spread = (high_price - low_price) / low_price

        # Check if spread exceeds threshold
        if spread > self.threshold:
            low_exchange = lowest_exchange[0]
            high_exchange = highest_exchange[0]

            # Get approximate quantity from order books
            low_quantity = max(float(ask.quantity) for ask in state.symbols[asset][low_exchange].asks[:3])
            high_quantity = max(float(bid.quantity) for bid in state.symbols[asset][high_exchange].bids[:3])
            volume = min(low_quantity, high_quantity)

            logger.info(
                f"Found mid-price arbitrage for {asset}: "
                f"Buy from {low_exchange} at ~{low_price:.2f}, "
                f"Sell on {high_exchange} at ~{high_price:.2f}, "
                f"Spread: {spread:.4%}"
            )

            # Create opportunity object
            return TradeOpportunity(
                id=str(uuid.uuid4()),
                strategy=self.name,
                pair=asset,
                buy_exchange=low_exchange,
                sell_exchange=high_exchange,
                buy_price=low_price,
                sell_price=high_price,
                spread=spread,
                volume=volume,
                opportunity_timestamp=time.time(),
            )

        return None
