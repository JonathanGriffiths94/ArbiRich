import logging
import time
import uuid
from typing import Dict, Optional, Set

from src.arbirich.models.models import OrderBookState, TradeOpportunity
from src.arbirich.services.strategies.base_strategy import ArbitrageStrategy

logger = logging.getLogger(__name__)


class HighFrequencyStrategy(ArbitrageStrategy):
    """
    High-frequency trading strategy that detects micro price movements
    and executes rapid trades to capture small spreads with high volume.
    """

    def __init__(self, name: str, config: Dict):
        super().__init__(name, config)

        # Additional HFT-specific parameters
        self.min_volume = config.get("additional_info", {}).get("min_volume", 0.001)
        self.max_slippage = config.get("additional_info", {}).get("max_slippage", 0.0002)
        self.execution_delay = config.get("additional_info", {}).get("execution_delay", 0.05)
        self.price_change_threshold = config.get("additional_info", {}).get("price_change_threshold", 0.0001)

        # Tracking price movement for each asset-exchange pair
        self.price_history = {}  # {asset: {exchange: [prices]}}
        self.history_limit = config.get("additional_info", {}).get("history_size", 10)
        self.last_opportunity_time = {}  # {asset: timestamp}
        self.cooldown_period = config.get("additional_info", {}).get("cooldown_seconds", 1)

        # Track previously detected opportunities to avoid repeats
        self.recent_opportunities: Set[str] = set()
        self.opportunity_ttl = 10  # seconds before forgetting an opportunity signature

        logger.info(
            f"Initialized {self.__class__.__name__} with threshold={self.threshold:.8f}, "
            f"price_change_threshold={self.price_change_threshold:.8f}, "
            f"execution_delay={self.execution_delay:.3f}s"
        )

    def update_price_history(self, asset: str, exchange: str, price: float):
        """Update the price history for this asset-exchange pair."""
        if asset not in self.price_history:
            self.price_history[asset] = {}

        if exchange not in self.price_history[asset]:
            self.price_history[asset][exchange] = []

        history = self.price_history[asset][exchange]
        history.append(price)

        # Keep history at the specified size limit
        if len(history) > self.history_limit:
            history.pop(0)

    def get_price_velocity(self, asset: str, exchange: str) -> Optional[float]:
        """Calculate price velocity (rate of change) for a specific asset-exchange pair."""
        if asset not in self.price_history or exchange not in self.price_history[asset]:
            return None

        history = self.price_history[asset][exchange]
        if len(history) < 2:
            return None

        # Calculate average rate of change over the last few prices
        changes = [history[i] / history[i - 1] - 1 for i in range(1, len(history))]
        return sum(changes) / len(changes)

    def detect_arbitrage(self, asset: str, state: OrderBookState) -> Optional[TradeOpportunity]:
        """
        Detect high-frequency arbitrage opportunities by analyzing price velocities
        and micro price movements across exchanges.

        Parameters:
            asset: The asset symbol (e.g., 'BTC-USDT')
            state: Current order book state across exchanges

        Returns:
            TradeOpportunity object if an opportunity is found, None otherwise
        """
        if asset not in state.symbols or len(state.symbols[asset]) < 2:
            return None

        # Check cooldown period
        current_time = time.time()
        if asset in self.last_opportunity_time:
            elapsed = current_time - self.last_opportunity_time[asset]
            if elapsed < self.cooldown_period:
                # Still in cooldown period for this asset
                return None

        # Track best exchanges based on price trends
        rising_exchanges = []
        falling_exchanges = []

        # Analyze each exchange's orderbook and price trends
        for exchange, order_book in state.symbols[asset].items():
            # Skip if book is invalid or empty
            if not order_book.bids or not order_book.asks:
                continue

            # Get mid price
            mid_price = order_book.get_mid_price()
            if not mid_price:
                continue

            # Update price history
            self.update_price_history(asset, exchange, mid_price)

            # Calculate price velocity
            velocity = self.get_price_velocity(asset, exchange)
            if velocity is None:
                continue

            # Classify exchanges by price direction
            if velocity > self.price_change_threshold:
                rising_exchanges.append((exchange, mid_price, velocity))
            elif velocity < -self.price_change_threshold:
                falling_exchanges.append((exchange, mid_price, velocity))

        # Look for opportunities based on price trends
        if rising_exchanges and falling_exchanges:
            # Sort exchanges by velocity (strongest trend first)
            rising_exchanges.sort(key=lambda x: x[2], reverse=True)
            falling_exchanges.sort(key=lambda x: x[2])

            # Get best match (highest rising and fastest falling)
            buy_exchange, buy_price, _ = falling_exchanges[0]  # Buy from falling exchange
            sell_exchange, sell_price, _ = rising_exchanges[0]  # Sell to rising exchange

            # Calculate potential spread
            spread = (sell_price - buy_price) / buy_price

            # Check spread against threshold
            if spread > self.threshold:
                # Get volume information from the order books
                buy_vol = state.symbols[asset][buy_exchange].get_best_ask().quantity
                sell_vol = state.symbols[asset][sell_exchange].get_best_bid().quantity
                volume = min(buy_vol, sell_vol)

                # Ensure minimum volume
                if volume < self.min_volume:
                    return None

                # Create opportunity signature to avoid duplicates
                signature = f"{asset}:{buy_exchange}:{sell_exchange}:{buy_price:.5f}:{sell_price:.5f}"

                # Check if we've seen this opportunity recently
                if signature in self.recent_opportunities:
                    return None

                # Add to recent opportunities
                self.recent_opportunities.add(signature)
                # Schedule cleanup of old signatures
                # (in a production system, you'd use a timer or periodic cleanup)

                # Update last opportunity time
                self.last_opportunity_time[asset] = current_time

                logger.info(
                    f"HFT opportunity: {asset} - Buy from {buy_exchange} at {buy_price:.8f}, "
                    f"Sell on {sell_exchange} at {sell_price:.8f}, "
                    f"Spread: {spread:.4%}, Volume: {volume:.8f}"
                )

                # Create opportunity
                return TradeOpportunity(
                    id=str(uuid.uuid4()),
                    strategy=self.name,
                    pair=asset,
                    buy_exchange=buy_exchange,
                    sell_exchange=sell_exchange,
                    buy_price=buy_price,
                    sell_price=sell_price,
                    spread=spread,
                    volume=volume,
                    opportunity_timestamp=current_time,
                )

        # Clean up old opportunity signatures
        current_time = time.time()
        self.recent_opportunities = {
            sig for sig in self.recent_opportunities if current_time - float(sig.split(":")[-1]) < self.opportunity_ttl
        }

        return None
