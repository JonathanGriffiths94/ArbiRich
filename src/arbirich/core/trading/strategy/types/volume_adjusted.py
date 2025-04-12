# trading/strategy/types/volume_adjusted.py
import logging
import time
import uuid
from typing import Dict, Optional

from src.arbirich.models.models import OrderBookState, TradeOpportunity

from .arbitrage_type import ArbitrageType

logger = logging.getLogger(__name__)


class VolumeAdjustedArbitrage(ArbitrageType):
    """
    Volume-adjusted arbitrage strategy that accounts for order book depth

    This strategy calculates weighted average prices based on order book depth
    to find opportunities with sufficient liquidity for actual execution.
    """

    def __init__(self, config: Dict):
        super().__init__(config)
        self.threshold = config.get("threshold", 0.0015)  # Default 0.15%
        self.name = config.get("name", "volume_adjusted_arbitrage")
        self.target_volume = config.get("target_volume", 0.1)  # Default target volume in base currency
        self.min_depth_percentage = config.get("min_depth_percentage", 0.7)  # Minimum % of target volume

    def detect_opportunities(self, state: OrderBookState) -> Dict[str, TradeOpportunity]:
        """
        Detect volume-adjusted arbitrage opportunities across all symbols

        Args:
            state: Current order book state across exchanges

        Returns:
            Dictionary of opportunities by symbol
        """
        opportunities = {}

        # Check each symbol
        for asset in state.symbols:
            opportunity = self.detect_arbitrage(asset, state)
            if opportunity:
                opportunities[asset] = opportunity

        return opportunities

    def detect_arbitrage(self, asset: str, state: OrderBookState) -> Optional[TradeOpportunity]:
        """
        Detect volume-adjusted arbitrage opportunities accounting for liquidity depth.

        Parameters:
            asset: The asset symbol (e.g., 'BTC-USDT')
            state: Current order book state across exchanges

        Returns:
            TradeOpportunity object if an opportunity is found, None otherwise
        """
        logger.debug(f"Checking volume-adjusted arbitrage for {asset} with threshold {self.threshold:.6f}")

        if asset not in state.symbols:
            return None

        if len(state.symbols[asset]) < 2:
            # Need at least 2 exchanges to compare
            return None

        # Calculate volume-weighted prices for each exchange
        exchange_prices = {}

        for exchange, order_book in state.symbols[asset].items():
            # Skip if book is empty or invalid
            if not order_book.bids or not order_book.asks:
                continue

            # Calculate weighted average prices for target volume
            try:
                weighted_bid = self._calculate_weighted_price(order_book.bids, self.target_volume, is_bid=True)
                weighted_ask = self._calculate_weighted_price(order_book.asks, self.target_volume, is_bid=False)

                if weighted_bid and weighted_ask:
                    exchange_prices[exchange] = {
                        "weighted_bid": weighted_bid["price"],
                        "weighted_ask": weighted_ask["price"],
                        "bid_volume": weighted_bid["volume"],
                        "ask_volume": weighted_ask["volume"],
                    }
            except Exception as e:
                logger.error(f"Error calculating weighted prices for {exchange}: {e}")

        # Find arbitrage opportunities
        opportunities = []
        exchanges = list(exchange_prices.keys())

        for i in range(len(exchanges)):
            for j in range(i + 1, len(exchanges)):
                ex1 = exchanges[i]
                ex2 = exchanges[j]

                # Try both directions
                # Direction 1: Buy on ex1, sell on ex2
                buy_price = exchange_prices[ex1]["weighted_ask"]
                sell_price = exchange_prices[ex2]["weighted_bid"]
                buy_volume = exchange_prices[ex1]["ask_volume"]
                sell_volume = exchange_prices[ex2]["bid_volume"]

                spread = (sell_price - buy_price) / buy_price

                if spread > self.threshold:
                    # Check if we have sufficient volume
                    available_volume = min(buy_volume, sell_volume)
                    min_required = self.target_volume * self.min_depth_percentage

                    if available_volume >= min_required:
                        opportunities.append(
                            {
                                "buy_exchange": ex1,
                                "sell_exchange": ex2,
                                "buy_price": buy_price,
                                "sell_price": sell_price,
                                "spread": spread,
                                "volume": available_volume,
                            }
                        )

                # Direction 2: Buy on ex2, sell on ex1
                buy_price = exchange_prices[ex2]["weighted_ask"]
                sell_price = exchange_prices[ex1]["weighted_bid"]
                buy_volume = exchange_prices[ex2]["ask_volume"]
                sell_volume = exchange_prices[ex1]["bid_volume"]

                spread = (sell_price - buy_price) / buy_price

                if spread > self.threshold:
                    # Check if we have sufficient volume
                    available_volume = min(buy_volume, sell_volume)
                    min_required = self.target_volume * self.min_depth_percentage

                    if available_volume >= min_required:
                        opportunities.append(
                            {
                                "buy_exchange": ex2,
                                "sell_exchange": ex1,
                                "buy_price": buy_price,
                                "sell_price": sell_price,
                                "spread": spread,
                                "volume": available_volume,
                            }
                        )

        # Select best opportunity (highest spread)
        if opportunities:
            best_opp = max(opportunities, key=lambda x: x["spread"])

            logger.info(
                f"Found volume-adjusted arbitrage opportunity for {asset}: "
                f"Buy from {best_opp['buy_exchange']} at {best_opp['buy_price']}, "
                f"Sell on {best_opp['sell_exchange']} at {best_opp['sell_price']}, "
                f"Spread: {best_opp['spread']:.4%}, Volume: {best_opp['volume']}"
            )

            # Create opportunity object
            return TradeOpportunity(
                id=str(uuid.uuid4()),
                strategy=self.name,
                pair=asset,
                buy_exchange=best_opp["buy_exchange"],
                sell_exchange=best_opp["sell_exchange"],
                buy_price=best_opp["buy_price"],
                sell_price=best_opp["sell_price"],
                spread=best_opp["spread"],
                volume=best_opp["volume"],
                opportunity_timestamp=time.time(),
            )

        return None

    def _calculate_weighted_price(self, orders, target_volume, is_bid=True):
        """
        Calculate volume-weighted average price for a given target volume

        Args:
            orders: Dictionary of price -> quantity for orders
            target_volume: Target volume to execute
            is_bid: True if calculating for bids, False for asks

        Returns:
            Dictionary with weighted average price and available volume
        """
        total_volume = 0
        weighted_sum = 0

        # Sort prices (descending for bids, ascending for asks)
        sorted_prices = sorted(orders.keys(), reverse=is_bid)

        for price in sorted_prices:
            qty = orders[price]
            available = min(qty, target_volume - total_volume)

            if available <= 0:
                continue

            weighted_sum += price * available
            total_volume += available

            if total_volume >= target_volume:
                break

        if total_volume <= 0:
            return None

        weighted_price = weighted_sum / total_volume

        return {"price": weighted_price, "volume": total_volume}

    def validate_opportunity(self, opportunity: TradeOpportunity) -> bool:
        """Validate if the opportunity meets criteria"""
        if not opportunity:
            return False

        # Check if spread is above threshold
        if opportunity.spread <= self.threshold:
            return False

        # Check if volume is sufficient
        min_required = self.target_volume * self.min_depth_percentage
        if opportunity.volume < min_required:
            return False

        # Check if opportunity is fresh (within last 2 seconds)
        if time.time() - opportunity.opportunity_timestamp > 2:
            return False

        return True

    def calculate_spread(self, opportunity: TradeOpportunity) -> float:
        """Calculate the spread (already calculated in the opportunity object)"""
        return opportunity.spread
