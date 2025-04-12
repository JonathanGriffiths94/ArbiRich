# trading/strategy/types/basic.py
import logging
import time
import uuid
from typing import Dict, Optional

from src.arbirich.models.models import OrderBookState, TradeOpportunity

from .arbitrage_type import ArbitrageType

logger = logging.getLogger(__name__)


class BasicArbitrage(ArbitrageType):
    """
    Basic arbitrage strategy that looks for simple price differences between exchanges.
    """

    def __init__(self, strategy_id=None, strategy_name=None, config=None):
        """
        Initialize the basic arbitrage strategy.

        Args:
            strategy_id: Unique identifier for this strategy instance
            strategy_name: Human-readable name of the strategy
            config: Configuration dictionary
        """
        # Handle both constructor styles for backward compatibility
        if strategy_name is not None and config is not None:
            # New style constructor with separate parameters
            self.id = strategy_id
            self.name = strategy_name
            super().__init__(config)
        else:
            # Old style constructor where first param is name or config
            if isinstance(strategy_id, dict):
                config = strategy_id
                self.name = config.get("name", "basic_arbitrage")
                self.id = str(self.name)
                super().__init__(config)
            else:
                self.name = strategy_id or "basic_arbitrage"
                self.id = str(self.name)
                super().__init__(config or {})

        self.threshold = self.config.get("threshold", 0.001)  # Default 0.1%

    def detect_opportunities(self, state: OrderBookState) -> Dict[str, TradeOpportunity]:
        """
        Detect basic arbitrage opportunities across all symbols

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
        Detect basic arbitrage opportunities by comparing best bid/ask across exchanges.

        Parameters:
            asset: The asset symbol (e.g., 'BTC-USDT')
            state: Current order book state across exchanges

        Returns:
            TradeOpportunity object if an opportunity is found, None otherwise
        """
        logger.debug(f"Checking basic arbitrage for {asset} with threshold {self.threshold:.6f}")

        if asset not in state.symbols:
            return None

        if len(state.symbols[asset]) < 2:
            # Need at least 2 exchanges to compare
            return None

        # Find best bid and ask across all exchanges
        best_bid = {"price": -1.0, "exchange": None, "quantity": 0.0}
        best_ask = {"price": float("inf"), "exchange": None, "quantity": 0.0}

        # Iterate through all exchanges for this symbol
        for exchange, order_book in state.symbols[asset].items():
            # Skip if book is empty or invalid
            if not order_book.bids or not order_book.asks:
                continue

            # Find highest bid using the get_best_bid helper method
            try:
                highest_bid = order_book.get_best_bid()
                if highest_bid and highest_bid.price > best_bid["price"]:
                    best_bid["price"] = highest_bid.price
                    best_bid["exchange"] = exchange
                    best_bid["quantity"] = highest_bid.quantity
            except Exception as e:
                logger.error(f"Error processing bids for {exchange}: {e}")

            # Find lowest ask using the get_best_ask helper method
            try:
                lowest_ask = order_book.get_best_ask()
                if lowest_ask and lowest_ask.price < best_ask["price"]:
                    best_ask["price"] = lowest_ask.price
                    best_ask["exchange"] = exchange
                    best_ask["quantity"] = lowest_ask.quantity
            except Exception as e:
                logger.error(f"Error processing asks for {exchange}: {e}")

        # Check if we have valid bid and ask from different exchanges
        if (
            best_bid["exchange"] is not None
            and best_ask["exchange"] is not None
            and best_bid["exchange"] != best_ask["exchange"]
        ):
            # Calculate spread
            spread = (best_bid["price"] - best_ask["price"]) / best_ask["price"]

            # Check if spread exceeds threshold
            if spread > self.threshold:
                logger.info(
                    f"Found basic arbitrage opportunity for {asset}: "
                    f"Buy from {best_ask['exchange']} at {best_ask['price']}, "
                    f"Sell on {best_bid['exchange']} at {best_bid['price']}, "
                    f"Spread: {spread:.4%}"
                )

                # Calculate max tradable volume
                volume = min(best_bid["quantity"], best_ask["quantity"])
                if volume <= 0:
                    logger.warning(f"Zero or negative volume for opportunity: {volume}")
                    return None

                # Create opportunity object
                return TradeOpportunity(
                    id=str(uuid.uuid4()),
                    strategy=self.name,
                    pair=asset,
                    buy_exchange=best_ask["exchange"],
                    sell_exchange=best_bid["exchange"],
                    buy_price=best_ask["price"],
                    sell_price=best_bid["price"],
                    spread=spread,
                    volume=volume,
                    opportunity_timestamp=time.time(),
                )

        return None

    def validate_opportunity(self, opportunity: TradeOpportunity) -> bool:
        """Validate if the opportunity meets criteria"""
        if not opportunity:
            return False

        # Check if spread is above threshold
        if opportunity.spread <= self.threshold:
            return False

        # Check if volume is reasonable
        if opportunity.volume <= 0:
            return False

        # Check if opportunity is fresh (within last 5 seconds)
        if time.time() - opportunity.opportunity_timestamp > 5:
            return False

        return True

    def calculate_spread(self, opportunity: TradeOpportunity) -> float:
        """Calculate the spread (already calculated in the opportunity object)"""
        return opportunity.spread
