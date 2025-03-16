import logging
import time
import uuid
from typing import Optional

from src.arbirich.models.models import OrderBookState, TradeOpportunity
from src.arbirich.strategies.base_strategy import ArbitrageStrategy

logger = logging.getLogger(__name__)


class BasicArbitrageStrategy(ArbitrageStrategy):
    """
    Basic arbitrage strategy that looks for simple price differences between exchanges.
    """

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

            # Find highest bid
            try:
                highest_bid = max(order_book.bids, key=lambda x: float(x.price))
                if highest_bid.price > best_bid["price"]:
                    best_bid["price"] = highest_bid.price
                    best_bid["exchange"] = exchange
                    best_bid["quantity"] = highest_bid.quantity
            except Exception as e:
                logger.error(f"Error processing bids for {exchange}: {e}")

            # Find lowest ask
            try:
                lowest_ask = min(order_book.asks, key=lambda x: float(x.price))
                if lowest_ask.price < best_ask["price"]:
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
