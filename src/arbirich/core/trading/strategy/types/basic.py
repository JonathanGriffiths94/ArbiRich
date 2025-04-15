# trading/strategy/types/basic.py
import logging
import time
import uuid
from typing import Dict, Optional, Union

from src.arbirich.models.enums import StrategyType
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
                self.name = config.get("name", StrategyType.BASIC.value)
                self.id = str(self.name)
                super().__init__(config)
            else:
                self.name = strategy_id or StrategyType.BASIC.value
                self.id = str(self.name)
                super().__init__(config or {})

        self.threshold = self.config.get("threshold", 0.001)  # Default 0.1%

        logger.info(f"Initialized BasicArbitrage strategy: {self.name} with threshold {self.threshold}")

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
                logger.info(f"Detected opportunity for {asset}")
            else:
                logger.debug(f"No opportunity detected for {asset}")

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
            logger.debug(f"Asset {asset} not found in state")
            return None

        exchanges = list(state.symbols[asset].keys())
        if len(exchanges) < 2:
            # Need at least 2 exchanges to compare
            logger.debug(f"Not enough exchanges for {asset} (need at least 2, found {len(exchanges)})")
            return None

        # Track best bid and ask per exchange
        exchange_prices = {}

        # Log the exchanges we're checking
        logger.info(f"💹 ARBITRAGE CHECK: Comparing {len(exchanges)} exchanges for {asset}: {exchanges}")

        # Iterate through all exchanges for this symbol and collect the best prices
        for exchange, order_book in state.symbols[asset].items():
            # Skip if book is empty or invalid
            if (
                not hasattr(order_book, "bids")
                or not hasattr(order_book, "asks")
                or not order_book.bids
                or not order_book.asks
            ):
                logger.debug(f"❌ Empty or invalid order book for {exchange}:{asset}")
                continue

            exchange_prices[exchange] = {"bid": None, "ask": None}

            # Find highest bid using the get_best_bid helper method
            try:
                highest_bid = order_book.get_best_bid()
                if highest_bid:
                    logger.debug(f"📊 {exchange} best bid: {highest_bid.price:.8f} (qty: {highest_bid.quantity:.8f})")
                    exchange_prices[exchange]["bid"] = {"price": highest_bid.price, "quantity": highest_bid.quantity}
                else:
                    logger.debug(f"❌ No valid bid for {exchange}:{asset}")
            except Exception as e:
                logger.error(f"Error processing bids for {exchange}: {e}")

            # Find lowest ask using the get_best_ask helper method
            try:
                lowest_ask = order_book.get_best_ask()
                if lowest_ask:
                    logger.debug(f"📊 {exchange} best ask: {lowest_ask.price:.8f} (qty: {lowest_ask.quantity:.8f})")
                    exchange_prices[exchange]["ask"] = {"price": lowest_ask.price, "quantity": lowest_ask.quantity}
                else:
                    logger.debug(f"❌ No valid ask for {exchange}:{asset}")
            except Exception as e:
                logger.error(f"Error processing asks for {exchange}: {e}")

        # Find the best opportunity across different exchanges
        best_opportunity = None
        best_spread = self.threshold  # Start with the minimum acceptable spread

        # Compare each exchange's bid with other exchanges' asks
        for bid_exchange, bid_data in exchange_prices.items():
            if not bid_data["bid"]:
                continue  # Skip if no valid bid

            for ask_exchange, ask_data in exchange_prices.items():
                if ask_exchange == bid_exchange or not ask_data["ask"]:
                    continue  # Skip same exchange or invalid ask

                # Calculate potential spread
                spread = (bid_data["bid"]["price"] - ask_data["ask"]["price"]) / ask_data["ask"]["price"]

                logger.debug(
                    f"Cross-exchange check: {ask_exchange} ask {ask_data['ask']['price']:.8f} → {bid_exchange} bid {bid_data['bid']['price']:.8f}, spread: {spread:.6f}"
                )

                # Check if this is the best spread so far
                if spread > best_spread:
                    best_spread = spread
                    volume = min(bid_data["bid"]["quantity"], ask_data["ask"]["quantity"])
                    best_opportunity = {
                        "buy_exchange": ask_exchange,
                        "sell_exchange": bid_exchange,
                        "buy_price": ask_data["ask"]["price"],
                        "sell_price": bid_data["bid"]["price"],
                        "spread": spread,
                        "volume": volume,
                    }
                    logger.debug(
                        f"📈 New best opportunity: buy on {ask_exchange} at {ask_data['ask']['price']:.8f}, sell on {bid_exchange} at {bid_data['bid']['price']:.8f}, spread: {spread:.6f}"
                    )

        # If a valid opportunity was found, create and return the TradeOpportunity
        if best_opportunity:
            logger.info(
                f"💰 FOUND ARBITRAGE OPPORTUNITY for {asset}:\n"
                f"  • Buy from {best_opportunity['buy_exchange']} at {best_opportunity['buy_price']:.8f}\n"
                f"  • Sell on {best_opportunity['sell_exchange']} at {best_opportunity['sell_price']:.8f}\n"
                f"  • Spread: {best_opportunity['spread']:.4%}\n"
                f"  • Profit: {(best_opportunity['spread'] * best_opportunity['volume'] * best_opportunity['buy_price']):.8f}"
            )

            return TradeOpportunity(
                id=str(uuid.uuid4()),
                strategy=self.name,
                pair=asset,
                buy_exchange=best_opportunity["buy_exchange"],
                sell_exchange=best_opportunity["sell_exchange"],
                buy_price=best_opportunity["buy_price"],
                sell_price=best_opportunity["sell_price"],
                spread=best_opportunity["spread"],
                volume=best_opportunity["volume"],
                opportunity_timestamp=time.time(),
            )
        else:
            logger.debug(f"❌ No profitable arbitrage opportunity found for {asset}")
            return None

    def validate_opportunity(self, opportunity: Union[Dict, TradeOpportunity]) -> bool:
        """Validate if the opportunity meets criteria"""
        if not opportunity:
            return False

        # Perform validation checks
        if opportunity.spread <= self.threshold:
            return False

        if opportunity.volume <= 0:
            return False

        if time.time() - opportunity.opportunity_timestamp > 5:
            return False

        return True

    def calculate_spread(self, opportunity: Union[Dict, TradeOpportunity]) -> float:
        """Calculate the spread (already calculated in the opportunity object)"""
        return opportunity.spread
