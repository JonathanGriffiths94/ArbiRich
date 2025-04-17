import logging
import time
import uuid
from typing import Dict, Optional

from src.arbirich.models import OrderBookState, TradeOpportunity

from .arbitrage_type import ArbitrageType

logger = logging.getLogger(__name__)


class MidPriceArbitrage(ArbitrageType):
    """
    Mid-price arbitrage strategy that uses mid-price for more stable signals

    This strategy compares the mid-price (average of best bid and ask) across
    exchanges to find arbitrage opportunities with less noise from spread variations.
    """

    def __init__(self, strategy_id=None, strategy_name=None, config=None):
        """
        Initialize the mid-price arbitrage strategy.

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
                self.name = config.get("name", "mid_price_arbitrage")
                self.id = str(self.name)
                super().__init__(config)
            else:
                self.name = strategy_id or "mid_price_arbitrage"
                self.id = str(self.name)
                super().__init__(config or {})

        self.threshold = self.config.get("threshold", 0.002)  # Default 0.2%
        self.min_depth = self.config.get("min_depth", 3)  # Minimum order book depth

    def detect_opportunities(self, state: OrderBookState) -> Dict[str, TradeOpportunity]:
        """
        Detect mid-price arbitrage opportunities across all symbols

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
        Detect mid-price arbitrage opportunities by comparing mid-prices across exchanges.

        Parameters:
            asset: The asset symbol (e.g., 'BTC-USDT')
            state: Current order book state across exchanges

        Returns:
            TradeOpportunity object if an opportunity is found, None otherwise
        """
        logger.info(f"🔍 Checking mid-price arbitrage for {asset} with threshold {self.threshold:.6f}")

        if asset not in state.symbols:
            logger.info(f"❌ Asset {asset} not found in state")
            return None

        # Get list of exchanges for this asset
        exchanges = list(state.symbols[asset].keys())
        if len(exchanges) < 2:
            # Need at least 2 exchanges to compare
            logger.info(f"❌ Not enough exchanges for {asset} (need at least 2, found {len(exchanges)})")
            return None

        # Log the exchanges we have data for
        logger.info(f"💹 ARBITRAGE CHECK: Analyzing {len(exchanges)} exchanges for {asset}: {exchanges}")

        # Calculate mid-prices for each exchange
        exchange_mid_prices = {}

        for exchange, order_book in state.symbols[asset].items():
            # Skip if book is empty or invalid
            if (
                not hasattr(order_book, "bids")
                or not hasattr(order_book, "asks")
                or not order_book.bids
                or not order_book.asks
            ):
                logger.info(f"❌ Empty or invalid order book for {exchange}:{asset}")
                continue

            # Check if order book has sufficient depth
            if len(order_book.bids) < self.min_depth or len(order_book.asks) < self.min_depth:
                logger.info(
                    f"❌ Insufficient depth for {exchange}:{asset} - bids: {len(order_book.bids)}, asks: {len(order_book.asks)}, min required: {self.min_depth}"
                )
                continue

            # Calculate mid price using top N levels
            try:
                best_bid = order_book.get_best_bid()
                best_ask = order_book.get_best_ask()

                if not best_bid or not best_ask:
                    logger.info(f"❌ Missing best bid or ask for {exchange}:{asset}")
                    continue

                mid_price = (best_bid.price + best_ask.price) / 2
                exchange_mid_prices[exchange] = {
                    "mid_price": mid_price,
                    "bid": best_bid.price,
                    "ask": best_ask.price,
                    "bid_qty": best_bid.quantity,
                    "ask_qty": best_ask.quantity,
                }
                logger.info(
                    f"📊 {exchange} mid-price: {mid_price:.8f} (bid: {best_bid.price:.8f}, ask: {best_ask.price:.8f})"
                )
            except Exception as e:
                logger.error(f"Error calculating mid-price for {exchange}: {e}")

        # Log all the mid-prices we calculated
        if exchange_mid_prices:
            logger.info(f"📊 Calculated mid-prices for {asset}:")
            for exch, data in exchange_mid_prices.items():
                logger.info(f"  • {exch}: {data['mid_price']:.8f}")
        else:
            logger.info(f"❌ No valid mid-prices calculated for {asset}")
            return None

        # Compare mid-prices across exchanges
        opportunities = []
        exchanges = list(exchange_mid_prices.keys())

        logger.info(f"🔄 Comparing {len(exchanges)} exchanges with valid mid-prices for {asset}")

        for i in range(len(exchanges)):
            for j in range(i + 1, len(exchanges)):
                ex1 = exchanges[i]
                ex2 = exchanges[j]

                # Log each comparison we're making
                logger.info(f"🔄 Comparing {ex1} vs {ex2} for {asset}")

                # Calculate relative price difference
                price1 = exchange_mid_prices[ex1]["mid_price"]
                price2 = exchange_mid_prices[ex2]["mid_price"]

                # Check both directions
                if price1 > price2:
                    # ex1 has higher price (sell on ex1, buy on ex2)
                    price_diff = (price1 - price2) / price2
                    logger.info(f"📈 {ex1} price higher than {ex2} by {price_diff:.6f} ({price1:.8f} vs {price2:.8f})")

                    if price_diff > self.threshold:
                        # Create opportunity
                        sell_exchange = ex1
                        buy_exchange = ex2
                        sell_price = exchange_mid_prices[ex1]["bid"]
                        buy_price = exchange_mid_prices[ex2]["ask"]
                        volume = min(exchange_mid_prices[ex1]["bid_qty"], exchange_mid_prices[ex2]["ask_qty"])

                        # Recalculate spread using actual bid/ask
                        spread = (sell_price - buy_price) / buy_price
                        logger.info(
                            f"📊 Recalculated spread using bid/ask: {spread:.6f} (threshold: {self.threshold:.6f})"
                        )

                        if spread > self.threshold:
                            opportunities.append(
                                {
                                    "buy_exchange": buy_exchange,
                                    "sell_exchange": sell_exchange,
                                    "buy_price": buy_price,
                                    "sell_price": sell_price,
                                    "spread": spread,
                                    "volume": volume,
                                }
                            )
                            # Log each individual opportunity
                            logger.info(
                                f"💰 POTENTIAL OPPORTUNITY: {asset}\n"
                                f"  • Buy on {buy_exchange} @ {buy_price:.8f}\n"
                                f"  • Sell on {sell_exchange} @ {sell_price:.8f}\n"
                                f"  • Spread: {spread:.6%}\n"
                                f"  • Volume: {volume:.8f}\n"
                                f"  • Est. Profit: {(spread * volume * buy_price):.8f}"
                            )
                        else:
                            logger.info(f"❌ Recalculated spread {spread:.6f} below threshold {self.threshold}")
                    else:
                        logger.info(f"❌ Price difference {price_diff:.6f} below threshold {self.threshold}")
                else:
                    # ex2 has higher price (sell on ex2, buy on ex1)
                    price_diff = (price2 - price1) / price1
                    logger.info(f"📉 {ex2} price higher than {ex1} by {price_diff:.6f} ({price2:.8f} vs {price1:.8f})")

                    if price_diff > self.threshold:
                        # Create opportunity
                        sell_exchange = ex2
                        buy_exchange = ex1
                        sell_price = exchange_mid_prices[ex2]["bid"]
                        buy_price = exchange_mid_prices[ex1]["ask"]
                        volume = min(exchange_mid_prices[ex2]["bid_qty"], exchange_mid_prices[ex1]["ask_qty"])

                        # Recalculate spread using actual bid/ask
                        spread = (sell_price - buy_price) / buy_price
                        logger.info(
                            f"📊 Recalculated spread using bid/ask: {spread:.6f} (threshold: {self.threshold:.6f})"
                        )

                        if spread > self.threshold:
                            opportunities.append(
                                {
                                    "buy_exchange": buy_exchange,
                                    "sell_exchange": sell_exchange,
                                    "buy_price": buy_price,
                                    "sell_price": sell_price,
                                    "spread": spread,
                                    "volume": volume,
                                }
                            )
                            # Log each individual opportunity
                            logger.info(
                                f"💰 POTENTIAL OPPORTUNITY: {asset}\n"
                                f"  • Buy on {buy_exchange} @ {buy_price:.8f}\n"
                                f"  • Sell on {sell_exchange} @ {sell_price:.8f}\n"
                                f"  • Spread: {spread:.6%}\n"
                                f"  • Volume: {volume:.8f}\n"
                                f"  • Est. Profit: {(spread * volume * buy_price):.8f}"
                            )
                        else:
                            logger.info(f"❌ Recalculated spread {spread:.6f} below threshold {self.threshold}")
                    else:
                        logger.info(f"❌ Price difference {price_diff:.6f} below threshold {self.threshold}")

        # Select best opportunity (highest spread)
        if opportunities:
            logger.info(f"✅ Found {len(opportunities)} potential opportunities for {asset}")
            best_opp = max(opportunities, key=lambda x: x["spread"])

            logger.info(
                f"💰 BEST ARBITRAGE OPPORTUNITY for {asset}:\n"
                f"  • Buy from {best_opp['buy_exchange']} at {best_opp['buy_price']:.8f}\n"
                f"  • Sell on {best_opp['sell_exchange']} at {best_opp['sell_price']:.8f}\n"
                f"  • Spread: {best_opp['spread']:.4%}\n"
                f"  • Volume: {best_opp['volume']:.8f}\n"
                f"  • Est. Profit: {(best_opp['spread'] * best_opp['volume'] * best_opp['buy_price']):.8f}"
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
        else:
            logger.info(f"❌ No profitable opportunities found for {asset}")

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

        # Check if opportunity is fresh (within last 3 seconds)
        if time.time() - opportunity.opportunity_timestamp > 3:
            return False

        return True

    def calculate_spread(self, opportunity: TradeOpportunity) -> float:
        """Calculate the spread (already calculated in the opportunity object)"""
        return opportunity.spread
