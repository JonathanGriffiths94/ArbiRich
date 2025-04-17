import logging
import time
import uuid
from typing import Dict, Optional

from src.arbirich.models.models import OrderBookState, TradeOpportunity

from ..risk.exchange_profiles import ExchangeRiskProfiler
from .arbitrage_type import ArbitrageType

logger = logging.getLogger(__name__)


class LiquidityAdjustedArbitrage(ArbitrageType):
    """
    Liquidity-Adjusted Arbitrage Strategy

    This strategy extends the VWAP approach by incorporating dynamic slippage
    estimation, exchange-specific risk factors, and liquidity scoring to
    optimize arbitrage executions based on real market conditions.
    """

    def __init__(self, strategy_id=None, strategy_name=None, config=None):
        """
        Initialize the Liquidity-Adjusted arbitrage strategy.

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
                self.name = config.get("name", "liquidity_adjusted_arbitrage")
                self.id = str(self.name)
                super().__init__(config)
            else:
                self.name = strategy_id or "liquidity_adjusted_arbitrage"
                self.id = str(self.name)
                super().__init__(config or {})

        # Base configuration parameters
        self.threshold = self.config.get("threshold", 0.0015)  # Default 0.15%
        self.base_volume = self.config.get("target_volume", 100.0)  # Default target volume in base currency
        self.min_depth_percentage = self.config.get("min_depth_percentage", 0.7)  # Minimum % of target volume

        # Liquidity adjustment parameters
        self.slippage_factor = self.config.get("slippage_factor", 0.5)  # Weight for slippage consideration
        self.liquidity_multiplier = self.config.get("liquidity_multiplier", 1.5)  # Reward higher liquidity
        self.exchange_risk_factors = self.config.get("exchange_risk_factors", {})

        # Initialize the exchange risk profiler
        custom_profiles = self.config.get("exchange_risk_profiles", {})
        self.exchange_risk_profiler = ExchangeRiskProfiler(custom_profiles)

        # Use the profiler to set risk factors if not explicitly configured
        if not self.exchange_risk_factors:
            exchanges = self.config.get("exchanges", [])
            self.exchange_risk_factors = {
                exchange: self.exchange_risk_profiler.get_risk_factor(exchange) for exchange in exchanges
            }

        # Log the risk factors
        logger.info(f"Exchange risk factors: {self.exchange_risk_factors}")

        # Dynamic adjustment parameters
        self.dynamic_volume_adjust = self.config.get("dynamic_volume_adjust", True)
        self.max_slippage_tolerance = self.config.get("max_slippage", 0.0005)

        # Timeout setting for opportunity validity
        self.opportunity_timeout = self.config.get("opportunity_timeout", 2.0)  # seconds

    def detect_opportunities(self, state: OrderBookState) -> Dict[str, TradeOpportunity]:
        """
        Detect liquidity-adjusted arbitrage opportunities across all symbols

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
        Detect liquidity-adjusted arbitrage opportunities accounting for depth and risk factors.

        Parameters:
            asset: The asset symbol (e.g., 'BTC-USDT')
            state: Current order book state across exchanges

        Returns:
            TradeOpportunity object if an opportunity is found, None otherwise
        """
        logger.debug(f"Checking liquidity-adjusted arbitrage for {asset} with threshold {self.threshold:.6f}")

        if asset not in state.symbols:
            return None

        if len(state.symbols[asset]) < 2:
            # Need at least 2 exchanges to compare
            return None

        # Calculate liquidity-adjusted prices for each exchange
        exchange_data = {}

        for exchange, order_book in state.symbols[asset].items():
            # Skip if book is empty or invalid
            if not order_book.bids or not order_book.asks:
                continue

            try:
                # Calculate weighted average prices and get depth info
                bid_data = self._calculate_liquidity_adjusted_price(order_book.bids, self.base_volume, is_bid=True)
                ask_data = self._calculate_liquidity_adjusted_price(order_book.asks, self.base_volume, is_bid=False)

                if bid_data and ask_data:
                    # Apply exchange risk factor
                    risk_factor = self.exchange_risk_factors.get(exchange, 0.9)

                    # Calculate liquidity score (higher is better)
                    liquidity_score = self._calculate_liquidity_score(bid_data, ask_data, risk_factor)

                    exchange_data[exchange] = {
                        "weighted_bid": bid_data["price"],
                        "weighted_ask": ask_data["price"],
                        "bid_volume": bid_data["volume"],
                        "ask_volume": ask_data["volume"],
                        "bid_slippage": bid_data["slippage"],
                        "ask_slippage": ask_data["slippage"],
                        "liquidity_score": liquidity_score,
                        "bid_depth": bid_data["depth"],
                        "ask_depth": ask_data["depth"],
                        "risk_factor": risk_factor,
                    }
            except Exception as e:
                logger.error(f"Error calculating weighted prices for {exchange}: {e}")

        # Find arbitrage opportunities
        all_opportunities = []
        exchanges = list(exchange_data.keys())

        for i in range(len(exchanges)):
            for j in range(i + 1, len(exchanges)):
                ex1 = exchanges[i]
                ex2 = exchanges[j]

                # Direction 1: Buy on ex1, sell on ex2
                self._evaluate_opportunity(ex1, ex2, exchange_data, asset, all_opportunities)

                # Direction 2: Buy on ex2, sell on ex1
                self._evaluate_opportunity(ex2, ex1, exchange_data, asset, all_opportunities)

        # Select best opportunity based on adjusted score
        if all_opportunities:
            # Sort by adjusted score (combination of spread and liquidity)
            all_opportunities.sort(key=lambda x: x["adjusted_score"], reverse=True)
            best_opp = all_opportunities[0]

            logger.info(
                f"Found liquidity-adjusted arbitrage for {asset}: "
                f"Buy from {best_opp['buy_exchange']} at {best_opp['buy_price']:.8f}, "
                f"Sell on {best_opp['sell_exchange']} at {best_opp['sell_price']:.8f}, "
                f"Spread: {best_opp['spread']:.4%}, "
                f"Adjusted Volume: {best_opp['adjusted_volume']:.8f}, "
                f"Score: {best_opp['adjusted_score']:.4f}"
            )

            # Create opportunity object
            opportunity = TradeOpportunity(
                id=str(uuid.uuid4()),
                strategy=self.name,
                pair=asset,
                buy_exchange=best_opp["buy_exchange"],
                sell_exchange=best_opp["sell_exchange"],
                buy_price=best_opp["buy_price"],
                sell_price=best_opp["sell_price"],
                spread=best_opp["spread"],
                volume=best_opp["adjusted_volume"],
                opportunity_timestamp=time.time(),
                metadata={
                    "liquidity_score": best_opp["liquidity_score"],
                    "estimated_slippage": best_opp["estimated_slippage"],
                    "adjusted_score": best_opp["adjusted_score"],
                    "max_depth": best_opp["max_depth"],
                },
            )

            # Add detailed logging for opportunity
            logger.info(
                f"LIQUIDITY-ADJUSTED OPPORTUNITY DETAILS:\n"
                f"  ID: {opportunity.id}\n"
                f"  Asset: {asset}\n"
                f"  Buy Exchange: {opportunity.buy_exchange} @ {opportunity.buy_price:.8f}\n"
                f"  Sell Exchange: {opportunity.sell_exchange} @ {opportunity.sell_price:.8f}\n"
                f"  Raw Spread: {opportunity.spread:.6%}\n"
                f"  Adjusted Volume: {opportunity.volume:.8f}\n"
                f"  Liquidity Score: {best_opp['liquidity_score']:.4f}\n"
                f"  Estimated Slippage: {best_opp['estimated_slippage']:.6%}\n"
                f"  Net Spread (after slippage): {(opportunity.spread - best_opp['estimated_slippage']):.6%}\n"
                f"  Est. Profit: {(opportunity.spread * opportunity.volume * opportunity.buy_price):.8f}"
            )

            return opportunity

        return None

    def _evaluate_opportunity(self, buy_exchange, sell_exchange, exchange_data, asset, all_opportunities):
        """
        Evaluate potential arbitrage opportunity between two exchanges

        Args:
            buy_exchange: Exchange to buy on
            sell_exchange: Exchange to sell on
            exchange_data: Dictionary of exchange data
            asset: Asset symbol being traded
            all_opportunities: List to append valid opportunities to

        Returns:
            Updated opportunities list
        """
        buy_price = exchange_data[buy_exchange]["weighted_ask"]
        sell_price = exchange_data[sell_exchange]["weighted_bid"]
        buy_volume = exchange_data[buy_exchange]["ask_volume"]
        sell_volume = exchange_data[sell_exchange]["bid_volume"]

        # Replace the direct risk factor access with a call to the risk profiler
        # This ensures we get the most up-to-date risk factor including any dynamic adjustments
        buy_risk = self.exchange_risk_profiler.get_risk_factor(buy_exchange)
        sell_risk = self.exchange_risk_profiler.get_risk_factor(sell_exchange)

        # Update the stored risk factors (useful for logging and other components)
        exchange_data[buy_exchange]["risk_factor"] = buy_risk
        exchange_data[sell_exchange]["risk_factor"] = sell_risk

        # Calculate raw and risk-adjusted spread
        raw_spread = (sell_price - buy_price) / buy_price
        risk_adjusted_spread = raw_spread * buy_risk * sell_risk

        # Only consider if risk-adjusted spread exceeds threshold
        if risk_adjusted_spread > self.threshold:
            # Estimate slippage based on order book depth
            estimated_buy_slippage = exchange_data[buy_exchange]["ask_slippage"]
            estimated_sell_slippage = exchange_data[sell_exchange]["bid_slippage"]
            total_slippage = estimated_buy_slippage + estimated_sell_slippage

            # Skip if slippage would eliminate profit
            if total_slippage >= raw_spread:
                return all_opportunities

            # Check if we have sufficient volume
            available_volume = min(buy_volume, sell_volume)
            min_required = self.base_volume * self.min_depth_percentage

            if available_volume >= min_required:
                # Calculate adjusted volume based on liquidity conditions
                adjusted_volume = self._calculate_adjusted_volume(
                    available_volume,
                    exchange_data[buy_exchange]["ask_depth"],
                    exchange_data[sell_exchange]["bid_depth"],
                    buy_risk,
                    sell_risk,
                )

                # Combined liquidity score
                liquidity_score = (
                    exchange_data[buy_exchange]["liquidity_score"] + exchange_data[sell_exchange]["liquidity_score"]
                ) / 2

                # Final adjusted score - considers spread, liquidity, and slippage
                adjusted_score = (raw_spread - total_slippage) * self.liquidity_multiplier * liquidity_score

                # Store detailed opportunity data
                all_opportunities.append(
                    {
                        "buy_exchange": buy_exchange,
                        "sell_exchange": sell_exchange,
                        "buy_price": buy_price,
                        "sell_price": sell_price,
                        "spread": raw_spread,
                        "risk_adjusted_spread": risk_adjusted_spread,
                        "raw_volume": available_volume,
                        "adjusted_volume": adjusted_volume,
                        "estimated_slippage": total_slippage,
                        "liquidity_score": liquidity_score,
                        "adjusted_score": adjusted_score,
                        "max_depth": min(
                            exchange_data[buy_exchange]["ask_depth"], exchange_data[sell_exchange]["bid_depth"]
                        ),
                    }
                )

        return all_opportunities

    def _calculate_liquidity_adjusted_price(self, orders, target_volume, is_bid=True):
        """
        Calculate liquidity-adjusted weighted average price with slippage estimates

        Args:
            orders: Dictionary of price -> quantity for orders
            target_volume: Target volume to execute
            is_bid: True if calculating for bids, False for asks

        Returns:
            Dictionary with weighted average price, slippage estimates, and depths
        """
        total_volume = 0
        weighted_sum = 0
        price_levels = 0
        total_depth = 0

        # Sort prices (descending for bids, ascending for asks)
        sorted_prices = sorted(orders.keys(), reverse=is_bid)

        # Get the best price for slippage calculation
        best_price = sorted_prices[0] if sorted_prices else 0

        # Track price movements for slippage estimation
        price_movements = []

        for price in sorted_prices:
            qty = orders[price]
            total_depth += qty

            available = min(qty, target_volume - total_volume)
            if available <= 0:
                continue

            weighted_sum += price * available
            total_volume += available
            price_levels += 1

            # Track price movement from best price
            price_delta = abs(price - best_price) / best_price
            price_movements.append((price_delta, available))

            if total_volume >= target_volume:
                break

        if total_volume <= 0:
            return None

        weighted_price = weighted_sum / total_volume

        # Calculate estimated slippage as volume-weighted average price deviation
        slippage = 0
        if price_movements:
            slippage = sum(delta * vol for delta, vol in price_movements) / total_volume

        return {
            "price": weighted_price,
            "volume": total_volume,
            "slippage": slippage,
            "depth": total_depth,
            "price_levels": price_levels,
        }

    def _calculate_liquidity_score(self, bid_data, ask_data, risk_factor):
        """
        Calculate a liquidity score based on order book depth and structure

        Args:
            bid_data: Bid side order book analysis
            ask_data: Ask side order book analysis
            risk_factor: Exchange risk factor

        Returns:
            Liquidity score (higher is better)
        """
        # Factors to consider for liquidity scoring:
        # 1. Total depth relative to target volume
        # 2. Number of price levels (more = better)
        # 3. Slippage (lower = better)

        depth_score = min(1.0, (bid_data["depth"] + ask_data["depth"]) / (2 * self.base_volume * 3))
        levels_score = min(1.0, (bid_data["price_levels"] + ask_data["price_levels"]) / 10)
        slippage_score = 1.0 - min(1.0, (bid_data["slippage"] + ask_data["slippage"]) * 50)

        # Combined score weighted by importance
        liquidity_score = (depth_score * 0.5 + levels_score * 0.2 + slippage_score * 0.3) * risk_factor

        return liquidity_score

    def _calculate_adjusted_volume(self, available_volume, buy_depth, sell_depth, buy_risk, sell_risk):
        """
        Calculate adjusted trading volume based on market conditions

        Args:
            available_volume: Raw available volume
            buy_depth: Total depth on buy side
            sell_depth: Total depth on sell side
            buy_risk: Buy exchange risk factor
            sell_risk: Sell exchange risk factor

        Returns:
            Adjusted trading volume
        """
        if not self.dynamic_volume_adjust:
            return available_volume

        # Scale position by depth relative to base volume
        depth_ratio = min(buy_depth, sell_depth) / (self.base_volume * 3)
        depth_factor = min(1.0, depth_ratio) * 0.9 + 0.1  # Between 0.1 and 1.0

        # Apply risk factors
        risk_factor = buy_risk * sell_risk

        # Calculate final adjusted volume
        adjusted_volume = available_volume * depth_factor * risk_factor

        # Ensure we don't return more than available
        return min(adjusted_volume, available_volume)

    def validate_opportunity(self, opportunity: TradeOpportunity) -> bool:
        """
        Validate if the opportunity meets all criteria for execution

        Args:
            opportunity: The arbitrage opportunity to validate

        Returns:
            True if the opportunity is valid, False otherwise
        """
        if not opportunity:
            return False

        # Extract metadata if available
        metadata = opportunity.metadata or {}

        # Check if spread exceeds threshold after slippage
        estimated_slippage = metadata.get("estimated_slippage", 0)
        net_spread = opportunity.spread - estimated_slippage

        if net_spread <= self.threshold:
            logger.debug(f"Opportunity rejected: Net spread {net_spread:.6f} below threshold {self.threshold:.6f}")
            return False

        # Check if volume is sufficient
        min_required = self.base_volume * self.min_depth_percentage
        if opportunity.volume < min_required:
            logger.debug(f"Opportunity rejected: Volume {opportunity.volume:.6f} below minimum {min_required:.6f}")
            return False

        # Check if liquidity score is acceptable
        liquidity_score = metadata.get("liquidity_score", 0)
        if liquidity_score < 0.5:  # Minimum acceptable liquidity score
            logger.debug(f"Opportunity rejected: Liquidity score {liquidity_score:.4f} below minimum 0.5")
            return False

        # Check if opportunity is fresh (within timeout period)
        if time.time() - opportunity.opportunity_timestamp > self.opportunity_timeout:
            logger.debug(
                f"Opportunity rejected: Age {time.time() - opportunity.opportunity_timestamp:.2f}s exceeds timeout {self.opportunity_timeout:.2f}s"
            )
            return False

        return True

    def calculate_spread(self, opportunity: TradeOpportunity) -> float:
        """
        Calculate the effective spread accounting for estimated slippage

        Args:
            opportunity: The arbitrage opportunity

        Returns:
            Effective spread after slippage adjustment
        """
        # Extract metadata if available
        metadata = opportunity.metadata or {}
        estimated_slippage = metadata.get("estimated_slippage", 0)

        # Calculate net spread after slippage
        return max(0, opportunity.spread - estimated_slippage)
