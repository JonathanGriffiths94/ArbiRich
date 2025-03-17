import logging
from typing import Optional

from src.arbirich.execution_strategies.base_execution_strategy import ExecutionStrategy
from src.arbirich.models.models import TradeExecution, TradeOpportunity

logger = logging.getLogger(__name__)


class MarketOrderStrategy(ExecutionStrategy):
    """
    Simple market order execution strategy.
    Executes trades using market orders for fast execution.
    """

    def should_execute(self, opportunity: TradeOpportunity) -> bool:
        """
        Check if the opportunity meets basic execution criteria:
        - Spread exceeds minimum profit threshold plus maximum slippage
        - Volume is within reasonable bounds
        - Both exchanges are supported
        """
        # Check if we support both exchanges
        if opportunity.buy_exchange not in self.exchanges or opportunity.sell_exchange not in self.exchanges:
            logger.info(
                f"Skipping opportunity: unsupported exchanges {opportunity.buy_exchange}, {opportunity.sell_exchange}"
            )
            return False

        # Check if pair is supported
        if opportunity.pair not in self.pairs:
            logger.info(f"Skipping opportunity: unsupported pair {opportunity.pair}")
            return False

        # Calculate minimum required spread
        min_required_spread = self.min_profit + 2 * self.max_slippage  # Account for slippage on both sides

        # Check if spread is sufficient
        if opportunity.spread < min_required_spread:
            logger.info(
                f"Skipping opportunity: spread {opportunity.spread:.4%} below required {min_required_spread:.4%}"
            )
            return False

        # Check volume requirements
        min_volume = self.config.get("min_volume", 0.001)  # Minimum trade size
        max_volume = self.config.get("max_volume", 1.0)  # Maximum trade size

        if opportunity.volume < min_volume:
            logger.info(f"Skipping opportunity: volume {opportunity.volume} below minimum {min_volume}")
            return False

        if opportunity.volume > max_volume:
            logger.info(f"Capping opportunity volume from {opportunity.volume} to maximum {max_volume}")
            # We can still execute but will limit the volume

        logger.info(f"Opportunity qualifies for execution: {opportunity}")
        return True

    async def execute_trade(self, opportunity: TradeOpportunity) -> Optional[TradeExecution]:
        """
        Execute the trade using market orders.

        1. Place buy order on the buy exchange
        2. Place sell order on the sell exchange
        3. Return execution details
        """
        import time
        import uuid

        from src.arbirich.exchange.exchange_factory import get_exchange_client

        # Create a unique execution ID
        execution_id = str(uuid.uuid4())

        try:
            # Get exchange clients
            buy_exchange_client = get_exchange_client(opportunity.buy_exchange)
            sell_exchange_client = get_exchange_client(opportunity.sell_exchange)

            # Determine execution volume (respecting max volume limit)
            max_volume = self.config.get("max_volume", 1.0)
            execution_volume = min(opportunity.volume, max_volume)

            # Execute buy order
            logger.info(f"Executing buy order on {opportunity.buy_exchange} for {execution_volume} {opportunity.pair}")
            buy_order_result = await buy_exchange_client.place_market_order(
                symbol=opportunity.pair, side="buy", quantity=execution_volume
            )

            if not buy_order_result or not buy_order_result.get("success"):
                logger.error(f"Buy order failed: {buy_order_result}")
                return None

            # Get the actual execution price
            executed_buy_price = buy_order_result.get("price", opportunity.buy_price)

            # Execute sell order
            logger.info(
                f"Executing sell order on {opportunity.sell_exchange} for {execution_volume} {opportunity.pair}"
            )
            sell_order_result = await sell_exchange_client.place_market_order(
                symbol=opportunity.pair, side="sell", quantity=execution_volume
            )

            if not sell_order_result or not sell_order_result.get("success"):
                logger.error(f"Sell order failed: {sell_order_result}. Attempting to revert buy order.")
                # In a real implementation, you would attempt to sell back the bought assets
                # to minimize losses from a partial execution
                return None

            # Get the actual execution price
            executed_sell_price = sell_order_result.get("price", opportunity.sell_price)

            # Calculate actual spread achieved
            actual_spread = (executed_sell_price - executed_buy_price) / executed_buy_price

            # Create and return the execution record
            execution = TradeExecution(
                id=execution_id,
                strategy=opportunity.strategy,
                pair=opportunity.pair,
                buy_exchange=opportunity.buy_exchange,
                sell_exchange=opportunity.sell_exchange,
                executed_buy_price=executed_buy_price,
                executed_sell_price=executed_sell_price,
                spread=actual_spread,
                volume=execution_volume,
                execution_timestamp=time.time(),
                opportunity_id=opportunity.id,
            )

            logger.info(f"Trade execution completed: {execution}")
            return execution

        except Exception as e:
            logger.error(f"Error executing trade: {e}")
            return None

    async def cancel_trade(self, execution_id: str) -> bool:
        """
        Cancel an ongoing trade execution.
        For market orders, this is generally not possible once submitted.
        """
        logger.warning(f"Cannot cancel market order execution {execution_id} as orders are immediate")
        return False
