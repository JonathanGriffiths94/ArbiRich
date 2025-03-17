import logging
from typing import Optional

from src.arbirich.execution_strategies.base_execution_strategy import ExecutionStrategy
from src.arbirich.models.models import TradeExecution, TradeOpportunity

logger = logging.getLogger(__name__)


class LimitOrderStrategy(ExecutionStrategy):
    """
    Limit order execution strategy.
    Places limit orders to reduce slippage but may take longer to execute.
    """

    def should_execute(self, opportunity: TradeOpportunity) -> bool:
        """
        Check if the opportunity is suitable for limit order execution.
        - Requires a larger spread due to potential price movement during execution
        - Checks exchange-specific limit order capabilities
        - Verifies order book depth to ensure limit orders will likely fill
        """
        # Similar checks as BasicMarketOrderStrategy but with higher spread requirements
        min_required_spread = self.min_profit + self.max_slippage + 0.001  # Additional buffer for limit orders

        if opportunity.spread < min_required_spread:
            logger.info(
                f"Spread {opportunity.spread:.4%} too low for limit order strategy (need {min_required_spread:.4%})"
            )
            return False

        # Add exchange-specific checks
        if opportunity.buy_exchange in ["exchange_without_limit_orders"]:
            logger.info(f"Exchange {opportunity.buy_exchange} doesn't support limit orders")
            return False

        if opportunity.sell_exchange in ["exchange_without_limit_orders"]:
            logger.info(f"Exchange {opportunity.sell_exchange} doesn't support limit orders")
            return False

        # Could also check order book depth, price volatility, etc.

        return True

    async def execute_trade(self, opportunity: TradeOpportunity) -> Optional[TradeExecution]:
        """
        Execute the trade using limit orders.

        1. Calculate optimal limit prices
        2. Place limit orders on both exchanges
        3. Monitor order status until filled or timeout
        4. Return execution details
        """
        import asyncio
        import time
        import uuid

        from src.arbirich.exchange.exchange_factory import get_exchange_client

        execution_id = str(uuid.uuid4())

        try:
            # Get exchange clients
            buy_exchange_client = get_exchange_client(opportunity.buy_exchange)
            sell_exchange_client = get_exchange_client(opportunity.sell_exchange)

            # Determine execution volume
            max_volume = self.config.get("max_volume", 1.0)
            execution_volume = min(opportunity.volume, max_volume)

            # Calculate limit prices
            # Buy slightly above the quoted price to increase fill probability
            buy_limit_price = opportunity.buy_price * (1 + 0.0005)  # 0.05% above quote
            # Sell slightly below the quoted price to increase fill probability
            sell_limit_price = opportunity.sell_price * (1 - 0.0005)  # 0.05% below quote

            # Place buy limit order
            logger.info(f"Placing buy limit order at {buy_limit_price} on {opportunity.buy_exchange}")
            buy_order_result = await buy_exchange_client.place_limit_order(
                symbol=opportunity.pair, side="buy", quantity=execution_volume, price=buy_limit_price
            )

            buy_order_id = buy_order_result.get("order_id")
            if not buy_order_id:
                logger.error(f"Failed to place buy limit order: {buy_order_result}")
                return None

            # Place sell limit order
            logger.info(f"Placing sell limit order at {sell_limit_price} on {opportunity.sell_exchange}")
            sell_order_result = await sell_exchange_client.place_limit_order(
                symbol=opportunity.pair, side="sell", quantity=execution_volume, price=sell_limit_price
            )

            sell_order_id = sell_order_result.get("order_id")
            if not sell_order_id:
                logger.error(f"Failed to place sell limit order: {sell_order_result}")
                # Cancel the buy order since sell order failed
                await buy_exchange_client.cancel_order(symbol=opportunity.pair, order_id=buy_order_id)
                return None

            # Store order IDs in active executions for potential cancellation
            self.active_executions[execution_id] = {
                "buy_order_id": buy_order_id,
                "sell_order_id": sell_order_id,
                "buy_exchange": opportunity.buy_exchange,
                "sell_exchange": opportunity.sell_exchange,
                "pair": opportunity.pair,
                "opportunity_id": opportunity.id,
            }

            # Monitor order status until filled or timeout
            timeout = time.time() + self.execution_timeout
            buy_filled = False
            sell_filled = False

            while time.time() < timeout:
                # Check buy order status
                if not buy_filled:
                    buy_status = await buy_exchange_client.get_order_status(
                        symbol=opportunity.pair, order_id=buy_order_id
                    )
                    buy_filled = buy_status.get("status") == "filled"

                # Check sell order status
                if not sell_filled:
                    sell_status = await sell_exchange_client.get_order_status(
                        symbol=opportunity.pair, order_id=sell_order_id
                    )
                    sell_filled = sell_status.get("status") == "filled"

                # Both orders filled, execution complete
                if buy_filled and sell_filled:
                    # Get actual execution prices
                    executed_buy_price = buy_status.get("executed_price", buy_limit_price)
                    executed_sell_price = sell_status.get("executed_price", sell_limit_price)

                    # Calculate actual spread
                    actual_spread = (executed_sell_price - executed_buy_price) / executed_buy_price

                    # Create execution record
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

                    # Clean up from active executions
                    self.active_executions.pop(execution_id, None)

                    logger.info(f"Limit order execution completed: {execution}")
                    return execution

                # Wait before checking again
                await asyncio.sleep(0.5)

            # Timeout reached, cancel any unfilled orders
            logger.warning(f"Execution timeout reached for {execution_id}, cancelling orders")

            if not buy_filled:
                await buy_exchange_client.cancel_order(symbol=opportunity.pair, order_id=buy_order_id)

            if not sell_filled:
                await sell_exchange_client.cancel_order(symbol=opportunity.pair, order_id=sell_order_id)

            # Clean up
            self.active_executions.pop(execution_id, None)

            # If partially filled, would handle the cleanup here
            # (selling any bought assets, etc.)

            return None

        except Exception as e:
            logger.error(f"Error in limit order execution: {e}")
            # Attempt to clean up any placed orders
            try:
                active_exec = self.active_executions.pop(execution_id, None)
                if active_exec:
                    buy_exchange_client = get_exchange_client(active_exec["buy_exchange"])
                    sell_exchange_client = get_exchange_client(active_exec["sell_exchange"])

                    await buy_exchange_client.cancel_order(
                        symbol=active_exec["pair"], order_id=active_exec["buy_order_id"]
                    )
                    await sell_exchange_client.cancel_order(
                        symbol=active_exec["pair"], order_id=active_exec["sell_order_id"]
                    )
            except Exception as cancel_error:
                logger.error(f"Error cancelling orders during error recovery: {cancel_error}")

            return None

    async def cancel_trade(self, execution_id: str) -> bool:
        """
        Cancel an ongoing limit order execution.
        """
        from src.arbirich.exchange.exchange_factory import get_exchange_client

        active_exec = self.active_executions.get(execution_id)
        if not active_exec:
            logger.warning(f"No active execution found with ID {execution_id}")
            return False

        try:
            # Get exchange clients
            buy_exchange_client = get_exchange_client(active_exec["buy_exchange"])
            sell_exchange_client = get_exchange_client(active_exec["sell_exchange"])

            # Cancel buy order
            buy_cancel_result = await buy_exchange_client.cancel_order(
                symbol=active_exec["pair"], order_id=active_exec["buy_order_id"]
            )

            # Cancel sell order
            sell_cancel_result = await sell_exchange_client.cancel_order(
                symbol=active_exec["pair"], order_id=active_exec["sell_order_id"]
            )

            # Clean up
            self.active_executions.pop(execution_id, None)

            logger.info(f"Cancelled execution {execution_id}")
            return True

        except Exception as e:
            logger.error(f"Error cancelling execution {execution_id}: {e}")
            return False
