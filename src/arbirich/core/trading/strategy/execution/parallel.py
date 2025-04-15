import asyncio
import logging
import time
from typing import Dict

from src.arbirich.models.models import TradeExecution, TradeOpportunity

from .method import ExecutionMethod


class ParallelExecution(ExecutionMethod):
    """
    Executes trade legs in parallel to minimize delay between buy and sell.

    This approach is best for markets with high liquidity and low volatility
    where the risk of divergence during execution is low.
    """

    def __init__(self, config: Dict):
        super().__init__(config)
        self.logger = logging.getLogger("execution.parallel")
        self.timeout = config.get("timeout", 3000)  # ms
        self.retry_attempts = config.get("retry_attempts", 2)
        self.retry_delay = config.get("retry_delay", 200)  # ms
        self.cleanup_failed_trades = config.get("cleanup_failed_trades", True)

    async def execute(self, opportunity: TradeOpportunity, position_size: float) -> TradeExecution:
        """Execute buy and sell legs in parallel"""
        self.logger.info(f"Executing parallel trade for {opportunity} with size {position_size}")
        start_time = time.time()

        # Add validation before attempting execution
        if not opportunity.buy_exchange or not opportunity.sell_exchange:
            error_msg = f"Invalid exchanges: buy={opportunity.buy_exchange}, sell={opportunity.sell_exchange}"
            self.logger.error(error_msg)
            return TradeExecution(
                id=f"invalid-{int(time.time())}",
                strategy=opportunity.strategy,
                pair=opportunity.pair,
                buy_exchange=opportunity.buy_exchange or "",
                sell_exchange=opportunity.sell_exchange or "",
                executed_buy_price=0.0,
                executed_sell_price=0.0,
                spread=0.0,
                volume=position_size,
                execution_timestamp=time.time(),
                success=False,
                partial=False,
                profit=0,
                execution_time=0,
                error=error_msg,
                opportunity_id=opportunity.id,
                details=None,
            )

        if opportunity.buy_price <= 0 or opportunity.sell_price <= 0:
            error_msg = f"Invalid prices: buy={opportunity.buy_price}, sell={opportunity.sell_price}"
            self.logger.error(error_msg)
            return TradeExecution(
                id=f"invalid-{int(time.time())}",
                strategy=opportunity.strategy,
                pair=opportunity.pair,
                buy_exchange=opportunity.buy_exchange,
                sell_exchange=opportunity.sell_exchange,
                executed_buy_price=0.0,
                executed_sell_price=0.0,
                spread=0.0,
                volume=position_size,
                execution_timestamp=time.time(),
                success=False,
                partial=False,
                profit=0,
                execution_time=0,
                error=error_msg,
                opportunity_id=opportunity.id,
                details=None,
            )

        try:
            # Configure tasks for buy and sell legs
            buy_task = self._execute_buy(
                exchange=opportunity.buy_exchange,
                trading_pair=opportunity.pair,
                price=opportunity.buy_price,
                volume=position_size,
            )

            sell_task = self._execute_sell(
                exchange=opportunity.sell_exchange,
                trading_pair=opportunity.pair,
                price=opportunity.sell_price,
                volume=position_size,
            )

            # Execute both legs with timeout
            # Fixed: Use wait_for instead of passing timeout to gather directly
            try:
                # Run both tasks concurrently with a timeout
                results = await asyncio.wait_for(
                    asyncio.gather(buy_task, sell_task),
                    timeout=self.timeout / 1000.0,  # Convert ms to seconds
                )
                buy_result, sell_result = results
            except asyncio.TimeoutError:
                self.logger.error(f"Execution timed out after {self.timeout}ms")
                return TradeExecution(
                    id=f"timeout-{int(time.time())}",
                    strategy=opportunity.strategy,
                    pair=opportunity.pair,
                    buy_exchange=opportunity.buy_exchange,
                    sell_exchange=opportunity.sell_exchange,
                    executed_buy_price=0.0,
                    executed_sell_price=0.0,
                    spread=0.0,
                    volume=position_size,
                    execution_timestamp=time.time(),
                    success=False,
                    partial=False,
                    profit=0,
                    execution_time=(time.time() - start_time) * 1000,
                    error=f"Execution timed out after {self.timeout}ms",
                    opportunity_id=opportunity.id,
                    details=None,
                )

            # Process results - handle any leg failures
            buy_success = buy_result.get("success", False) if isinstance(buy_result, dict) else False
            sell_success = sell_result.get("success", False) if isinstance(sell_result, dict) else False

            if not buy_success or not sell_success:
                # Handle partial execution
                partial = buy_success or sell_success
                error_msg = ""

                if not buy_success:
                    buy_error = (
                        buy_result.get("error", "Unknown buy error")
                        if isinstance(buy_result, dict)
                        else str(buy_result)
                    )
                    error_msg += f"Buy failed: {buy_error}. "

                if not sell_success:
                    sell_error = (
                        sell_result.get("error", "Unknown sell error")
                        if isinstance(sell_result, dict)
                        else str(sell_result)
                    )
                    error_msg += f"Sell failed: {sell_error}"

                execution_time = (time.time() - start_time) * 1000
                self.logger.error(f"Trade execution failed: {error_msg}")

                # For partial executions, we might need to cleanup
                if partial and self.cleanup_failed_trades:
                    if buy_success and not sell_success:
                        self.logger.warning("Buy succeeded but sell failed - initiating cleanup")
                        await self._cleanup_partial_buy(buy_result, opportunity)
                    elif sell_success and not buy_success:
                        self.logger.warning("Sell succeeded but buy failed - initiating cleanup")
                        await self._cleanup_partial_sell(sell_result, opportunity)

                # Return execution with failure details
                return TradeExecution(
                    id=f"failed-{int(time.time())}",
                    strategy=opportunity.strategy,
                    pair=opportunity.pair,
                    buy_exchange=opportunity.buy_exchange,
                    sell_exchange=opportunity.sell_exchange,
                    executed_buy_price=buy_result.get("executed_price", 0.0) if isinstance(buy_result, dict) else 0.0,
                    executed_sell_price=sell_result.get("executed_price", 0.0)
                    if isinstance(sell_result, dict)
                    else 0.0,
                    spread=0.0,
                    volume=position_size,
                    execution_timestamp=time.time(),
                    success=False,
                    partial=partial,
                    profit=0,
                    execution_time=execution_time,
                    error=error_msg,
                    opportunity_id=opportunity.id,
                    details={
                        "buy_result": buy_result if isinstance(buy_result, dict) else None,
                        "sell_result": sell_result if isinstance(sell_result, dict) else None,
                    },
                )

            # Both legs succeeded
            execution_time = (time.time() - start_time) * 1000

            # Calculate profit
            buy_price = buy_result.get("executed_price", opportunity.buy_price)
            sell_price = sell_result.get("executed_price", opportunity.sell_price)
            profit = (sell_price - buy_price) * position_size

            self.logger.info(f"Parallel execution complete. Profit: {profit}, Time: {execution_time}ms")

            return TradeExecution(
                id=f"success-{int(time.time())}",
                strategy=opportunity.strategy,
                pair=opportunity.pair,
                buy_exchange=opportunity.buy_exchange,
                sell_exchange=opportunity.sell_exchange,
                executed_buy_price=buy_price,
                executed_sell_price=sell_price,
                spread=(sell_price - buy_price) / buy_price if buy_price > 0 else 0,
                volume=position_size,
                execution_timestamp=time.time(),
                success=True,
                partial=False,
                profit=profit,
                execution_time=execution_time,
                opportunity_id=opportunity.id,
                details={"buy_result": buy_result, "sell_result": sell_result},
            )

        except Exception as e:
            self.logger.error(f"Error executing trade: {e}")
            return TradeExecution(
                id=f"error-{int(time.time())}",
                strategy=opportunity.strategy,
                pair=opportunity.pair,
                buy_exchange=opportunity.buy_exchange,
                sell_exchange=opportunity.sell_exchange,
                executed_buy_price=0.0,
                executed_sell_price=0.0,
                spread=0.0,
                volume=position_size,
                execution_timestamp=time.time(),
                success=False,
                partial=False,
                profit=0,
                execution_time=(time.time() - start_time) * 1000,
                error=str(e),
                opportunity_id=opportunity.id,
                details=None,
            )

    async def handle_partial_execution(self, result: TradeExecution) -> None:
        """Handle partial execution by attempting to rebalance positions"""
        self.logger.warning("Handling partial execution in parallel trade")

        details = result.details or {}
        buy_result = details.get("buy_result", {})
        sell_result = details.get("sell_result", {})

        # Use the same cleanup logic as during execution
        if buy_result and buy_result.get("success") and not (sell_result and sell_result.get("success")):
            await self._cleanup_partial_buy(buy_result)

        elif sell_result and sell_result.get("success") and not (buy_result and buy_result.get("success")):
            await self._cleanup_partial_sell(sell_result)

    async def handle_failure(self, result: TradeExecution) -> None:
        """Handle complete failure of trade execution"""
        self.logger.error(f"Parallel trade failed: {result.error}")
        # Implement retry or circuit breaker logic if needed

    async def _execute_buy(self, exchange: str, trading_pair: str, price: float, volume: float) -> Dict:
        """Execute a buy order"""
        # Use the execution service instead of direct exchange service
        from src.arbirich.models.config_models import ExecutionConfig
        from src.arbirich.services.execution.execution_service import ExecutionService

        # Get singleton instance instead of creating a new one
        execution_service = ExecutionService.get_instance(method_type="parallel", config=ExecutionConfig())
        await execution_service.initialize()

        # Format the trade data for the execution service
        trade_data = {
            "exchange": exchange,
            "symbol": trading_pair,
            "side": "buy",
            "price": price,
            "amount": volume,
            "order_type": "limit",
        }

        # Execute the trade
        order_result = await execution_service.execute_trade(trade_data)
        return order_result

    async def _execute_sell(self, exchange: str, trading_pair: str, price: float, volume: float) -> Dict:
        """Execute a sell order"""
        # Use the execution service instead of direct exchange service
        from src.arbirich.models.config_models import ExecutionConfig
        from src.arbirich.services.execution.execution_service import ExecutionService

        # Get singleton instance instead of creating a new one
        execution_service = ExecutionService.get_instance(method_type="parallel", config=ExecutionConfig())
        await execution_service.initialize()

        # Format the trade data for the execution service
        trade_data = {
            "exchange": exchange,
            "symbol": trading_pair,
            "side": "sell",
            "price": price,
            "amount": volume,
            "order_type": "limit",
        }

        # Execute the trade
        order_result = await execution_service.execute_trade(trade_data)
        return order_result

    async def _cleanup_partial_buy(self, buy_result: Dict, opportunity=None) -> None:
        """Clean up after a partial execution where only buy succeeded"""
        # Sell what we bought to minimize risk
        exchange = buy_result.get("exchange")
        trading_pair = buy_result.get("trading_pair") or (opportunity.pair if opportunity else None)
        volume = buy_result.get("executed_volume", 0)

        if not exchange or not trading_pair or not volume:
            self.logger.error("Missing information for buy cleanup")
            return

        if volume > 0:
            self.logger.info(f"Cleaning up partial buy by selling {volume} on {exchange}")
            from src.arbirich.services.execution.execution_service import ExecutionService

            try:
                execution_service = ExecutionService(method_type="parallel")
                await execution_service.initialize()

                # Format the trade data for the execution service
                trade_data = {
                    "exchange": exchange,
                    "symbol": trading_pair,
                    "side": "sell",
                    "price": None,  # Market order
                    "amount": volume,
                    "order_type": "market",
                }

                # Execute the trade
                await execution_service.execute_trade(trade_data)
                self.logger.info("Cleanup successful")
            except Exception as e:
                self.logger.error(f"Error during cleanup: {e}")

    async def _cleanup_partial_sell(self, sell_result: Dict, opportunity=None) -> None:
        """Clean up after a partial execution where only sell succeeded"""
        # Buy to cover what we sold
        exchange = sell_result.get("exchange")
        trading_pair = sell_result.get("trading_pair") or (opportunity.pair if opportunity else None)
        volume = sell_result.get("executed_volume", 0)

        if not exchange or not trading_pair or not volume:
            self.logger.error("Missing information for sell cleanup")
            return

        if volume > 0:
            self.logger.info(f"Cleaning up partial sell by buying {volume} on {exchange}")
            from src.arbirich.services.execution.execution_service import ExecutionService

            try:
                execution_service = ExecutionService(method_type="parallel")
                await execution_service.initialize()

                # Format the trade data for the execution service
                trade_data = {
                    "exchange": exchange,
                    "symbol": trading_pair,
                    "side": "buy",
                    "price": None,  # Market order
                    "amount": volume,
                    "order_type": "market",
                }

                # Execute the trade
                await execution_service.execute_trade(trade_data)
                self.logger.info("Cleanup successful")
            except Exception as e:
                self.logger.error(f"Error during cleanup: {e}")
