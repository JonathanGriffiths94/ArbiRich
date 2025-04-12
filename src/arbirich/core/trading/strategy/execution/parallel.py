import asyncio
import logging
import time
from typing import Any, Dict

from .method import ExecutionMethod, TradeResult


class ParallelExecution(ExecutionMethod):
    """
    Executes all legs of an arbitrage trade simultaneously

    This approach aims to minimize execution time but may increase risk if
    one leg fails while others succeed.
    """

    def __init__(self, config: Dict):
        super().__init__(config)
        self.logger = logging.getLogger("execution.parallel")
        self.timeout = config.get("timeout", 3000)  # ms

    async def execute(self, opportunity: Any, position_size: float) -> TradeResult:
        """Execute all trade legs in parallel"""
        self.logger.info(f"Executing parallel trade for {opportunity} with size {position_size}")
        start_time = time.time()

        try:
            # Create tasks for buy and sell sides
            buy_task = asyncio.create_task(
                self._execute_buy(
                    exchange=opportunity.buy_exchange,
                    trading_pair=opportunity.trading_pair,
                    price=opportunity.buy_price,
                    volume=position_size,
                )
            )

            sell_task = asyncio.create_task(
                self._execute_sell(
                    exchange=opportunity.sell_exchange,
                    trading_pair=opportunity.trading_pair,
                    price=opportunity.sell_price,
                    volume=position_size,
                )
            )

            # Wait for both tasks to complete with timeout
            timeout_seconds = self.timeout / 1000
            results = await asyncio.gather(buy_task, sell_task, return_exceptions=True, timeout=timeout_seconds)

            # Check results
            buy_result, sell_result = results

            if isinstance(buy_result, Exception):
                self.logger.error(f"Buy side failed: {buy_result}")
                return TradeResult(
                    success=False,
                    partial=sell_result.get("success", False),
                    profit=0,
                    execution_time=(time.time() - start_time) * 1000,
                    error=f"Buy side failed: {str(buy_result)}",
                    details={"sell_result": sell_result},
                )

            if isinstance(sell_result, Exception):
                self.logger.error(f"Sell side failed: {sell_result}")
                return TradeResult(
                    success=False,
                    partial=buy_result.get("success", False),
                    profit=0,
                    execution_time=(time.time() - start_time) * 1000,
                    error=f"Sell side failed: {str(sell_result)}",
                    details={"buy_result": buy_result},
                )

            # Both sides succeeded
            execution_time = (time.time() - start_time) * 1000

            # Calculate profit
            buy_cost = buy_result.get("executed_price", opportunity.buy_price) * position_size
            sell_revenue = sell_result.get("executed_price", opportunity.sell_price) * position_size
            profit = sell_revenue - buy_cost

            return TradeResult(
                success=True,
                partial=False,
                profit=profit,
                execution_time=execution_time,
                details={"buy_result": buy_result, "sell_result": sell_result},
            )

        except asyncio.TimeoutError:
            self.logger.error("Trade execution timed out")
            return TradeResult(
                success=False, partial=False, profit=0, execution_time=self.timeout, error="Execution timed out"
            )

        except Exception as e:
            self.logger.error(f"Error executing trade: {e}")
            return TradeResult(
                success=False, partial=False, profit=0, execution_time=(time.time() - start_time) * 1000, error=str(e)
            )

    async def handle_partial_execution(self, result: TradeResult) -> None:
        """Handle partial execution by attempting to rebalance positions"""
        self.logger.warning("Handling partial execution")

        details = result.details or {}
        buy_result = details.get("buy_result", {})
        sell_result = details.get("sell_result", {})

        # Determine which side executed and which failed
        if buy_result.get("success") and not sell_result.get("success"):
            # Buy succeeded but sell failed - we need to sell what we bought
            await self._cleanup_partial_buy(buy_result)

        elif sell_result.get("success") and not buy_result.get("success"):
            # Sell succeeded but buy failed - we need to buy to cover
            await self._cleanup_partial_sell(sell_result)

    async def handle_failure(self, result: TradeResult) -> None:
        """Handle complete failure of trade execution"""
        self.logger.error(f"Trade failed: {result.error}")
        # Log the failure for later analysis
        # Optionally implement retry logic or circuit breaker patterns

    async def _execute_buy(self, exchange: str, trading_pair: str, price: float, volume: float) -> Dict:
        """Execute a buy order"""
        # This would call your actual exchange API
        # For now, this is a placeholder implementation
        from src.arbirich.services.exchange_service import place_order

        order_result = await place_order(
            exchange=exchange, trading_pair=trading_pair, side="buy", price=price, volume=volume, order_type="limit"
        )

        return order_result

    async def _execute_sell(self, exchange: str, trading_pair: str, price: float, volume: float) -> Dict:
        """Execute a sell order"""
        # This would call your actual exchange API
        # For now, this is a placeholder implementation
        from src.arbirich.services.exchange_service import place_order

        order_result = await place_order(
            exchange=exchange, trading_pair=trading_pair, side="sell", price=price, volume=volume, order_type="limit"
        )

        return order_result

    async def _cleanup_partial_buy(self, buy_result: Dict) -> None:
        """Clean up after a partial execution where only buy succeeded"""
        # Sell what we bought to minimize risk
        exchange = buy_result.get("exchange")
        trading_pair = buy_result.get("trading_pair")
        volume = buy_result.get("executed_volume", 0)

        if volume > 0:
            self.logger.info(f"Cleaning up partial buy by selling {volume} on {exchange}")
            from src.arbirich.services.exchange_service import place_order

            try:
                await place_order(
                    exchange=exchange,
                    trading_pair=trading_pair,
                    side="sell",
                    price=None,  # Market order
                    volume=volume,
                    order_type="market",
                )
                self.logger.info("Cleanup successful")
            except Exception as e:
                self.logger.error(f"Error during cleanup: {e}")

    async def _cleanup_partial_sell(self, sell_result: Dict) -> None:
        """Clean up after a partial execution where only sell succeeded"""
        # Buy to cover what we sold
        exchange = sell_result.get("exchange")
        trading_pair = sell_result.get("trading_pair")
        volume = sell_result.get("executed_volume", 0)

        if volume > 0:
            self.logger.info(f"Cleaning up partial sell by buying {volume} on {exchange}")
            from src.arbirich.services.exchange_service import place_order

            try:
                await place_order(
                    exchange=exchange,
                    trading_pair=trading_pair,
                    side="buy",
                    price=None,  # Market order
                    volume=volume,
                    order_type="market",
                )
                self.logger.info("Cleanup successful")
            except Exception as e:
                self.logger.error(f"Error during cleanup: {e}")
