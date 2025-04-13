import asyncio
import logging
import time
from typing import Any, Dict

from .method import ExecutionMethod, TradeResult


class StaggeredExecution(ExecutionMethod):
    """
    Executes trade legs sequentially with configurable delays between them.

    This approach reduces the risk of single-sided executions in volatile markets
    by allowing time to confirm one leg before executing the next.
    """

    def __init__(self, config: Dict):
        super().__init__(config)
        self.logger = logging.getLogger("execution.staggered")
        self.timeout = config.get("timeout", 5000)  # ms (longer default than parallel)
        self.stagger_delay = config.get("stagger_delay", 500)  # ms delay between legs
        self.abort_on_first_failure = config.get("abort_on_first_failure", True)
        self.leg_order = config.get("leg_order", "buy_first")  # or "sell_first"
        self.retry_attempts = config.get("retry_attempts", 1)
        self.retry_delay = config.get("retry_delay", 200)  # ms delay between retries

    async def execute(self, opportunity: Any, position_size: float) -> TradeResult:
        """Execute trade legs in sequence with delays between them"""
        self.logger.info(f"Executing staggered trade for {opportunity} with size {position_size}")
        start_time = time.time()

        try:
            # Determine execution order
            if self.leg_order == "sell_first":
                first_leg = ("sell", self._execute_sell, opportunity.sell_exchange, opportunity.sell_price)
                second_leg = ("buy", self._execute_buy, opportunity.buy_exchange, opportunity.buy_price)
            else:
                # Default to buy_first
                first_leg = ("buy", self._execute_buy, opportunity.buy_exchange, opportunity.buy_price)
                second_leg = ("sell", self._execute_sell, opportunity.sell_exchange, opportunity.sell_price)

            # Execute first leg
            leg_type, leg_func, exchange, price = first_leg
            self.logger.info(f"Executing first leg: {leg_type} on {exchange} at {price}")

            first_result = await self._execute_with_retry(
                leg_func, exchange=exchange, trading_pair=opportunity.trading_pair, price=price, volume=position_size
            )

            if isinstance(first_result, Exception) or not first_result.get("success", False):
                error_msg = str(first_result) if isinstance(first_result, Exception) else "Execution failed"
                self.logger.error(f"First leg ({leg_type}) failed: {error_msg}")
                return TradeResult(
                    success=False,
                    partial=False,
                    profit=0,
                    execution_time=(time.time() - start_time) * 1000,
                    error=f"First leg ({leg_type}) failed: {error_msg}",
                    details={f"{leg_type}_result": first_result if not isinstance(first_result, Exception) else None},
                )

            # Wait for configured delay before executing second leg
            self.logger.info(f"First leg successful, waiting {self.stagger_delay}ms before executing second leg")
            await asyncio.sleep(self.stagger_delay / 1000)

            # Check if we've exceeded timeout
            current_time = time.time()
            if (current_time - start_time) * 1000 > self.timeout:
                self.logger.error("Timeout reached after first leg execution")
                # Handle partial execution
                return TradeResult(
                    success=False,
                    partial=True,
                    profit=0,
                    execution_time=(current_time - start_time) * 1000,
                    error="Timeout after first leg execution",
                    details={f"{leg_type}_result": first_result},
                )

            # Execute second leg
            leg_type, leg_func, exchange, price = second_leg
            self.logger.info(f"Executing second leg: {leg_type} on {exchange} at {price}")

            second_result = await self._execute_with_retry(
                leg_func, exchange=exchange, trading_pair=opportunity.trading_pair, price=price, volume=position_size
            )

            if isinstance(second_result, Exception) or not second_result.get("success", False):
                error_msg = str(second_result) if isinstance(second_result, Exception) else "Execution failed"
                self.logger.error(f"Second leg ({leg_type}) failed: {error_msg}")

                # Set first leg details based on leg order
                first_leg_type = "buy" if leg_type == "sell" else "sell"

                return TradeResult(
                    success=False,
                    partial=True,
                    profit=0,
                    execution_time=(time.time() - start_time) * 1000,
                    error=f"Second leg ({leg_type}) failed: {error_msg}",
                    details={
                        f"{first_leg_type}_result": first_result,
                        f"{leg_type}_result": second_result if not isinstance(second_result, Exception) else None,
                    },
                )

            # Both legs succeeded
            execution_time = (time.time() - start_time) * 1000

            # Set results based on leg order
            if self.leg_order == "sell_first":
                sell_result = first_result
                buy_result = second_result
            else:
                buy_result = first_result
                sell_result = second_result

            # Calculate profit
            buy_cost = buy_result.get("executed_price", opportunity.buy_price) * position_size
            sell_revenue = sell_result.get("executed_price", opportunity.sell_price) * position_size
            profit = sell_revenue - buy_cost

            self.logger.info(f"Staggered execution complete. Profit: {profit}, Time: {execution_time}ms")

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

    async def _execute_with_retry(self, leg_func, **kwargs) -> Dict:
        """Execute a leg with retry logic"""
        attempt = 0
        last_error = None

        while attempt <= self.retry_attempts:
            try:
                if attempt > 0:
                    self.logger.info(f"Retry attempt {attempt}/{self.retry_attempts}")
                    await asyncio.sleep(self.retry_delay / 1000)

                result = await leg_func(**kwargs)
                if result.get("success", False):
                    return result

                last_error = result.get("error", "Unknown execution error")
                self.logger.warning(f"Execution attempt {attempt} failed: {last_error}")

            except Exception as e:
                last_error = str(e)
                self.logger.error(f"Error in execution attempt {attempt}: {e}")

            attempt += 1

        return {"success": False, "error": last_error, "attempts": attempt}

    async def handle_partial_execution(self, result: TradeResult) -> None:
        """Handle partial execution by attempting to rebalance positions"""
        self.logger.warning("Handling partial execution in staggered trade")

        details = result.details or {}
        buy_result = details.get("buy_result", {})
        sell_result = details.get("sell_result", {})

        # Determine which side executed and which failed
        if buy_result and buy_result.get("success") and (not sell_result or not sell_result.get("success")):
            # Buy succeeded but sell failed - we need to sell what we bought
            await self._cleanup_partial_buy(buy_result)

        elif sell_result and sell_result.get("success") and (not buy_result or not buy_result.get("success")):
            # Sell succeeded but buy failed - we need to buy to cover
            await self._cleanup_partial_sell(sell_result)

    async def handle_failure(self, result: TradeResult) -> None:
        """Handle complete failure of trade execution"""
        self.logger.error(f"Staggered trade failed: {result.error}")
        # Implement retry or circuit breaker logic if needed

    async def _execute_buy(self, exchange: str, trading_pair: str, price: float, volume: float) -> Dict:
        """Execute a buy order"""
        # Use the execution service instead of direct exchange service
        from src.arbirich.services.execution.execution_service import ExecutionService

        execution_service = ExecutionService(method_type="parallel")
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
        from src.arbirich.services.execution.execution_service import ExecutionService

        execution_service = ExecutionService(method_type="parallel")
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

    async def _cleanup_partial_buy(self, buy_result: Dict) -> None:
        """Clean up after a partial execution where only buy succeeded"""
        # Sell what we bought to minimize risk
        exchange = buy_result.get("exchange")
        trading_pair = buy_result.get("trading_pair")
        volume = buy_result.get("executed_volume", 0)

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

    async def _cleanup_partial_sell(self, sell_result: Dict) -> None:
        """Clean up after a partial execution where only sell succeeded"""
        # Buy to cover what we sold
        exchange = sell_result.get("exchange")
        trading_pair = sell_result.get("trading_pair")
        volume = sell_result.get("executed_volume", 0)

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
