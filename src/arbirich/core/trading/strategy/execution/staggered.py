import asyncio
import logging
import time
import uuid
from typing import Dict, List

from src.arbirich.core.trading.strategy.execution.method import ExecutionMethod
from src.arbirich.models.enums import OrderSide
from src.arbirich.models.models import TradeExecution, TradeOpportunity, TradeRequest


class StaggeredExecution(ExecutionMethod):
    """Execute trades in a staggered sequence across exchanges"""

    def __init__(self, config: Dict):
        super().__init__(config)
        self.logger = logging.getLogger(__name__)
        # Get staggered execution specific config
        self.execution_order = config.get("execution_order", "buy_first")  # buy_first or sell_first
        self.delay_between_orders = config.get("delay_between_orders", 0.5)  # seconds

    async def execute(self, opportunity: TradeOpportunity, position_size: float) -> TradeExecution:
        """
        Execute buy and sell orders in a staggered sequence

        Args:
            opportunity: The trade opportunity
            position_size: Size of position to execute

        Returns:
            TradeExecution object with execution results
        """
        self.logger.info(f"Executing staggered trade for {opportunity.id} on {opportunity.pair}")
        start_time = time.time()

        # Convert opportunity to trade requests
        trade_requests = self.opportunity_to_trade_requests(opportunity, position_size)

        if not trade_requests:
            return TradeExecution(
                id=str(uuid.uuid4()),
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
                profit=0.0,
                execution_time=0.0,
                error="No valid trade requests generated from opportunity",
                opportunity_id=opportunity.id,
                details={},
            )

        # Sort trade requests based on execution order
        sorted_requests = self._sort_requests_by_execution_order(trade_requests)

        # Execute requests in sequence
        results = []
        abort_execution = False

        for request in sorted_requests:
            if abort_execution:
                self.logger.warning("Aborting remaining executions due to previous failure")
                break

            result = await self._execute_single_request(request)
            results.append(result)

            # Check if we should continue with next trade
            if not result.get("success", False) and self.config.get("abort_on_failure", True):
                abort_execution = True
                self.logger.warning(f"Execution aborted due to failure: {result.get('error', 'Unknown error')}")

            # Add delay between orders if specified
            if self.delay_between_orders > 0 and not abort_execution and len(sorted_requests) > 1:
                await asyncio.sleep(self.delay_between_orders)

        # Process execution results
        return self._process_execution_results(
            opportunity=opportunity, position_size=position_size, results=results, start_time=start_time
        )

    def _sort_requests_by_execution_order(self, requests: List[TradeRequest]) -> List[TradeRequest]:
        """Sort trade requests based on execution order configuration"""
        if self.execution_order == "sell_first":
            # Sort with sell orders first
            return sorted(requests, key=lambda r: 0 if r.side == OrderSide.SELL else 1)
        else:
            # Default: buy orders first
            return sorted(requests, key=lambda r: 0 if r.side == OrderSide.BUY else 1)

    async def _execute_single_request(self, request: TradeRequest) -> Dict:
        """Execute a single trade request and return result dict"""
        try:
            # Implement the exchange-specific logic to execute the trade
            # This is a placeholder - you'll need to implement the actual trading logic
            self.logger.info(f"Executing {request.side.value} order on {request.exchange} for {request.symbol}")

            # For example, use an exchange service:
            # from src.arbirich.services.exchange_service import ExchangeService
            # exchange_service = await ExchangeService.get_instance(request.exchange)
            # result = await exchange_service.create_order(
            #     symbol=request.symbol,
            #     side=request.side,
            #     order_type=request.order_type,
            #     amount=request.amount,
            #     price=request.price
            # )

            # For now, simulate a successful execution
            return {
                "request": request.model_dump(),
                "success": True,
                "executed_price": request.price,
                "executed_amount": request.amount,
                "exchange": request.exchange,
                "symbol": request.symbol,
                "side": request.side.value,
                "order_id": f"simulated-{uuid.uuid4()}",
                "timestamp": time.time(),
            }

        except Exception as e:
            self.logger.error(f"Error executing trade request: {e}")
            return {
                "request": request.model_dump(),
                "success": False,
                "error": str(e),
                "exchange": request.exchange,
                "symbol": request.symbol,
                "side": request.side.value,
                "timestamp": time.time(),
            }

    def _process_execution_results(
        self, opportunity: TradeOpportunity, position_size: float, results: List[Dict], start_time: float
    ) -> TradeExecution:
        """Process execution results into a TradeExecution object"""
        executed_buy_price = 0.0
        executed_sell_price = 0.0
        buy_success = False
        sell_success = False
        errors = []

        # Process buy and sell results
        for result in results:
            if isinstance(result, Exception):
                errors.append(f"Exception: {str(result)}")
                continue

            if not result.get("success", False):
                errors.append(
                    f"{result.get('side', 'unknown')} on {result.get('exchange', 'unknown')}: {result.get('error', 'Unknown error')}"
                )
                continue

            if result.get("side") == "BUY":
                executed_buy_price = result.get("executed_price", 0.0)
                buy_success = True
            elif result.get("side") == "SELL":
                executed_sell_price = result.get("executed_price", 0.0)
                sell_success = True

        # Calculate spread and profit
        spread = executed_sell_price - executed_buy_price if executed_buy_price > 0 and executed_sell_price > 0 else 0.0
        profit = spread * position_size if spread > 0 else 0.0

        # Determine overall success
        success = (buy_success or not opportunity.buy_exchange) and (sell_success or not opportunity.sell_exchange)
        partial = not success and (buy_success or sell_success)

        return TradeExecution(
            id=str(uuid.uuid4()),
            strategy=opportunity.strategy,
            pair=opportunity.pair,
            buy_exchange=opportunity.buy_exchange,
            sell_exchange=opportunity.sell_exchange,
            executed_buy_price=executed_buy_price,
            executed_sell_price=executed_sell_price,
            spread=spread,
            volume=position_size,
            execution_timestamp=time.time(),
            success=success,
            partial=partial,
            profit=profit,
            execution_time=time.time() - start_time,
            error="; ".join(errors) if errors else None,
            opportunity_id=opportunity.id,
            details={"results": results},
        )

    async def handle_partial_execution(self, result: TradeExecution) -> None:
        """
        Handle cases where only part of a trade was executed

        Args:
            result: The result of the partially executed trade
        """
        self.logger.warning(f"Handling partial execution for trade {result.id}")
        # Implement recovery logic here (e.g., closing positions)
        # This is a placeholder - you'll need to implement actual recovery logic
        pass

    async def handle_failure(self, result: TradeExecution) -> None:
        """
        Handle cases where a trade failed to execute

        Args:
            result: The result of the failed trade
        """
        self.logger.error(f"Handling failure for trade {result.id}: {result.error}")
        # Implement failure handling logic here
        # This is a placeholder - you'll need to implement actual failure handling
        pass
