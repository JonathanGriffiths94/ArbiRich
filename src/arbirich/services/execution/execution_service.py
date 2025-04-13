import asyncio
import logging
from typing import Any, Dict, Union

from src.arbirich.core.trading.strategy.execution.method import ExecutionMethod, TradeResult
from src.arbirich.core.trading.strategy.execution.parallel import ParallelExecution
from src.arbirich.core.trading.strategy.execution.staggered import StaggeredExecution


class ExecutionService:
    """
    Service responsible for executing trades across different exchanges.
    """

    def __init__(self, method_type=None, config=None):
        """
        Initialize the execution service.

        Args:
            method_type (str, optional): Type of execution method to use (e.g. 'parallel', 'staggered')
            config (Dict, optional): Configuration for the execution service
        """
        self.logger = logging.getLogger(__name__)
        self.exchange_clients = {}
        self.method_type = method_type or "parallel"
        self.config = config or {}
        self.execution_method = None

    async def initialize(self):
        """
        Initialize the execution service and its connections to exchanges.
        """
        self.logger.info(f"Initializing ExecutionService with method: {self.method_type}")

        # Initialize the appropriate execution method
        self.execution_method = self._create_execution_method(self.method_type, self.config)

        self.logger.info(f"ExecutionService initialized with {self.method_type} method")

    def _create_execution_method(self, method_type: str, config: Dict) -> ExecutionMethod:
        """
        Create and return the appropriate execution method based on the method type.

        Args:
            method_type: The type of execution method to create
            config: Configuration for the execution method

        Returns:
            An instance of the requested execution method
        """
        if method_type == "parallel":
            return ParallelExecution(config)
        elif method_type == "staggered":
            return StaggeredExecution(config)
        else:
            self.logger.warning(f"Unknown execution method type: {method_type}. Defaulting to parallel.")
            return ParallelExecution(config)

    async def execute_trade(
        self, trade_data: Union[Dict[str, Any], Any], position_size: float = None
    ) -> Dict[str, Any]:
        """
        Execute a trade based on the provided trade data or opportunity.

        Args:
            trade_data: Either a dictionary with trade information or a trade opportunity object
            position_size: The size/volume of the position to take (may be in trade_data if it's a dict)

        Returns:
            Dict[str, Any]: Result of the trade execution
        """
        # Handle different input formats for backwards compatibility
        opportunity = None
        if isinstance(trade_data, dict):
            self.logger.info(f"Executing trade with legacy format: {trade_data}")

            # Legacy format - extract data for the execution method
            exchange = trade_data.get("exchange")
            symbol = trade_data.get("symbol")
            side = trade_data.get("side")
            amount = trade_data.get("amount")
            price = trade_data.get("price")

            # If using legacy format and no execution method, simulate the old behavior
            if not self.execution_method:
                return {
                    "success": True,
                    "order_id": f"mock-order-{exchange}-{symbol}",
                    "timestamp": asyncio.get_event_loop().time(),
                    "trade_data": trade_data,
                    "method": self.method_type,
                }

            # Convert legacy format to opportunity-like object
            opportunity = type(
                "OpportunityObject",
                (),
                {
                    "buy_exchange": exchange if side == "buy" else None,
                    "sell_exchange": exchange if side == "sell" else None,
                    "buy_price": price if side == "buy" else None,
                    "sell_price": price if side == "sell" else None,
                    "trading_pair": symbol,
                    "volume": amount,
                },
            )

            # Use provided amount as position size if none was explicitly provided
            if position_size is None:
                position_size = amount
        else:
            # New format - trade_data is the opportunity object
            opportunity = trade_data
            self.logger.info(f"Executing trade for opportunity: {opportunity}")

            # If position_size is still None, try to get it from the opportunity
            if position_size is None and hasattr(opportunity, "volume"):
                position_size = opportunity.volume

        # Ensure we have both opportunity and position_size
        if not opportunity:
            self.logger.error("No valid opportunity provided")
            return {
                "success": False,
                "error": "No valid opportunity provided",
                "trade_data": trade_data,
            }

        if not position_size:
            self.logger.error("No position size provided or available in opportunity")
            return {
                "success": False,
                "error": "No position size provided",
                "opportunity": opportunity,
            }

        if not self.execution_method:
            self.logger.error("Execution method not initialized")
            return {
                "success": False,
                "error": "Execution method not initialized",
                "opportunity": opportunity,
            }

        try:
            # Execute the trade using the current execution method
            result = await self.execution_method.execute(opportunity, position_size)

            # Handle partial executions if needed
            if not result.success and result.partial:
                await self.execution_method.handle_partial_execution(result)

            # Handle complete failures if needed
            if not result.success and not result.partial:
                await self.execution_method.handle_failure(result)

            # Convert TradeResult to dict for easier handling
            return {
                "success": result.success,
                "partial": result.partial,
                "profit": result.profit,
                "execution_time": result.execution_time,
                "error": result.error,
                "details": result.details,
                "opportunity": opportunity,
                "position_size": position_size,
                "method": self.method_type,
            }
        except Exception as e:
            self.logger.error(f"Failed to execute trade: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "opportunity": opportunity,
                "position_size": position_size,
                "method": self.method_type,
            }

    async def handle_partial_execution(self, result: Dict[str, Any]) -> None:
        """
        Handle partial execution of a trade.

        Args:
            result: The result of the trade execution
        """
        if not self.execution_method:
            self.logger.error("Cannot handle partial execution: Execution method not initialized")
            return

        try:
            # Convert dict back to TradeResult for the execution method
            trade_result = TradeResult(
                success=result.get("success", False),
                partial=result.get("partial", False),
                profit=result.get("profit", 0),
                execution_time=result.get("execution_time", 0),
                error=result.get("error"),
                details=result.get("details", {}),
            )

            await self.execution_method.handle_partial_execution(trade_result)
        except Exception as e:
            self.logger.error(f"Error handling partial execution: {e}")

    async def handle_failure(self, result: Dict[str, Any]) -> None:
        """
        Handle complete failure of a trade execution.

        Args:
            result: The result of the trade execution
        """
        if not self.execution_method:
            self.logger.error("Cannot handle failure: Execution method not initialized")
            return

        try:
            # Convert dict back to TradeResult for the execution method
            trade_result = TradeResult(
                success=result.get("success", False),
                partial=result.get("partial", False),
                profit=result.get("profit", 0),
                execution_time=result.get("execution_time", 0),
                error=result.get("error"),
                details=result.get("details", {}),
            )

            await self.execution_method.handle_failure(trade_result)
        except Exception as e:
            self.logger.error(f"Error handling execution failure: {e}")
