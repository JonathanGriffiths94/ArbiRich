from abc import ABC, abstractmethod
from typing import Dict, List

from src.arbirich.models.enums import OrderSide, OrderType
from src.arbirich.models.models import TradeExecution, TradeOpportunity, TradeRequest


class ExecutionMethod(ABC):
    """Base class for different trade execution methods"""

    def __init__(self, config: Dict):
        self.config = config

    @abstractmethod
    async def execute(self, opportunity: TradeOpportunity, position_size: float) -> TradeExecution:
        """
        Execute a trade based on an arbitrage opportunity

        Args:
            opportunity: The opportunity object
            position_size: The size of the position to take

        Returns:
            TradeExecution with execution details
        """
        pass

    def opportunity_to_trade_requests(self, opportunity: TradeOpportunity, position_size: float) -> List[TradeRequest]:
        """
        Convert a TradeOpportunity to a list of TradeRequest objects.
        This helper method can be used by implementations of the execute method.

        Args:
            opportunity: The trade opportunity to convert
            position_size: The size/volume to use

        Returns:
            List of TradeRequest objects
        """
        requests = []

        # Create buy request if applicable
        if opportunity.buy_exchange:
            buy_request = TradeRequest(
                exchange=opportunity.buy_exchange,
                symbol=opportunity.pair,
                side=OrderSide.BUY,
                price=opportunity.buy_price,
                amount=position_size,
                order_type=OrderType.LIMIT,
                strategy=opportunity.strategy,
                execution_id=opportunity.id,
            )
            requests.append(buy_request)

        # Create sell request if applicable
        if opportunity.sell_exchange:
            sell_request = TradeRequest(
                exchange=opportunity.sell_exchange,
                symbol=opportunity.pair,
                side=OrderSide.SELL,
                price=opportunity.sell_price,
                amount=position_size,
                order_type=OrderType.LIMIT,
                strategy=opportunity.strategy,
                execution_id=opportunity.id,
            )
            requests.append(sell_request)

        return requests

    @abstractmethod
    async def handle_partial_execution(self, result: TradeExecution) -> None:
        """
        Handle cases where only part of a trade was executed

        Args:
            result: The result of the partially executed trade
        """
        pass

    @abstractmethod
    async def handle_failure(self, result: TradeExecution) -> None:
        """
        Handle cases where a trade failed to execute

        Args:
            result: The result of the failed trade
        """
        pass
