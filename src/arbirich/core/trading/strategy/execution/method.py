from abc import ABC, abstractmethod
from typing import Dict

from src.arbirich.models.models import TradeExecution, TradeOpportunity


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
