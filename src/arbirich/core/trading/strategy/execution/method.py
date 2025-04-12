from abc import ABC, abstractmethod
from typing import Any, Dict, NamedTuple


class TradeResult(NamedTuple):
    """Container for trade execution results"""

    success: bool
    partial: bool
    profit: float
    execution_time: float  # in milliseconds
    error: str = None
    details: Dict = None


class ExecutionMethod(ABC):
    """Base class for different trade execution methods"""

    def __init__(self, config: Dict):
        self.config = config

    @abstractmethod
    async def execute(self, opportunity: Any, position_size: float) -> TradeResult:
        """
        Execute a trade based on an arbitrage opportunity

        Args:
            opportunity: The opportunity object
            position_size: The size of the position to take

        Returns:
            TradeResult with execution details
        """
        pass

    @abstractmethod
    async def handle_partial_execution(self, result: TradeResult) -> None:
        """
        Handle cases where only part of a trade was executed

        Args:
            result: The result of the partially executed trade
        """
        pass

    @abstractmethod
    async def handle_failure(self, result: TradeResult) -> None:
        """
        Handle cases where a trade failed to execute

        Args:
            result: The result of the failed trade
        """
        pass
