import abc
import logging
from typing import Dict, Optional

from src.arbirich.models.models import TradeExecution, TradeOpportunity

logger = logging.getLogger(__name__)


class ExecutionStrategy(abc.ABC):
    """
    Abstract base class for all execution strategies.
    Each strategy implements its own logic for executing trades based on opportunities.
    """

    def __init__(self, name: str, config: dict):
        """
        Initialize the execution strategy.

        Parameters:
            name: The strategy name
            config: Configuration parameters for the strategy
        """
        self.name = name
        self.config = config
        self.risk_limit = config.get("risk_limit", 0.05)  # Default 5% of capital
        self.max_slippage = config.get("max_slippage", 0.001)  # Default 0.1% max slippage
        self.min_profit = config.get("min_profit", 0.0005)  # Default 0.05% min profit
        self.execution_timeout = config.get("execution_timeout", 5.0)  # Default 5 seconds timeout
        self.pairs = config.get("pairs", [])
        self.exchanges = config.get("exchanges", [])

        # Track state if needed
        self.active_executions: Dict[str, TradeExecution] = {}

        logger.info(f"Initialized execution strategy '{name}' with risk limit {self.risk_limit:.2%}")

    @abc.abstractmethod
    def should_execute(self, opportunity: TradeOpportunity) -> bool:
        """
        Determine if an opportunity should be executed based on strategy criteria.

        Parameters:
            opportunity: The trade opportunity to evaluate

        Returns:
            True if the opportunity should be executed, False otherwise
        """
        pass

    @abc.abstractmethod
    async def execute_trade(self, opportunity: TradeOpportunity) -> Optional[TradeExecution]:
        """
        Execute a trade based on the opportunity.

        Parameters:
            opportunity: The trade opportunity to execute

        Returns:
            TradeExecution if successful, None if execution failed
        """
        pass

    @abc.abstractmethod
    async def cancel_trade(self, execution_id: str) -> bool:
        """
        Cancel an ongoing trade execution.

        Parameters:
            execution_id: ID of the execution to cancel

        Returns:
            True if successfully cancelled, False otherwise
        """
        pass
