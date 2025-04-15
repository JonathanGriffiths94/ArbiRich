import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import Dict

from src.arbirich.models.models import OrderBookState, TradeExecution, TradeOpportunity

logger = logging.getLogger(__name__)


class ArbitrageType(ABC):
    """Base class for different arbitrage detection and analysis strategies"""

    def __init__(self, config: Dict):
        self.config = config

        # Initialize risk management component
        from ..parameters.configuration import ConfigurationParameters
        from ..risk.management import RiskManagement

        # Initialize risk management with default config if not provided
        risk_config = config.get("risk_management", {})
        self.risk_management = RiskManagement(risk_config)

        # Initialize config parameters
        config_params = config.get("configuration", {})
        self.config_params = ConfigurationParameters(config_params)

    @abstractmethod
    def detect_opportunities(self, order_books: OrderBookState) -> Dict[str, TradeOpportunity]:
        """
        Detect arbitrage opportunities from order book data

        Args:
            order_books: Dictionary of order books by exchange and trading pair

        Returns:
            Dictionary of opportunity objects keyed by symbol
        """
        pass

    @abstractmethod
    def validate_opportunity(self, opportunity: TradeOpportunity) -> bool:
        """
        Validate if an opportunity is actionable

        Args:
            opportunity: The opportunity object to validate

        Returns:
            True if the opportunity is valid, False otherwise
        """
        pass

    @abstractmethod
    def calculate_spread(self, opportunity: TradeOpportunity) -> float:
        """
        Calculate the expected profit or spread for an opportunity

        Args:
            opportunity: The opportunity object

        Returns:
            The spread as a decimal (e.g., 0.01 for 1%)
        """
        pass

    def _get_or_create_event_loop(self):
        """Get the current event loop or create a new one if none exists."""
        try:
            return asyncio.get_event_loop()
        except RuntimeError:
            # Create new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop

    def execute_trade(self, opportunity: TradeOpportunity, position_size: float = None) -> TradeExecution:
        """
        Execute a trade based on the opportunity using the configured execution method.

        Args:
            opportunity: The trade opportunity to execute
            position_size: Optional position size override (if None, uses opportunity volume)

        Returns:
            TradeExecution object with the execution results
        """

        # Validate the opportunity
        if not self.validate_opportunity(opportunity):
            logger.warning(f"Opportunity {opportunity.id} is no longer valid")
            return TradeExecution(
                id=opportunity.id,
                strategy=opportunity.strategy,
                pair=opportunity.pair,
                buy_exchange=opportunity.buy_exchange,
                sell_exchange=opportunity.sell_exchange,
                executed_buy_price=0.0,
                executed_sell_price=0.0,
                spread=0.0,
                volume=0.0,
                execution_timestamp=time.time(),
                success=False,
                error="Opportunity is no longer valid",
            )

        # Determine position size
        if position_size is None:
            try:
                position_size = self.risk_management.calculate_position_size(opportunity, self.config_params)
            except AttributeError as e:
                logger.error(f"Error calculating position size: {e}")
                # Fall back to opportunity volume if risk management fails
                position_size = opportunity.volume
                if not position_size:
                    return TradeExecution(
                        id=opportunity.id,
                        strategy=opportunity.strategy,
                        pair=opportunity.pair,
                        buy_exchange=opportunity.buy_exchange,
                        sell_exchange=opportunity.sell_exchange,
                        executed_buy_price=0.0,
                        executed_sell_price=0.0,
                        spread=0.0,
                        volume=0.0,
                        execution_timestamp=time.time(),
                        success=False,
                        error="Unable to determine position size",
                    )

        if position_size <= 0:
            logger.warning(f"Invalid position size: {position_size}")
            return TradeExecution(
                id=opportunity.id,
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
                error="Invalid position size",
            )

        # Determine which execution method to use from config
        execution_method_type = self.config.get("execution_method", "parallel")
        logger.info(f"Using {execution_method_type} execution method")

        # Create execution service
        try:
            from src.arbirich.services.execution.execution_service import ExecutionService

            # Create and initialize execution service
            execution_service = ExecutionService(method_type=execution_method_type, config=self.config)

            # Run the initialization in the event loop
            loop = self._get_or_create_event_loop()
            if loop.is_running():
                future = asyncio.ensure_future(execution_service.initialize())
                loop.run_until_complete(future)
            else:
                loop.run_until_complete(execution_service.initialize())

            # Execute the trade
            result = None
            if loop.is_running():
                future = asyncio.ensure_future(execution_service.execute_trade(opportunity, position_size))
                result = loop.run_until_complete(future)
            else:
                result = loop.run_until_complete(execution_service.execute_trade(opportunity, position_size))

            if isinstance(result, TradeExecution):
                return result
            else:
                logger.error(f"Execution service returned unexpected type: {type(result)}")
                return TradeExecution(
                    id=opportunity.id,
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
                    error="Unexpected result type from execution service",
                    opportunity_id=opportunity.id,
                )

        except Exception as e:
            logger.error(f"Error executing trade: {e}", exc_info=True)
            return TradeExecution(
                id=opportunity.id,
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
                error=str(e),
                opportunity_id=opportunity.id,
            )
