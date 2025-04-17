import logging
import time
import uuid
from typing import ClassVar, Dict, Literal, Optional, Union

from src.arbirich.core.strategy.execution.method import ExecutionMethod
from src.arbirich.core.strategy.execution.parallel import ParallelExecution
from src.arbirich.core.strategy.execution.staggered import StaggeredExecution
from src.arbirich.models import BaseExecutionConfig, TradeExecution, TradeOpportunity


class ExecutionService:
    """
    Service responsible for executing trades across different exchanges.
    """

    # Class variable to store instance registry
    _instances: ClassVar[Dict[str, "ExecutionService"]] = {}
    _initialization_lock = False

    @classmethod
    def get_instance(
        cls, method_type: Literal["parallel", "staggered"] = "parallel", config: Union[Dict, BaseExecutionConfig] = None
    ) -> "ExecutionService":
        """
        Get or create an ExecutionService instance (singleton pattern).

        Args:
            method_type: Type of execution method to use
            config: Configuration for the execution service

        Returns:
            Shared ExecutionService instance for the specified method type
        """
        # Create a key that uniquely identifies this configuration
        if isinstance(config, dict):
            key = f"{method_type}"
        else:
            key = f"{method_type}"

        # Return existing instance if available
        if key in cls._instances:
            return cls._instances[key]

        # Create new instance
        instance = cls(method_type=method_type, config=config)
        cls._instances[key] = instance
        return instance

    @classmethod
    def reset_instances(cls):
        """Clear all cached ExecutionService instances"""
        cls._instances.clear()

    def __init__(
        self,
        method_type: Literal["parallel", "staggered"] = "parallel",
        config: Union[Dict, BaseExecutionConfig] = None,
    ):
        """
        Initialize the execution service.

        Args:
            method_type: Type of execution method to use (parallel or staggered)
            config: Configuration for the execution service (dict will be converted to ExecutionConfig)
        """
        self.logger = logging.getLogger(__name__)
        self.method_type = method_type

        # Always convert config to ExecutionConfig if it's a dictionary
        if isinstance(config, dict):
            self.config = BaseExecutionConfig(**config)
        else:
            self.config = config or BaseExecutionConfig()  # Use default config if none provided

        self.execution_method: Optional[ExecutionMethod] = None
        self._initialized = False

    async def initialize(self) -> None:
        """
        Initialize the execution service and its connections to exchanges.
        Only initializes once, even if called multiple times.
        """
        if self._initialized:
            return

        self.logger.info(f"🔧 Initializing ExecutionService with method: {self.method_type}")

        try:
            config_dict = self.config.model_dump()

            self.execution_method = self._create_execution_method(self.method_type, config_dict)
            self._initialized = True
            self.logger.info(f"🟢 ExecutionService initialized with {self.method_type} method")
        except Exception as e:
            self.logger.error(f"Error initializing execution service: {e}")
            raise

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
            self.logger.warning(f"⚠️ Unknown execution method type: {method_type}. Defaulting to parallel.")
            return ParallelExecution(config)

    async def execute_trade(
        self, trade_data: Union[Dict, TradeOpportunity], position_size: Optional[float] = None
    ) -> TradeExecution:
        """
        Execute a trade based on the provided trade opportunity.

        Args:
            trade_data: Trade opportunity or dictionary convertible to TradeOpportunity
            position_size: The size/volume of the position to take

        Returns:
            TradeExecution: Result of the trade execution
        """
        # Convert dictionary to TradeOpportunity if needed
        if isinstance(trade_data, dict):
            self.logger.info("🔄 Converting dictionary to TradeOpportunity")
            # Try to convert the dictionary to a TradeOpportunity
            try:
                trade_data = TradeOpportunity(**trade_data)
            except Exception as e:
                error_msg = f"Failed to convert dictionary to TradeOpportunity: {str(e)}"
                self.logger.error(f"❌ {error_msg}")
                return self._create_error_execution(
                    str(uuid.uuid4()),
                    trade_data.get("strategy", "unknown"),
                    trade_data.get("pair", "unknown"),
                    trade_data.get("buy_exchange", ""),
                    trade_data.get("sell_exchange", ""),
                    position_size or trade_data.get("volume", 0.0),
                    error_msg,
                )

        # At this point, trade_data should be a TradeOpportunity
        if not isinstance(trade_data, TradeOpportunity):
            error_msg = f"Unsupported trade data type: {type(trade_data)}, expected TradeOpportunity"
            self.logger.error(f"❌ {error_msg}")
            return self._create_error_execution(
                str(uuid.uuid4()),
                "unknown",
                "unknown",
                "",
                "",
                position_size or 0.0,
                error_msg,
            )

        opportunity = trade_data
        self.logger.info(f"🎯 Executing trade for opportunity: {opportunity.id} on {opportunity.pair}")

        # Get position size from opportunity if not provided
        if position_size is None:
            position_size = opportunity.volume

        if not self.execution_method:
            self.logger.error("❌ Execution method not initialized")
            return self._create_error_execution(
                opportunity.id,
                opportunity.strategy,
                opportunity.pair,
                opportunity.buy_exchange,
                opportunity.sell_exchange,
                position_size,
                "Execution method not initialized",
            )

        try:
            # Execute the trade using the current execution method
            result: TradeExecution = await self.execution_method.execute(opportunity, position_size)

            # Handle partial executions if needed
            if not result.success and result.partial:
                self.logger.info(f"⚠️ Handling partial execution for trade {result.id}")
                await self.execution_method.handle_partial_execution(result)

            # Handle complete failures if needed
            if not result.success and not result.partial:
                self.logger.warning(f"❌ Handling complete failure for trade {result.id}")
                await self.execution_method.handle_failure(result)

            return result

        except Exception as e:
            self.logger.error(f"❌ Failed to execute trade: {str(e)}")
            return self._create_error_execution(
                opportunity.id,
                opportunity.strategy,
                opportunity.pair,
                opportunity.buy_exchange,
                opportunity.sell_exchange,
                position_size,
                str(e),
            )

    def _create_error_execution(
        self, id: str, strategy: str, pair: str, buy_exchange: str, sell_exchange: str, volume: float, error: str
    ) -> TradeExecution:
        """Create a TradeExecution object representing an error"""
        return TradeExecution(
            id=id,
            strategy=strategy,
            pair=pair,
            buy_exchange=buy_exchange,
            sell_exchange=sell_exchange,
            executed_buy_price=0.0,
            executed_sell_price=0.0,
            spread=0.0,
            volume=volume,
            execution_timestamp=time.time(),
            success=False,
            partial=False,
            profit=0.0,
            execution_time=0.0,
            error=error,
            opportunity_id=id,
            details={},
        )

    async def handle_partial_execution(self, result: TradeExecution) -> None:
        """
        Handle partial execution of a trade.

        Args:
            result: The result of the trade execution
        """
        if not self.execution_method:
            self.logger.error("❌ Cannot handle partial execution: Execution method not initialized")
            return

        try:
            await self.execution_method.handle_partial_execution(result)
        except Exception as e:
            self.logger.error(f"❌ Error handling partial execution: {e}")

    async def handle_failure(self, result: TradeExecution) -> None:
        """
        Handle complete failure of a trade execution.

        Args:
            result: The result of the trade execution
        """
        if not self.execution_method:
            self.logger.error("❌ Cannot handle failure: Execution method not initialized")
            return

        try:
            await self.execution_method.handle_failure(result)
        except Exception as e:
            self.logger.error(f"❌ Error handling failure: {e}")

    @classmethod
    async def execute_opportunity(
        cls, opportunity: TradeOpportunity, position_size: Optional[float] = None
    ) -> TradeExecution:
        """
        Static helper method to directly execute a trade opportunity.
        Gets or creates an execution service instance and handles initialization.

        Args:
            opportunity: The opportunity to execute
            position_size: Optional position size (will use opportunity.volume if None)

        Returns:
            TradeExecution with the execution results
        """
        logger = logging.getLogger(__name__)

        try:
            # Get the singleton instance
            service = cls.get_instance()

            # Initialize if needed
            if not service._initialized:
                await service.initialize()

            # Execute the trade
            return await service.execute_trade(opportunity, position_size)

        except Exception as e:
            logger.error(f"Error executing opportunity via static method: {e}", exc_info=True)

            # Create and return an error execution result
            error_result = TradeExecution(
                id=str(uuid.uuid4()),
                strategy=opportunity.strategy,
                pair=opportunity.pair,
                buy_exchange=opportunity.buy_exchange or "",
                sell_exchange=opportunity.sell_exchange or "",
                executed_buy_price=0.0,
                executed_sell_price=0.0,
                spread=0.0,
                volume=position_size or opportunity.volume or 0.0,
                execution_timestamp=time.time(),
                success=False,
                partial=False,
                profit=0.0,
                execution_time=0.0,
                error=f"Execution service error: {str(e)}",
                opportunity_id=opportunity.id,
                details={"error": str(e)},
            )

            return error_result
