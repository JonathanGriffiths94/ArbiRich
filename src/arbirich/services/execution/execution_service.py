import logging
import time
import uuid
from typing import ClassVar, Dict, Literal, Optional, Union

from src.arbirich.core.trading.strategy.execution.method import ExecutionMethod
from src.arbirich.core.trading.strategy.execution.parallel import ParallelExecution
from src.arbirich.core.trading.strategy.execution.staggered import StaggeredExecution
from src.arbirich.models.config_models import ExecutionConfig
from src.arbirich.models.enums import OrderType
from src.arbirich.models.models import TradeExecution, TradeOpportunity, TradeRequest


class ExecutionService:
    """
    Service responsible for executing trades across different exchanges.
    """

    # Class variable to store instance registry
    _instances: ClassVar[Dict[str, "ExecutionService"]] = {}
    _initialization_lock = False

    @classmethod
    def get_instance(
        cls, method_type: Literal["parallel", "staggered"] = "parallel", config: Union[Dict, ExecutionConfig] = None
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
        self, method_type: Literal["parallel", "staggered"] = "parallel", config: Union[Dict, ExecutionConfig] = None
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
            self.config = ExecutionConfig(**config)
        else:
            self.config = config or ExecutionConfig()  # Use default config if none provided

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
            # Always use the Pydantic model's model_dump or dict method
            if hasattr(self.config, "model_dump"):
                # Newer Pydantic v2 model
                config_dict = self.config.model_dump()
            elif hasattr(self.config, "dict"):
                # Older Pydantic model
                config_dict = self.config.dict()
            else:
                # Should never happen since we ensure self.config is a Pydantic model in __init__
                config_dict = {}
                self.logger.warning(f"Config object has no dict() or model_dump() method: {type(self.config)}")

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
        self, trade_data: Union[TradeRequest, Dict, TradeOpportunity], position_size: Optional[float] = None
    ) -> TradeExecution:
        """
        Execute a trade based on the provided trade data or opportunity.

        Args:
            trade_data: Trade request model, dictionary, or TradeOpportunity
            position_size: The size/volume of the position to take

        Returns:
            TradeExecution: Result of the trade execution
        """
        # Handle different input formats
        opportunity: TradeOpportunity

        if isinstance(trade_data, TradeRequest):
            from src.arbirich.models.enums import OrderSide

            if trade_data.side == OrderSide.BUY:
                opportunity = TradeOpportunity(
                    id=trade_data.execution_id or str(uuid.uuid4()),
                    strategy=trade_data.strategy,
                    pair=trade_data.symbol,
                    buy_exchange=trade_data.exchange,
                    sell_exchange="",  # Not applicable for single-exchange trades
                    buy_price=trade_data.price or 0.0,
                    sell_price=0.0,  # Not applicable
                    spread=0.0,  # Not applicable
                    volume=trade_data.amount,
                    opportunity_timestamp=time.time(),
                )
            else:  # sell
                opportunity = TradeOpportunity(
                    id=trade_data.execution_id or str(uuid.uuid4()),
                    strategy=trade_data.strategy,
                    pair=trade_data.symbol,
                    buy_exchange="",  # Not applicable for single-exchange trades
                    sell_exchange=trade_data.exchange,
                    buy_price=0.0,  # Not applicable
                    sell_price=trade_data.price or 0.0,
                    spread=0.0,  # Not applicable
                    volume=trade_data.amount,
                    opportunity_timestamp=time.time(),
                )

            # Use provided amount as position size if none was explicitly provided
            if position_size is None:
                position_size = trade_data.amount
        elif isinstance(trade_data, dict):
            # For backward compatibility, convert dict to TradeRequest
            trade_request = TradeRequest(
                exchange=trade_data["exchange"],
                symbol=trade_data["symbol"],
                side=trade_data["side"],
                price=trade_data.get("price"),
                amount=trade_data["amount"],
                order_type=trade_data.get("order_type", OrderType.LIMIT),
                strategy=trade_data.get("strategy", "legacy"),
            )
            return await self.execute_trade(trade_request, position_size)
        else:
            # TradeOpportunity object
            opportunity = trade_data
            self.logger.info(f"🎯 Executing trade for opportunity: {opportunity.id} on {opportunity.pair}")

            # Get position size from opportunity if not provided
            if position_size is None:
                position_size = opportunity.volume

        if not self.execution_method:
            self.logger.error("❌ Execution method not initialized")
            # Return empty trade execution with error
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
                error="Execution method not initialized",
                opportunity_id=opportunity.id,
                details={},
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
            # Return error execution result
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
                error=str(e),
                opportunity_id=opportunity.id,
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
