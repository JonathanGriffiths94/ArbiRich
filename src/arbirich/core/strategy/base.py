import logging
from typing import Dict


class ArbitrageStrategy:
    """Main Strategy Coordinator that orchestrates arbitrage operations"""

    def __init__(self, strategy_id: str, strategy_name: str, config: Dict):
        self.id = strategy_id
        self.name = strategy_name
        self.config = config
        self.logger = logging.getLogger(f"strategy.{strategy_name}")
        self.active = False
        self.task = None

        # Initialize core components
        from .parameters.configuration import ConfigurationParameters
        from .parameters.exchanges import ExchangeAndTradingPairs
        from .parameters.performance import PerformanceMetrics
        from .risk.management import RiskManagement

        # Create the risk management component
        self.risk_management = RiskManagement(config.get("risk_management", {}))

        # Create performance metrics tracker
        self.performance_metrics = PerformanceMetrics()

        # Create parameter objects
        self.config_params = ConfigurationParameters(config.get("configuration", {}))
        self.exchange_params = ExchangeAndTradingPairs(config.get("exchanges", {}))

        # The arbitrage type and execution method will be set by subclasses
        self.arbitrage_type = None
        self.execution_method = None
        self.risk_aware_execution = None  # New field for risk-aware execution

    def _initialize_execution_method(self):
        """Initialize the execution method based on configuration"""
        try:
            # Get execution config
            execution_config = self.config.get("execution", {})
            execution_method = execution_config.get("method", "parallel")

            # Import execution methods
            from src.arbirich.core.strategy.execution.parallel import ParallelExecution
            from src.arbirich.core.strategy.execution.risk_aware_execution import RiskAwareExecution
            from src.arbirich.core.strategy.execution.staggered import StaggeredExecution

            # Create the appropriate execution method
            if execution_method == "staggered":
                base_execution = StaggeredExecution(execution_config)
            else:
                base_execution = ParallelExecution(execution_config)

            self.execution_method = base_execution

            # Create risk-aware wrapper around the execution method
            self.risk_aware_execution = RiskAwareExecution(
                execution_method=base_execution, risk_management=self.risk_management, config_params=self.config_params
            )

            # Add custom hooks if needed
            if execution_config.get("add_default_hooks", True):
                self._add_default_execution_hooks()

            self.logger.info(f"Initialized risk-aware {execution_method} execution method")

        except Exception as e:
            self.logger.error(f"Error initializing execution method: {e}")
            # Set to None - will be caught during validation
            self.execution_method = None
            self.risk_aware_execution = None

    def _add_default_execution_hooks(self):
        """Add default execution hooks for common risk patterns"""
        if not self.risk_aware_execution:
            return

        # Example pre-execution hook: Time-of-day risk adjustment
        def time_of_day_risk(opportunity, position_size):
            import datetime

            hour = datetime.datetime.now().hour

            # Reduce position sizes during potentially volatile periods
            # (e.g., market opens, closes, low liquidity periods)
            if hour in [0, 1, 2, 3, 12, 13]:  # Example hours
                return {"position_size": position_size * 0.8}
            return None

        # Example post-execution hook: Log detailed execution metrics
        def log_execution_metrics(opportunity, result):
            if result.success:
                self.logger.info(
                    f"EXECUTION METRICS: "
                    f"Spread: {opportunity.spread:.6f}, "
                    f"Volume: {result.volume:.8f}, "
                    f"Profit: {result.profit:.8f}, "
                    f"Time: {result.execution_time:.2f}ms"
                )
            else:
                self.logger.warning(f"FAILED EXECUTION: Error: {result.error}, Partial: {result.partial}")

        # Add the hooks
        self.risk_aware_execution.add_pre_execution_hook(time_of_day_risk)
        self.risk_aware_execution.add_post_execution_hook(log_execution_metrics)

    async def execute_opportunity(self, opportunity, position_size=None):
        """
        Execute a trade for an opportunity using the strategy's execution method.

        Parameters:
            opportunity: The opportunity to execute
            position_size: Optional position size (will be calculated if not provided)

        Returns:
            Execution result
        """
        try:
            if self.risk_aware_execution:
                self.logger.info(f"Executing opportunity {opportunity.id} with risk-aware execution")
                return await self.risk_aware_execution.execute(opportunity, position_size)
            elif self.execution_method:
                # Calculate position size if not provided
                if position_size is None:
                    position_size = self.risk_management.calculate_position_size(opportunity, self.config_params)

                # Execute the trade
                self.logger.info(f"Executing opportunity {opportunity.id} with position size {position_size}")
                result = await self.execution_method.execute(opportunity, position_size)

                # Handle partial executions or failures
                if not result.success and result.partial:
                    await self.execution_method.handle_partial_execution(result)
                elif not result.success:
                    await self.execution_method.handle_failure(result)

                # Update performance metrics
                self.performance_metrics.update(result)

                return result
            else:
                self.logger.error("No execution method configured")
                return None

        except Exception as e:
            self.logger.error(f"Error executing opportunity: {e}", exc_info=True)
            return None
