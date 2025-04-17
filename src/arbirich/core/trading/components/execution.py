import asyncio
from typing import Any, Dict, List, Optional

from src.arbirich.models import Strategy
from src.arbirich.services.database.repositories.strategy_repository import StrategyRepository

from .base import Component


class ExecutionComponent(Component):
    """
    Execution component responsible for executing trades

    This component manages Bytewax flows for trade execution processes
    including opportunity validation and order placement.
    """

    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self.update_interval = config.get("update_interval", 1.0)  # seconds
        self.debug_mode = config.get("debug_mode", False)
        self.active_flows = {}
        self.strategies = {}
        self.flow_manager = None  # Initialize flow_manager attribute
        self.strategy_repository = None

    async def initialize(self) -> bool:
        """Initialize execution component with database strategies."""
        try:
            # Updated import paths to reflect new structure
            from src.arbirich.core.trading.flows.bytewax_flows.execution.execution_flow import build_execution_flow
            from src.arbirich.core.trading.flows.flow_manager import BytewaxFlowManager
            from src.arbirich.services.database.database_service import DatabaseService

            # Get a database connection from the service
            db_service = DatabaseService()

            # Create the strategy repository
            self.strategy_repository = StrategyRepository(engine=db_service.engine)

            # Get active strategies using the repository
            active_strategies = self._get_active_strategies()

            if not active_strategies:
                self.logger.warning("No active strategies found for execution component")
                # Return True anyway to allow component to start
                self.strategies = {}
                return True

            # Initialize strategies
            for strategy in active_strategies:
                strategy_id = str(strategy.id)
                strategy_name = strategy.name

                self.logger.info(f"Initializing execution component for strategy: {strategy_name}")

                # Create a flow ID and manager for this strategy
                flow_id = f"execution_{strategy_name}"
                flow_manager = BytewaxFlowManager.get_or_create(flow_id)

                # Configure the flow builder
                flow_manager.build_flow = lambda strat=strategy: build_execution_flow(
                    strategy_name=strat.name, debug_mode=self.debug_mode
                )

                # Store the flow manager (use the first one if multiple strategies)
                if self.flow_manager is None:
                    self.flow_manager = flow_manager

                # Add to active flows
                self.active_flows[flow_id] = {"strategy_id": strategy_id, "strategy_name": strategy_name}

                # Add strategy to the tracked strategies
                try:
                    # Extract strategy parameters for execution logic
                    self.strategies[strategy_id] = {
                        "id": strategy_id,
                        "name": strategy_name,
                        "active": True,
                        "min_spread": strategy.min_spread,
                        "threshold": strategy.threshold,
                        "max_slippage": strategy.max_slippage,
                        "min_volume": strategy.min_volume,
                        "risk_profile": strategy.risk_profile.model_dump() if strategy.risk_profile else None,
                    }

                    # Log execution strategies for this strategy
                    if hasattr(strategy, "execution_mappings") and strategy.execution_mappings:
                        execution_count = len(strategy.execution_mappings)
                        self.logger.info(f"Found {execution_count} execution strategies for {strategy_name}")

                except Exception as e:
                    self.logger.error(f"Error initializing strategy {strategy_name}: {e}", exc_info=True)

            if self.strategies:
                self.logger.info(f"Initialized execution component with {len(self.strategies)} strategies")
                return True
            else:
                self.logger.warning("No strategies were successfully initialized for execution component")
                return False

        except Exception as e:
            self.logger.error(f"Error initializing execution component: {e}", exc_info=True)
            return False

    def _get_active_strategies(self) -> List[Strategy]:
        """Get active strategies from the repository"""
        try:
            if self.strategy_repository is None:
                self.logger.error("Strategy repository not initialized")
                return []

            return self.strategy_repository.get_active()
        except Exception as e:
            self.logger.error(f"Error retrieving active strategies: {e}", exc_info=True)
            return []

    def _get_strategy_by_name(self, strategy_name: str) -> Optional[Strategy]:
        """Get a strategy by name from the repository"""
        try:
            if self.strategy_repository is None:
                self.logger.error("Strategy repository not initialized")
                return None

            return self.strategy_repository.get_by_name(strategy_name)
        except Exception as e:
            self.logger.error(f"Error retrieving strategy by name {strategy_name}: {e}", exc_info=True)
            return None

    async def _ensure_reporting_flow(self) -> bool:
        """
        Ensure that the reporting flow is running.
        This is important for execution operations as they depend on reporting for metrics.

        Returns:
            bool: True if the reporting flow is running or was started successfully
        """
        try:
            # Import required modules
            from src.arbirich.core.trading.flows.reporting.reporting_flow import get_flow
            from src.arbirich.core.trading.flows.reporting.reporting_flow import start_flow as start_reporting_flow

            # Try to get the current flow instance
            reporting_flow = await get_flow()

            # Check if the flow exists and is active
            if reporting_flow and hasattr(reporting_flow, "active") and reporting_flow.active:
                self.logger.info("Reporting flow is already running")
                return True

            # If not active, start the flow
            self.logger.info("Starting reporting flow for execution operations")
            success = await start_reporting_flow()

            if success:
                self.logger.info("Reporting flow started successfully")
                return True
            else:
                self.logger.warning("Failed to start reporting flow")
                return False

        except Exception as e:
            self.logger.error(f"Error ensuring reporting flow: {e}", exc_info=True)
            # Return True to allow execution to continue even if reporting can't be started
            return True

    async def run(self) -> None:
        """Run the execution component."""
        try:
            self.logger.info(f"Running execution component for {len(self.strategies)} flows")

            # Check if we have any strategies configured
            if not self.strategies:
                self.logger.warning("No active strategies found for execution - entering idle monitoring mode")
                # Enter wait loop instead of failing
                while self.active:
                    await asyncio.sleep(5.0)  # Check every 5 seconds for shutdown signal
                    # Periodically check for new strategies
                    await self._check_for_new_strategies()
                return

            # First ensure reporting flow is running
            await self._ensure_reporting_flow()

            # Run all flows from strategies
            self.logger.info(f"Running execution component for {len(self.active_flows)} flows")

            try:
                # Ensure reporting flow is active - this helps coordinate execution actions
                try:
                    from src.arbirich.core.trading.flows.reporting.reporting_flow import get_flow
                    from src.arbirich.core.trading.flows.reporting.reporting_flow import start_flow as start_reporting

                    reporting_flow = await get_flow()
                    if not reporting_flow or not reporting_flow.active:
                        self.logger.info("Starting reporting flow for execution coordination")
                        await start_reporting()
                except Exception as e:
                    self.logger.warning(f"Could not ensure reporting flow: {e}")

                # Start the flow for each strategy
                for flow_id, flow_info in self.active_flows.items():
                    strategy_name = flow_info.get("strategy_name")
                    self.logger.info(f"Starting execution flow for strategy: {strategy_name}")
                    await self.flow_manager.run_flow()
                    self.logger.info(f"Started execution flow: {flow_id}")

                # Monitor flow until component is stopped
                while self.active:
                    try:
                        # Check if flow manager exists before trying to access is_running()
                        if self.flow_manager is not None and not self.flow_manager.is_running():
                            self.logger.error("Execution flow stopped unexpectedly, attempting to restart")
                            await self.flow_manager.run_flow()

                        # Periodically check for strategy updates
                        if self.strategy_repository:
                            self._refresh_active_strategies()

                        await asyncio.sleep(self.update_interval)
                    except Exception as e:
                        self.logger.error(f"Error monitoring execution flows: {e}", exc_info=True)

            except asyncio.CancelledError:
                self.logger.info("Execution component cancelled")
                raise

            except Exception as e:
                if not self.handle_error(e):
                    self.active = False

        except Exception as e:
            self.logger.error(f"Error in execution component: {e}", exc_info=True)
            if not self.handle_error(e):
                self.active = False
        finally:
            await self.cleanup()

    async def _check_for_new_strategies(self) -> bool:
        """Check if any new strategies have been activated."""
        try:
            if not self.strategy_repository:
                return False

            active_strategies = self._get_active_strategies()
            if active_strategies:
                self.logger.info(f"Found {len(active_strategies)} newly activated strategies, reinitializing...")
                # Initialize with the newly found strategies
                await self.initialize()
                return True
            return False
        except Exception as e:
            self.logger.error(f"Error checking for new strategies: {e}")
            return False

    def _refresh_active_strategies(self) -> None:
        """Refresh the list of active strategies"""
        try:
            # Get current active strategies
            current_strategies = self._get_active_strategies()

            # Create a dictionary of current strategy IDs for quick lookup
            current_strategy_ids = {str(strategy.id): strategy for strategy in current_strategies}

            # Check for strategies that have been deactivated
            strategies_to_remove = []
            for strategy_id in self.strategies:
                if strategy_id not in current_strategy_ids:
                    strategy_name = self.strategies[strategy_id]["name"]
                    self.logger.info(f"Strategy {strategy_name} has been deactivated")
                    strategies_to_remove.append(strategy_id)

                    # Remove from active flows
                    flow_id = f"execution_{strategy_name}"
                    if flow_id in self.active_flows:
                        del self.active_flows[flow_id]

            # Remove deactivated strategies
            for strategy_id in strategies_to_remove:
                del self.strategies[strategy_id]

            # Check for new active strategies
            for strategy_id, strategy in current_strategy_ids.items():
                if strategy_id not in self.strategies:
                    strategy_name = strategy.name
                    self.logger.info(f"New active strategy detected: {strategy_name}")

                    # Add to strategies dict with execution parameters
                    self.strategies[strategy_id] = {
                        "id": strategy_id,
                        "name": strategy_name,
                        "active": True,
                        "min_spread": strategy.min_spread,
                        "threshold": strategy.threshold,
                        "max_slippage": strategy.max_slippage,
                        "min_volume": strategy.min_volume,
                        "risk_profile": strategy.risk_profile.model_dump() if strategy.risk_profile else None,
                    }

                    # Add to active flows
                    flow_id = f"execution_{strategy_name}"
                    self.active_flows[flow_id] = {"strategy_id": strategy_id, "strategy_name": strategy_name}

                    # Create flow manager if needed
                    from src.arbirich.core.trading.flows.bytewax_flows.execution.execution_flow import (
                        build_execution_flow,
                    )
                    from src.arbirich.core.trading.flows.flow_manager import BytewaxFlowManager

                    flow_manager = BytewaxFlowManager.get_or_create(flow_id)
                    flow_manager.build_flow = lambda strat=strategy: build_execution_flow(
                        strategy_name=strat.name, debug_mode=self.debug_mode
                    )

                    # Use this as the flow manager if we don't have one
                    if self.flow_manager is None:
                        self.flow_manager = flow_manager

                # Check for updated strategy parameters
                elif strategy_id in self.strategies:
                    current_strategy = current_strategy_ids[strategy_id]
                    existing_strategy = self.strategies[strategy_id]

                    # Check if any key execution parameters have changed
                    if (
                        existing_strategy["min_spread"] != current_strategy.min_spread
                        or existing_strategy["threshold"] != current_strategy.threshold
                        or existing_strategy["max_slippage"] != current_strategy.max_slippage
                        or existing_strategy["min_volume"] != current_strategy.min_volume
                    ):
                        self.logger.info(f"Updated parameters detected for strategy: {current_strategy.name}")

                        # Update the strategy parameters
                        existing_strategy["min_spread"] = current_strategy.min_spread
                        existing_strategy["threshold"] = current_strategy.threshold
                        existing_strategy["max_slippage"] = current_strategy.max_slippage
                        existing_strategy["min_volume"] = current_strategy.min_volume
                        existing_strategy["risk_profile"] = (
                            current_strategy.risk_profile.model_dump() if current_strategy.risk_profile else None
                        )

        except Exception as e:
            self.logger.error(f"Error refreshing active strategies: {e}", exc_info=True)

    async def cleanup(self) -> None:
        """Stop all execution flows"""
        self.logger.info("Cleaning up execution component")

        try:
            # Stop all flows
            if self.flow_manager:
                await self.flow_manager.stop_flow_async()
                self.logger.info("Stopped all execution flows")

            # Clear the active flows
            self.active_flows = {}

            # Clear strategies
            self.strategies = {}

            # Close repository if needed
            self.strategy_repository = None
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}", exc_info=True)
