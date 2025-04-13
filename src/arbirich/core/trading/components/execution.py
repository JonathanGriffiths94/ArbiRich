import asyncio
from typing import Any, Dict

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

    async def initialize(self) -> bool:
        """Initialize execution component with database strategies."""
        try:
            # Updated import paths to reflect new structure
            from src.arbirich.core.trading.flows.bytewax_flows.execution.execution_flow import build_execution_flow
            from src.arbirich.core.trading.flows.flow_manager import BytewaxFlowManager
            from src.arbirich.services.database.database_service import DatabaseService

            with DatabaseService() as db_service:
                # Get active strategies
                active_strategies = db_service.get_active_strategies()

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
                        self.strategies[strategy_id] = {"id": strategy_id, "name": strategy_name, "active": True}
                    except Exception as e:
                        self.logger.error(f"Error initializing strategy {strategy_name}: {e}")

                if self.strategies:
                    self.logger.info(f"Initialized execution component with {len(self.strategies)} strategies")
                    return True
                else:
                    self.logger.warning("No strategies were successfully initialized for execution component")
                    return False

        except Exception as e:
            self.logger.error(f"Error initializing execution component: {e}", exc_info=True)
            return False

    async def run(self) -> None:
        """Run the execution component by starting Bytewax flows"""
        self.logger.info("Running execution component")

        try:
            # Start the flow for each strategy
            for flow_id, flow_info in self.active_flows.items():
                strategy_name = flow_info.get("strategy_name")
                self.logger.info(f"Starting execution flow for strategy: {strategy_name}")
                await self.flow_manager.run_flow()
                self.logger.info(f"Started execution flow: {flow_id}")

            # Monitor flow until component is stopped
            while self.active:
                # Check if flow is still running
                if not self.flow_manager.is_running():
                    self.logger.error("Execution flow stopped unexpectedly, attempting to restart")
                    await self.flow_manager.run_flow()

                await asyncio.sleep(self.update_interval)

        except asyncio.CancelledError:
            self.logger.info("Execution component cancelled")
            raise

        except Exception as e:
            if not self.handle_error(e):
                self.active = False

        finally:
            # Ensure cleanup happens
            await self.cleanup()

    async def cleanup(self) -> None:
        """Stop all execution flows"""
        self.logger.info("Cleaning up execution component")

        try:
            # Stop the flow
            await self.flow_manager.stop_flow_async()
            self.logger.info("Stopped execution flow")

            # Clear the active flows
            self.active_flows = {}
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}", exc_info=True)
