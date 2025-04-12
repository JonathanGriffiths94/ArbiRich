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
        self.strategy_name = config.get("strategy_name", "default")
        self.active_flows = {}

        # Ensure we have a valid strategy name that matches what's used in other components
        if self.strategy_name == "default" and "strategy" in config:
            self.strategy_name = config["strategy"]

        # Important: Make sure we listen to the right channels
        self.logger.info(f"Execution component initialized for strategy: {self.strategy_name}")

    async def initialize(self) -> bool:
        """Initialize execution flows"""
        try:
            from src.arbirich.config.config import ALL_STRATEGIES
            from src.arbirich.core.trading.flows.bytewax_flows.execution.execution_flow import build_execution_flow
            from src.arbirich.core.trading.flows.flow_manager import BytewaxFlowManager

            # Validate strategy name against known strategies
            if self.strategy_name not in ALL_STRATEGIES and self.strategy_name != "default":
                self.logger.warning(f"Unknown strategy name: {self.strategy_name}, using generic channel")

            # Create flow ID
            flow_id = f"execution_{self.strategy_name}"

            # Get or create flow manager for this flow
            self.flow_manager = BytewaxFlowManager.get_or_create(flow_id)

            # Set up the flow building function
            # Configure the flow builder with our parameters
            self.flow_manager.build_flow = lambda: build_execution_flow(
                strategy_name=self.strategy_name, debug_mode=self.config.get("debug_mode", False)
            )

            self.active_flows[flow_id] = {
                "strategy_name": self.strategy_name,
                "channels": [f"trade_opportunities:{self.strategy_name}", "trade_opportunities"],
            }

            self.logger.info(f"Initialized execution component for strategy: {self.strategy_name}")
            self.logger.info(f"Listening on channels: {self.active_flows[flow_id]['channels']}")
            return True

        except Exception as e:
            self.logger.error(f"Error initializing execution flows: {e}", exc_info=True)
            return False

    async def run(self) -> None:
        """Run the execution component by starting Bytewax flows"""
        self.logger.info("Starting execution flows")

        try:
            # Start the flow
            await self.flow_manager.run_flow()
            self.logger.info(f"Started execution flow for strategy: {self.strategy_name}")

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
