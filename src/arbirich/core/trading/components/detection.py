import asyncio
from typing import Any, Dict

from .base import Component


class DetectionComponent(Component):
    """
    Detection component responsible for detecting arbitrage opportunities

    This component manages Bytewax flows for monitoring order books
    across exchanges to identify profitable trading opportunities.
    """

    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self.update_interval = config.get("update_interval", 1.0)  # seconds
        self.strategy_name = config.get("strategy_name")
        self.debug_mode = config.get("debug_mode", False)
        self.active_flows = {}
        self.strategies = {}

    async def initialize(self) -> bool:
        """Initialize detection component with database strategies."""
        try:
            # Initialize database connection
            from src.arbirich.services.database.database_service import DatabaseService

            with DatabaseService() as db_service:
                # Get active strategies
                active_strategies = db_service.get_active_strategies()

                if not active_strategies:
                    self.logger.warning("No active strategies found for detection component")
                    # Return True anyway to allow component to start
                    self.strategies = {}
                    return True

                # Initialize strategies
                for strategy in active_strategies:
                    strategy_id = str(strategy.id)
                    strategy_name = strategy.name

                    self.logger.info(f"Initializing detection component for strategy: {strategy_name}")

                    # Add strategy to the tracked strategies
                    try:
                        self.strategies[strategy_id] = {"id": strategy_id, "name": strategy_name, "active": True}
                    except Exception as e:
                        self.logger.error(f"Error initializing strategy {strategy_name}: {e}")

                if self.strategies:
                    self.logger.info(f"Initialized detection component with {len(self.strategies)} strategies")
                    return True
                else:
                    self.logger.warning("No strategies were successfully initialized for detection component")
                    return False

        except Exception as e:
            self.logger.error(f"Error initializing detection component: {e}", exc_info=True)
            return False

    async def run(self) -> None:
        """Run the detection component by starting Bytewax flows"""
        self.logger.info(f"Running detection component for strategy: {self.strategy_name}")

        try:
            # Start the flow
            for flow_id in self.active_flows:
                await self.flow_manager.run_flow()
                self.logger.info(f"Started detection flow: {flow_id}")

            # Monitor flow until component is stopped
            while self.active:
                # Check if flow is still running
                if not self.flow_manager.is_running():
                    self.logger.error("Detection flow stopped unexpectedly, attempting to restart")
                    await self.flow_manager.run_flow()

                await asyncio.sleep(self.update_interval)

        except asyncio.CancelledError:
            self.logger.info("Detection component cancelled")
            raise

        except Exception as e:
            if not self.handle_error(e):
                self.active = False

        finally:
            await self.cleanup()

    async def cleanup(self) -> None:
        """Stop all detection flows"""
        self.logger.info("Cleaning up detection component")

        try:
            # Stop the flow
            await self.flow_manager.stop_flow_async()
            self.logger.info("Stopped detection flow")

            # Clear the active flows
            self.active_flows = {}
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}", exc_info=True)
