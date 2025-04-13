import asyncio
from typing import Any, Dict, List, Optional

from src.arbirich.models.models import Strategy
from src.arbirich.services.database.repositories.strategy_repository import StrategyRepository

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
        self.debug_mode = True  # Always use debug mode to get more logging
        self.active_flows = {}
        self.strategies = {}
        self.flow_manager = None  # Initialize flow_manager attribute
        self.strategy_repository = None
        self.logger.info(f"Detection component initialized with debug_mode={self.debug_mode}")

    async def initialize(self) -> bool:
        """Initialize detection component with database strategies."""
        try:
            # Updated import paths to reflect new structure
            from src.arbirich.core.trading.flows.bytewax_flows.detection.detection_flow import build_detection_flow
            from src.arbirich.core.trading.flows.flow_manager import BytewaxFlowManager
            from src.arbirich.services.database.database_service import DatabaseService

            # Get a database connection from the service
            db_service = DatabaseService()

            # Create the strategy repository
            self.strategy_repository = StrategyRepository(engine=db_service.engine)

            # FORCE DEBUG MODE ON - Change from config parameter to fixed value
            self.debug_mode = True

            # Log that debug mode is enabled
            self.logger.info(f"🔍 Detection component initialized with DEBUG MODE {self.debug_mode}")

            # Get active strategies using the repository
            active_strategies = self._get_active_strategies()

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

                # Create a flow ID and manager for this strategy
                flow_id = f"detection_{strategy_name}"
                flow_manager = BytewaxFlowManager.get_or_create(flow_id)

                # Pass debug_mode explicitly in the lambda - Add log statement to verify
                self.logger.info(f"Creating flow manager for {strategy_name} with debug_mode={self.debug_mode}")
                flow_manager.build_flow = lambda strat=strategy: build_detection_flow(
                    strategy_name=strat.name, debug_mode=self.debug_mode, threshold=getattr(strat, "threshold", 0.001)
                )

                # Store the flow manager (use the first one if multiple strategies)
                if self.flow_manager is None:
                    self.flow_manager = flow_manager

                # Add to active flows
                self.active_flows[flow_id] = {"strategy_id": strategy_id, "strategy_name": strategy_name}

                # Add strategy to the tracked strategies
                try:
                    self.strategies[strategy_id] = {
                        "id": strategy_id,
                        "name": strategy_name,
                        "active": True,
                        "min_spread": strategy.min_spread,
                        "threshold": strategy.threshold,
                        "max_slippage": strategy.max_slippage,
                        "min_volume": strategy.min_volume,
                    }
                except Exception as e:
                    self.logger.error(f"Error initializing strategy {strategy_name}: {e}", exc_info=True)

            if self.strategies:
                self.logger.info(f"Initialized detection component with {len(self.strategies)} strategies")
                return True
            else:
                self.logger.warning("No strategies were successfully initialized for detection component")
                return False

        except Exception as e:
            self.logger.error(f"Error initializing detection component: {e}", exc_info=True)
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

    async def run(self) -> None:
        """Run the detection component by starting Bytewax flows"""
        self.logger.info(
            f"🚀 Running detection component with DEBUG_MODE={self.debug_mode} for {len(self.active_flows)} flows"
        )

        try:
            # Start the flows
            for flow_id, flow_info in self.active_flows.items():
                strategy_name = flow_info.get("strategy_name")
                self.logger.info(
                    f"⚙️ Starting detection flow: {flow_id} for strategy: {strategy_name} with debug_mode={self.debug_mode}"
                )
                await self.flow_manager.run_flow()

            # Monitor flow until component is stopped
            while self.active:
                # Check if flow is still running
                if not self.flow_manager.is_running():
                    self.logger.error("Detection flow stopped unexpectedly, attempting to restart")
                    await self.flow_manager.run_flow()

                # Periodically check for strategy updates
                if self.strategy_repository:
                    self._refresh_active_strategies()

                await asyncio.sleep(self.update_interval)

        except asyncio.CancelledError:
            self.logger.info("Detection component cancelled")
            raise

        except Exception as e:
            if not self.handle_error(e):
                self.active = False

        finally:
            await self.cleanup()

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
                    flow_id = f"detection_{strategy_name}"
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

                    # Add to strategies dict
                    self.strategies[strategy_id] = {
                        "id": strategy_id,
                        "name": strategy_name,
                        "active": True,
                        "min_spread": strategy.min_spread,
                        "threshold": strategy.threshold,
                        "max_slippage": strategy.max_slippage,
                        "min_volume": strategy.min_volume,
                    }

                    # Add to active flows
                    flow_id = f"detection_{strategy_name}"
                    self.active_flows[flow_id] = {"strategy_id": strategy_id, "strategy_name": strategy_name}

                    # Create flow manager if needed
                    from src.arbirich.core.trading.flows.bytewax_flows.detection.detection_flow import (
                        build_detection_flow,
                    )
                    from src.arbirich.core.trading.flows.flow_manager import BytewaxFlowManager

                    flow_manager = BytewaxFlowManager.get_or_create(flow_id)
                    flow_manager.build_flow = lambda strat=strategy: build_detection_flow(
                        strategy_name=strat.name, debug_mode=self.debug_mode
                    )

                    # Use this as the flow manager if we don't have one
                    if self.flow_manager is None:
                        self.flow_manager = flow_manager
        except Exception as e:
            self.logger.error(f"Error refreshing active strategies: {e}", exc_info=True)

    async def cleanup(self) -> None:
        """Stop all detection flows"""
        self.logger.info("Cleaning up detection component")

        try:
            # Stop the flow
            if self.flow_manager:
                await self.flow_manager.stop_flow_async()
                self.logger.info("Stopped detection flow")

            # Clear the active flows
            self.active_flows = {}

            # Clear strategies
            self.strategies = {}

            # Close repository if needed
            self.strategy_repository = None
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}", exc_info=True)

    async def stop(self) -> bool:
        """Stop detection component with enhanced cleanup"""
        self.logger.info("Stopping detection component")
        self.active = False

        # Mark the force kill flag to ensure all partitions exit
        try:
            from src.arbirich.core.trading.flows.bytewax_flows.detection.detection_source import (
                mark_detection_force_kill,
            )

            mark_detection_force_kill()
            self.logger.info("Set force kill flag for detection partitions")
        except Exception as e:
            self.logger.error(f"Error setting force kill flag: {e}")

        # Set the system shutdown flag
        try:
            from src.arbirich.core.state.system_state import mark_system_shutdown

            mark_system_shutdown(True)
            self.logger.info("Set system shutdown flag")
        except Exception as e:
            self.logger.error(f"Error setting system shutdown flag: {e}")

        # Stop the flow via the manager
        if self.flow_manager:
            try:
                await self.flow_manager.stop_flow_async()
                self.logger.info("Stopped detection flow via manager")
            except Exception as e:
                self.logger.error(f"Error stopping flow manager: {e}")

        # Call the base cleanup method for additional cleanup
        await self.cleanup()

        return True
