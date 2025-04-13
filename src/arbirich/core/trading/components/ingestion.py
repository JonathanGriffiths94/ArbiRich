import asyncio
from typing import Any, Dict, List

from .base import Component


class IngestionComponent(Component):
    """
    Ingestion component responsible for collecting market data

    This component manages Bytewax flows for market data ingestion
    and publishes data to internal channels for other components.
    """

    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self.update_interval = config.get("update_interval", 1.0)  # seconds
        self.debug_mode = config.get("debug_mode", False)
        self.exchange_configs = config.get("exchanges", {})
        self.flow_configs = config.get("flows", {})
        self.active_flows = {}
        self.flow_managers = {}
        self.strategies = {}
        self.exchange_pair_mapping = {}
        self.stop_event = asyncio.Event()  # Add stop event for cleaner shutdown

    async def initialize(self) -> bool:
        """Initialize Bytewax flows for data ingestion based on active strategies"""
        try:
            # Updated import paths to reflect new structure
            from src.arbirich.core.trading.flows.bytewax_flows.ingestion.ingestion_flow import build_ingestion_flow
            from src.arbirich.core.trading.flows.flow_manager import BytewaxFlowManager
            from src.arbirich.services.database.database_service import DatabaseService

            with DatabaseService() as db_service:
                # Get active strategies
                active_strategies = db_service.get_active_strategies()

                if not active_strategies:
                    self.logger.warning("No active strategies found for ingestion component")
                    # Fall back to configuration approach
                    exchange_pairs = self._get_exchange_pairs_from_config()
                else:
                    # Initialize strategies and build exchange-pair mapping
                    for strategy in active_strategies:
                        strategy_id = str(strategy.id)
                        strategy_name = strategy.name

                        self.logger.info(f"Found active strategy: {strategy_name}")

                        # Add strategy to the tracked strategies
                        try:
                            self.strategies[strategy_id] = {"id": strategy_id, "name": strategy_name, "active": True}
                        except Exception as e:
                            self.logger.error(f"Error tracking strategy {strategy_name}: {e}")

                    # Build exchange-pair mapping from active strategies
                    exchange_pairs = self._get_exchange_pairs()

                # Configure flows based on exchange pairs
                for exchange, pairs in exchange_pairs.items():
                    self.logger.info(f"Setting up ingestion for exchange {exchange} with {len(pairs)} pairs")
                    for pair in pairs:
                        flow_id = f"ingestion_{exchange}_{pair}"

                        # Get or create flow manager for this flow
                        flow_manager = BytewaxFlowManager.get_or_create(flow_id)

                        # Configure the flow builder
                        def create_flow_builder(exchange=exchange, pair=pair):
                            def builder():
                                self.logger.info(f"Building flow for {exchange}:{pair}")
                                return build_ingestion_flow(
                                    exchange=exchange, trading_pair=pair, debug_mode=self.debug_mode
                                )

                            return builder

                        flow_manager.set_flow_builder(create_flow_builder())

                        # Store flow manager reference
                        self.flow_managers[flow_id] = flow_manager

                        # Store flow configuration
                        self.active_flows[flow_id] = {"exchange": exchange, "trading_pair": pair}

                        self.logger.info(f"Configured ingestion flow for {exchange}:{pair}")

                if self.strategies:
                    self.logger.info(f"Initialized ingestion component with {len(self.strategies)} strategies")
                else:
                    self.logger.warning("No strategies were successfully initialized for ingestion component")

                self.logger.info(f"Initialized {len(self.active_flows)} data ingestion flows")
                return len(self.active_flows) > 0  # Return True only if we have flows

        except Exception as e:
            self.logger.error(f"Error initializing ingestion component: {e}", exc_info=True)
            return False

    async def run(self) -> None:
        """Run the ingestion component by starting Bytewax flows"""
        self.logger.info("Starting data ingestion flows")

        try:
            # Start all configured flows
            for flow_id, flow_manager in self.flow_managers.items():
                flow_config = self.active_flows[flow_id]
                self.logger.info(f"Starting flow {flow_id} for {flow_config['exchange']}:{flow_config['trading_pair']}")
                await flow_manager.run_flow()
                self.logger.info(f"Started flow {flow_id}")

            # Monitor flows until component is stopped
            while self.active:
                # Check flow status and track publishing stats
                active_count = 0
                for flow_id, flow_manager in self.flow_managers.items():
                    if flow_manager.is_running():
                        active_count += 1
                    else:
                        flow_config = self.active_flows[flow_id]
                        self.logger.error(
                            f"Flow {flow_id} for {flow_config['exchange']}:{flow_config['trading_pair']} stopped unexpectedly, restarting"
                        )
                        await flow_manager.run_flow()

                self.logger.info(f"Ingestion component monitoring {active_count} active flows")
                await asyncio.sleep(self.update_interval)

        except asyncio.CancelledError:
            self.logger.info("Ingestion component cancelled")
            raise

        except Exception as e:
            if not self.handle_error(e):
                self.active = False

        finally:
            # Ensure cleanup happens
            await self.cleanup()

    async def cleanup(self) -> None:
        """Stop all Bytewax flows"""
        self.logger.info("Cleaning up ingestion component")

        try:
            # Stop all active flows
            for flow_id, flow_manager in self.flow_managers.items():
                await flow_manager.stop_flow_async()
                self.logger.info(f"Stopped flow {flow_id}")

            # Clear the tracking dictionaries
            self.active_flows = {}
            self.flow_managers = {}
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}", exc_info=True)

    async def stop(self) -> bool:
        """
        Signal component to stop.
        Override the base class method but call it to ensure proper cleanup.

        Returns:
            bool: True if the component was stopped successfully
        """
        self.logger.info("Stopping ingestion component")
        self.stop_event.set()

        # Set system shutdown flag if available
        try:
            from src.arbirich.core.state.system_state import mark_system_shutdown

            mark_system_shutdown(True)
            self.logger.info("System shutdown flag set")
        except Exception as e:
            self.logger.error(f"Error setting system shutdown flag: {e}")

        # Use the base class stop method for standard cleanup
        return await super().stop()

    def handle_error(self, error: Exception) -> bool:
        """Handle component errors, return True if error was handled"""
        # Override the base class method
        self.logger.error(f"Error in ingestion component: {error}", exc_info=True)
        # Always attempt to continue despite errors
        return True

    def _get_exchange_pairs_from_config(self) -> Dict[str, List[str]]:
        """Get exchanges and pairs from configuration"""
        exchange_pairs = {}

        try:
            from src.arbirich.config.config import EXCHANGES, PAIRS

            for exchange in EXCHANGES:
                exchange_pairs[exchange] = []
                for base, quote in PAIRS:
                    exchange_pairs[exchange].append(f"{base}-{quote}")

            self.logger.info(f"Using configured exchanges and pairs: {exchange_pairs}")
        except Exception as e:
            self.logger.error(f"Error getting exchanges and pairs from config: {e}")

        return exchange_pairs

    def _get_exchange_pairs(self) -> Dict[str, List[str]]:
        """Get exchanges and pairs to monitor based on active strategies"""
        # Initialize empty exchange-pair mapping
        exchange_pairs = {}

        try:
            # Process each active strategy
            for strategy_id, strategy_info in self.strategies.items():
                strategy_name = strategy_info["name"]
                self.logger.info(f"Processing strategy: {strategy_name}")

                # First try to get configuration from config file
                from src.arbirich.config.config import get_strategy_config

                strategy_config = get_strategy_config(strategy_name)

                if strategy_config:
                    # Get exchanges and pairs from strategy config
                    strategy_exchanges = strategy_config.get("exchanges", [])
                    strategy_pairs = strategy_config.get("pairs", [])

                    self.logger.debug(
                        f"Found in config file - exchanges: {strategy_exchanges}, pairs: {strategy_pairs}"
                    )

                    # Add exchanges and pairs to collection
                    for exchange in strategy_exchanges:
                        if exchange not in exchange_pairs:
                            exchange_pairs[exchange] = []

                        # Process all pairs for this exchange
                        for pair in strategy_pairs:
                            # Normalize pair format
                            if isinstance(pair, tuple) and len(pair) == 2:
                                pair = f"{pair[0]}-{pair[1]}"
                            elif isinstance(pair, list) and len(pair) == 2:
                                pair = f"{pair[0]}-{pair[1]}"

                            if pair not in exchange_pairs[exchange]:
                                exchange_pairs[exchange].append(pair)

            # Log what we found
            self.logger.info(f"Final exchange-pair mapping for ingestion: {exchange_pairs}")

            # If we still have no exchanges or pairs, use fallback from config
            if not exchange_pairs:
                self.logger.warning("No valid exchange-pair combinations found from strategies, using fallback")
                exchange_pairs = self._get_exchange_pairs_from_config()

            return exchange_pairs

        except Exception as e:
            self.logger.error(f"Error getting exchange pairs: {e}", exc_info=True)
            # Fallback to config approach
            return self._get_exchange_pairs_from_config()
