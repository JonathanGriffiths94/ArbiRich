import asyncio
from typing import Any, Dict, List

from src.arbirich.services.database.database_service import DatabaseService

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
        self.exchange_configs = config.get("exchanges", {})
        self.flow_configs = config.get("flows", {})
        self.active_flows = {}
        self.flow_managers = {}
        self.stop_event = asyncio.Event()  # Add stop event for cleaner shutdown

    async def initialize(self) -> bool:
        """Initialize Bytewax flows for data ingestion"""
        try:
            # Updated import paths to reflect new structure
            from src.arbirich.core.trading.flows.bytewax_flows.ingestion.ingestion_flow import build_ingestion_flow
            from src.arbirich.core.trading.flows.flow_manager import BytewaxFlowManager

            # Get exchange configurations
            exchange_pairs = self._get_exchange_pairs()

            # Configure flows based on exchange pairs
            for exchange, pairs in exchange_pairs.items():
                for pair in pairs:
                    flow_id = f"ingestion_{exchange}_{pair}"

                    # Get or create flow manager for this flow
                    flow_manager = BytewaxFlowManager.get_or_create(flow_id)

                    # Configure the flow builder with exchange and pair information
                    # IMPORTANT: Using default arguments to properly capture exchange and pair values
                    def create_flow_builder(exchange=exchange, pair=pair):
                        def builder():
                            self.logger.info(f"Building flow for {exchange}:{pair}")
                            return build_ingestion_flow(exchange=exchange, trading_pair=pair)

                        return builder

                    flow_manager.set_flow_builder(create_flow_builder())

                    # Store flow manager reference
                    self.flow_managers[flow_id] = flow_manager

                    # Store flow configuration
                    self.active_flows[flow_id] = {"exchange": exchange, "trading_pair": pair}

                    self.logger.info(f"Configured ingestion flow for {exchange}:{pair}")

            self.logger.info(f"Initialized {len(self.active_flows)} data ingestion flows")
            return len(self.active_flows) > 0  # Return True only if we have flows

        except Exception as e:
            self.logger.error(f"Error initializing flows: {e}", exc_info=True)
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

    def _get_exchange_pairs(self) -> Dict[str, List[str]]:
        """Get exchanges and pairs to monitor"""
        # Fetch active strategies to determine what to monitor
        exchange_pairs = {}

        try:
            with DatabaseService() as db:
                active_strategies = db.get_active_strategies()

                self.logger.info(f"Found {len(active_strategies)} active strategies")

                if not active_strategies:
                    self.logger.warning("No active strategies found, using default config")
                    # Only in case of truly no active strategies, use defaults
                    from src.arbirich.config.config import EXCHANGES, PAIRS

                    for exchange in EXCHANGES:
                        exchange_pairs[exchange] = []
                        for base, quote in PAIRS:
                            exchange_pairs[exchange].append(f"{base}-{quote}")
                    return exchange_pairs

                # Process each active strategy
                for strategy in active_strategies:
                    self.logger.info(f"Processing strategy: {strategy.name}")

                    # First try to get configuration from config file
                    from src.arbirich.config.config import get_strategy_config

                    strategy_config = get_strategy_config(strategy.name)

                    if strategy_config:
                        # Get exchanges and pairs from strategy config
                        strategy_exchanges = strategy_config.get("exchanges", [])
                        strategy_pairs = strategy_config.get("pairs", [])

                        self.logger.debug(
                            f"Found in config file - exchanges: {strategy_exchanges}, pairs: {strategy_pairs}"
                        )
                    else:
                        # If not in config, try to get from database model
                        strategy_exchanges = []
                        strategy_pairs = []

                        # Try to get from additional_info
                        if hasattr(strategy, "additional_info") and strategy.additional_info:
                            # Handle both string and dict formats
                            additional_info = strategy.additional_info
                            if isinstance(additional_info, str):
                                try:
                                    import json

                                    additional_info = json.loads(additional_info)
                                except json.JSONDecodeError:
                                    self.logger.warning(
                                        f"Could not parse additional_info as JSON for strategy {strategy.name}"
                                    )
                                    additional_info = {}

                            if isinstance(additional_info, dict):
                                strategy_exchanges = additional_info.get("exchanges", [])
                                strategy_pairs = additional_info.get("pairs", [])

                                self.logger.debug(
                                    f"Found in additional_info - exchanges: {strategy_exchanges}, pairs: {strategy_pairs}"
                                )

                    # If still no exchanges/pairs, try getting from exchange_pair_mappings
                    if not strategy_exchanges or not strategy_pairs:
                        if hasattr(strategy, "exchange_pair_mappings") and strategy.exchange_pair_mappings:
                            self.logger.debug(f"Trying to get from exchange_pair_mappings for {strategy.name}")
                            # Extract exchanges and pairs from mappings
                            for mapping in strategy.exchange_pair_mappings:
                                if hasattr(mapping, "exchange_name") and mapping.exchange_name:
                                    if mapping.exchange_name not in strategy_exchanges:
                                        strategy_exchanges.append(mapping.exchange_name)

                                if hasattr(mapping, "pair_symbol") and mapping.pair_symbol:
                                    if mapping.pair_symbol not in strategy_pairs:
                                        strategy_pairs.append(mapping.pair_symbol)

                    # Skip if no exchanges or pairs found
                    if not strategy_exchanges or not strategy_pairs:
                        self.logger.warning(f"No exchanges or pairs found for strategy {strategy.name}, skipping")
                        continue

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

                # If after all that, we still have no exchanges or pairs, use fallback
                if not exchange_pairs:
                    self.logger.warning("No valid exchange-pair combinations found from strategies, using fallback")
                    from src.arbirich.config.config import STRATEGIES

                    # Use only the active strategies from config
                    for strategy_name, strategy_config in STRATEGIES.items():
                        strategy_exchanges = strategy_config.get("exchanges", [])
                        strategy_pairs = strategy_config.get("pairs", [])

                        for exchange in strategy_exchanges:
                            if exchange not in exchange_pairs:
                                exchange_pairs[exchange] = []

                            for pair in strategy_pairs:
                                # Normalize pair format
                                if isinstance(pair, tuple) and len(pair) == 2:
                                    pair_str = f"{pair[0]}-{pair[1]}"
                                elif isinstance(pair, list) and len(pair) == 2:
                                    pair_str = f"{pair[0]}-{pair[1]}"
                                elif isinstance(pair, str):
                                    pair_str = pair
                                else:
                                    self.logger.warning(f"Skipping invalid pair format: {pair}")
                                    continue

                                if pair_str not in exchange_pairs[exchange]:
                                    exchange_pairs[exchange].append(pair_str)

                return exchange_pairs

        except Exception as e:
            self.logger.error(f"Error getting exchange pairs: {e}", exc_info=True)
            # Don't return empty dict - try to use STRATEGIES config as fallback
            try:
                from src.arbirich.config.config import STRATEGIES

                for strategy_name, strategy_config in STRATEGIES.items():
                    strategy_exchanges = strategy_config.get("exchanges", [])
                    strategy_pairs = strategy_config.get("pairs", [])

                    for exchange in strategy_exchanges:
                        if exchange not in exchange_pairs:
                            exchange_pairs[exchange] = []

                        for pair in strategy_pairs:
                            # Normalize pair format
                            if isinstance(pair, tuple) and len(pair) == 2:
                                pair_str = f"{pair[0]}-{pair[1]}"
                            elif isinstance(pair, list) and len(pair) == 2:
                                pair_str = f"{pair[0]}-{pair[1]}"
                            elif isinstance(pair, str):
                                pair_str = pair
                            else:
                                continue

                            if pair_str not in exchange_pairs[exchange]:
                                exchange_pairs[exchange].append(pair_str)
            except Exception as inner_e:
                self.logger.error(f"Error using fallback configuration: {inner_e}", exc_info=True)

            return exchange_pairs
