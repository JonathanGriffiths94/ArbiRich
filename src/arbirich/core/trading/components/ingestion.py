import asyncio
from typing import Any, Dict, List, Optional, Tuple

import sqlalchemy as sa

from src.arbirich.models.models import Strategy
from src.arbirich.services.database.repositories.strategy_repository import StrategyRepository

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
        self.strategy_repository = None

    async def initialize(self) -> bool:
        """Initialize Bytewax flows for data ingestion based on active strategies"""
        try:
            # Updated import paths to reflect new structure
            from src.arbirich.core.trading.flows.bytewax_flows.ingestion.ingestion_flow import build_ingestion_flow
            from src.arbirich.core.trading.flows.flow_manager import BytewaxFlowManager
            from src.arbirich.services.database.database_service import DatabaseService

            # Get a database connection from the service
            db_service = DatabaseService()

            # Create the strategy repository
            self.strategy_repository = StrategyRepository(engine=db_service.engine)

            # Get active strategies using the repository
            active_strategies = self._get_active_strategies()

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

                    # Add strategy to the tracked strategies with exchange-pair mappings
                    try:
                        # Store complete strategy data including exchange-pair mappings
                        self.strategies[strategy_id] = {
                            "id": strategy_id,
                            "name": strategy_name,
                            "active": True,
                            "exchange_pair_mappings": self._extract_exchange_pair_mappings(strategy),
                        }
                    except Exception as e:
                        self.logger.error(f"Error tracking strategy {strategy_name}: {e}", exc_info=True)

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

    def _extract_exchange_pair_mappings(self, strategy: Strategy) -> List[Dict[str, Any]]:
        """Extract exchange-pair mappings from a strategy's Pydantic models"""
        mappings = []

        try:
            if hasattr(strategy, "exchange_pair_mappings") and strategy.exchange_pair_mappings:
                for mapping in strategy.exchange_pair_mappings:
                    # Skip mappings that don't have required fields
                    exchange_name = getattr(mapping, "exchange_name", None)
                    pair_symbol = getattr(mapping, "pair_symbol", None)

                    # If the exchange or pair is missing, try to look them up from the database
                    if not exchange_name or not pair_symbol:
                        exchange_id = getattr(mapping, "exchange_id", None)
                        trading_pair_id = getattr(mapping, "trading_pair_id", None)

                        if not exchange_id or not trading_pair_id:
                            self.logger.warning(f"Incomplete mapping data, missing IDs: {mapping}")
                            continue

                        # Look up exchange and pair details if we have a database connection
                        if self.strategy_repository:
                            exchange_name, pair_symbol = self._lookup_exchange_pair_details(
                                exchange_id, trading_pair_id
                            )

                            if not exchange_name or not pair_symbol:
                                self.logger.warning(
                                    f"Failed to look up exchange/pair details for IDs: {exchange_id}/{trading_pair_id}"
                                )
                                continue

                    # Create a clean dictionary from the Pydantic model
                    mappings.append(
                        {
                            "exchange_id": getattr(mapping, "exchange_id", None),
                            "exchange_name": exchange_name,
                            "trading_pair_id": getattr(mapping, "trading_pair_id", None),
                            "pair_symbol": pair_symbol,
                            "is_active": getattr(mapping, "is_active", True),
                        }
                    )

                self.logger.debug(
                    f"Extracted {len(mappings)} valid exchange-pair mappings for strategy {strategy.name}"
                )
            else:
                self.logger.warning(f"No exchange-pair mappings found for strategy {strategy.name}")
        except Exception as e:
            self.logger.error(f"Error extracting exchange-pair mappings: {e}", exc_info=True)

        return mappings

    def _lookup_exchange_pair_details(
        self, exchange_id: int, trading_pair_id: int
    ) -> Tuple[Optional[str], Optional[str]]:
        """Look up exchange name and pair symbol from the database"""
        try:
            # If we don't have a repository, we can't look up details
            if not self.strategy_repository:
                return None, None

            # Execute a query to get the exchange name and pair symbol
            with self.strategy_repository.engine.connect() as conn:
                query = sa.text("""
                    SELECT e.name as exchange_name, tp.symbol as pair_symbol
                    FROM exchanges e, trading_pairs tp
                    WHERE e.id = :exchange_id AND tp.id = :trading_pair_id
                """)

                result = conn.execute(query, {"exchange_id": exchange_id, "trading_pair_id": trading_pair_id})
                row = result.first()

                if row:
                    return row.exchange_name, row.pair_symbol
                else:
                    return None, None
        except Exception as e:
            self.logger.error(f"Error looking up exchange/pair details: {e}", exc_info=True)
            return None, None

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

                # Periodically check for strategy updates
                if self.strategy_repository:
                    self._refresh_active_strategies()

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

    def _refresh_active_strategies(self) -> None:
        """
        Refresh active strategies and update exchange-pair mappings if needed
        This allows adding or removing data feeds dynamically when strategies change
        """
        try:
            # Get current active strategies
            current_strategies = self._get_active_strategies()

            # Check if we need to rebuild exchange-pair mappings
            needs_rebuild = False

            # Create a dictionary of current strategy IDs for quick lookup
            current_strategy_ids = {str(strategy.id): strategy for strategy in current_strategies}

            # Check for strategies that have been deactivated
            strategies_to_remove = []
            for strategy_id in self.strategies:
                if strategy_id not in current_strategy_ids:
                    strategy_name = self.strategies[strategy_id]["name"]
                    self.logger.info(f"Strategy {strategy_name} has been deactivated")
                    strategies_to_remove.append(strategy_id)
                    needs_rebuild = True

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
                        "exchange_pair_mappings": self._extract_exchange_pair_mappings(strategy),
                    }
                    needs_rebuild = True

            # If we need to rebuild, update exchange-pair mappings and flows
            if needs_rebuild:
                self.logger.info("Rebuilding exchange-pair mappings due to strategy changes")
                self._update_exchange_pair_flows()

        except Exception as e:
            self.logger.error(f"Error refreshing active strategies: {e}", exc_info=True)

    async def _update_exchange_pair_flows(self) -> None:
        """Update exchange-pair flows based on current strategies"""
        try:
            # Get updated exchange-pair mapping
            new_exchange_pairs = self._get_exchange_pairs()

            # Track flows to add and remove
            flows_to_add = {}
            flows_to_remove = []

            # Check for flows to remove (pairs no longer needed)
            for flow_id, flow_config in self.active_flows.items():
                exchange = flow_config["exchange"]
                pair = flow_config["trading_pair"]

                # Check if this exchange-pair is still needed
                if exchange in new_exchange_pairs and pair in new_exchange_pairs[exchange]:
                    continue
                else:
                    flows_to_remove.append(flow_id)

            # Check for flows to add (new pairs needed)
            for exchange, pairs in new_exchange_pairs.items():
                for pair in pairs:
                    flow_id = f"ingestion_{exchange}_{pair}"

                    # Check if this flow already exists
                    if flow_id not in self.active_flows:
                        flows_to_add[flow_id] = {"exchange": exchange, "trading_pair": pair}

            # Remove flows no longer needed
            for flow_id in flows_to_remove:
                flow_config = self.active_flows[flow_id]
                self.logger.info(f"Removing flow {flow_id} for {flow_config['exchange']}:{flow_config['trading_pair']}")

                # Stop and remove the flow
                if flow_id in self.flow_managers:
                    await self.flow_managers[flow_id].stop_flow_async()
                    del self.flow_managers[flow_id]

                # Remove from active flows
                del self.active_flows[flow_id]

            # Add new flows
            from src.arbirich.core.trading.flows.bytewax_flows.ingestion.ingestion_flow import build_ingestion_flow
            from src.arbirich.core.trading.flows.flow_manager import BytewaxFlowManager

            for flow_id, flow_config in flows_to_add.items():
                exchange = flow_config["exchange"]
                pair = flow_config["trading_pair"]

                self.logger.info(f"Adding new flow {flow_id} for {exchange}:{pair}")

                # Create flow manager
                flow_manager = BytewaxFlowManager.get_or_create(flow_id)

                # Configure flow builder
                def create_flow_builder(exchange=exchange, pair=pair):
                    def builder():
                        self.logger.info(f"Building flow for {exchange}:{pair}")
                        return build_ingestion_flow(exchange=exchange, trading_pair=pair, debug_mode=self.debug_mode)

                    return builder

                flow_manager.set_flow_builder(create_flow_builder())

                # Store manager and config
                self.flow_managers[flow_id] = flow_manager
                self.active_flows[flow_id] = flow_config

                # Start the new flow
                await flow_manager.run_flow()

            if flows_to_remove or flows_to_add:
                self.logger.info(f"Updated ingestion flows: removed {len(flows_to_remove)}, added {len(flows_to_add)}")

        except Exception as e:
            self.logger.error(f"Error updating exchange-pair flows: {e}", exc_info=True)

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
            self.strategies = {}

            # Clean up repository
            self.strategy_repository = None
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

        # Check if we have any strategies
        if not self.strategies:
            raise ValueError("No active strategies found for ingestion")

        # Process each strategy's exchange-pair mappings
        for strategy_id, strategy_info in self.strategies.items():
            strategy_name = strategy_info["name"]
            self.logger.info(f"Processing strategy exchange-pairs: {strategy_name}")

            # Get the exchange-pair mappings from the strategy
            if "exchange_pair_mappings" not in strategy_info or not strategy_info["exchange_pair_mappings"]:
                raise ValueError(f"No exchange-pair mappings found for strategy: {strategy_name}")

            for mapping in strategy_info["exchange_pair_mappings"]:
                if mapping["is_active"]:
                    exchange_name = mapping["exchange_name"]
                    pair_symbol = mapping["pair_symbol"]

                    if not exchange_name or not pair_symbol:
                        raise ValueError(f"Missing exchange name or pair symbol in mapping: {mapping}")

                    # Add to exchange_pairs dictionary
                    if exchange_name not in exchange_pairs:
                        exchange_pairs[exchange_name] = []

                    if pair_symbol not in exchange_pairs[exchange_name]:
                        exchange_pairs[exchange_name].append(pair_symbol)
                        self.logger.debug(f"Added {exchange_name}:{pair_symbol} from strategy {strategy_name}")

        # Check if we found any exchange-pairs
        if not exchange_pairs:
            raise ValueError("No valid exchange-pair combinations found from strategies")

        # Log what we found
        self.logger.info(f"Final exchange-pair mapping for ingestion: {exchange_pairs}")
        return exchange_pairs
