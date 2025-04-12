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

    async def initialize(self) -> bool:
        """Initialize Bytewax flows for data ingestion"""
        try:
            from src.arbirich.core.trading.bytewax_flows.common.flow_manager import BytewaxFlowManager
            from src.arbirich.core.trading.bytewax_flows.ingestion.ingestion_flow import build_ingestion_flow

            # Get exchange configurations
            exchange_pairs = self._get_exchange_pairs()

            # Configure flows based on exchange pairs
            for exchange, pairs in exchange_pairs.items():
                for pair in pairs:
                    flow_id = f"ingestion_{exchange}_{pair}"

                    # Get or create flow manager for this flow
                    flow_manager = BytewaxFlowManager.get_or_create(flow_id)

                    # Configure the flow builder with exchange and pair information
                    flow_manager.build_flow = lambda ex=exchange, pr=pair: build_ingestion_flow(
                        exchange=ex, trading_pair=pr
                    )

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
                await flow_manager.run_flow()
                self.logger.info(f"Started flow {flow_id}")

            # Monitor flows until component is stopped
            while self.active:
                # Check flow status periodically
                for flow_id, flow_manager in self.flow_managers.items():
                    if not flow_manager.is_running():
                        self.logger.error(f"Flow {flow_id} stopped unexpectedly, attempting to restart")
                        await flow_manager.run_flow()

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

    def _get_exchange_pairs(self) -> Dict[str, List[str]]:
        """Get exchanges and pairs to monitor"""
        # Fetch active strategies to determine what to monitor

        try:
            with DatabaseService() as db:
                active_strategies = db.get_active_strategies()

            # Collect all exchanges and pairs
            exchange_pairs = {}

            for strategy in active_strategies:
                # Pydantic models don't have a 'get' method, access attributes directly
                # and handle the case where they might not exist
                strategy_exchanges = []
                strategy_pairs = []

                # If Strategy object has additional_info field, it might contain the configuration
                if hasattr(strategy, "additional_info") and strategy.additional_info:
                    # Handle both string and dict formats
                    additional_info = strategy.additional_info
                    if isinstance(additional_info, str):
                        try:
                            import json

                            additional_info = json.loads(additional_info)
                        except json.JSONDecodeError:
                            additional_info = {}

                    if isinstance(additional_info, dict):
                        strategy_exchanges = additional_info.get("exchanges", [])
                        strategy_pairs = additional_info.get("pairs", [])

                # Add exchanges to the collection
                for exchange in strategy_exchanges:
                    if exchange not in exchange_pairs:
                        exchange_pairs[exchange] = []

                    # Process all pairs for this exchange
                    for pair in strategy_pairs:
                        # Normalize pair format
                        if isinstance(pair, tuple):
                            pair = f"{pair[0]}-{pair[1]}"
                        elif isinstance(pair, list) and len(pair) == 2:
                            pair = f"{pair[0]}-{pair[1]}"

                        if pair not in exchange_pairs[exchange]:
                            exchange_pairs[exchange].append(pair)

            # If no active strategies, use defaults from config
            if not exchange_pairs:
                from src.arbirich.config.config import EXCHANGES, PAIRS

                for exchange in EXCHANGES:
                    exchange_pairs[exchange] = []
                    for base, quote in PAIRS:
                        exchange_pairs[exchange].append(f"{base}-{quote}")

            return exchange_pairs

        except Exception as e:
            self.logger.error(f"Error getting exchange pairs: {e}", exc_info=True)
            # Return empty dict on error
            return {}
