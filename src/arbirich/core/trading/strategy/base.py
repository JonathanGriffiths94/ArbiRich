import asyncio
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

    async def initialize(self):
        """Initialize the strategy (connect to APIs, etc.)"""
        self.logger.info(f"Initializing strategy: {self.name}")
        # Implementation specific to strategy needs
        return True

    async def start(self):
        """Activate the strategy"""
        if self.active:
            self.logger.warning(f"Strategy {self.name} is already active")
            return

        self.logger.info(f"Starting strategy: {self.name}")
        self.active = True
        # Create a task that runs the strategy
        self.task = asyncio.create_task(self.run_loop())

    async def stop(self):
        """Deactivate the strategy"""
        if not self.active:
            self.logger.warning(f"Strategy {self.name} is not active")
            return

        self.logger.info(f"Stopping strategy: {self.name}")
        self.active = False

        # Cancel the task if running
        if self.task and not self.task.done():
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

        self.task = None

    async def run_loop(self):
        """Main strategy loop"""
        self.logger.info(f"Strategy {self.name} loop started")
        try:
            while self.active:
                # Process order books and execute trades
                await self.process_cycle()
                # Sleep to avoid excessive CPU usage
                await asyncio.sleep(self.config_params.cycle_interval)
        except asyncio.CancelledError:
            self.logger.info(f"Strategy {self.name} loop cancelled")
            raise
        except Exception as e:
            self.logger.error(f"Error in strategy {self.name} loop: {e}", exc_info=True)
            self.active = False

    async def process_cycle(self):
        """Process a single cycle of the strategy"""
        try:
            # Get latest order book data
            order_books = await self.get_order_books()

            # Detect opportunities
            opportunities = self.arbitrage_type.detect_opportunities(order_books)

            if not opportunities:
                return

            self.logger.info(f"Found {len(opportunities)} opportunities")

            # Process each opportunity
            for opportunity in opportunities:
                await self.process_opportunity(opportunity, order_books)

        except Exception as e:
            self.logger.error(f"Error processing cycle: {e}", exc_info=True)

    async def process_opportunity(self, opportunity, order_books):
        """Process a single arbitrage opportunity"""
        try:
            # Validate the opportunity
            if not self.arbitrage_type.validate_opportunity(opportunity):
                return

            # Calculate spread/profit
            spread = self.arbitrage_type.calculate_spread(opportunity)

            # Check if spread meets minimum threshold
            if spread < self.config_params.min_spread:
                return

            # Perform risk checks
            if not self.risk_management.validate_trade(opportunity, spread):
                return

            # Calculate position size
            position_size = self.risk_management.calculate_position_size(opportunity, self.config_params)

            # Check circuit breakers
            if not self.risk_management.check_circuit_breakers(self.performance_metrics):
                self.logger.warning("Circuit breaker triggered, skipping opportunity")
                return

            # Execute the trade
            result = await self.execution_method.execute(opportunity, position_size)

            # Update performance metrics
            self.performance_metrics.update(result)

            # Log the result
            self.logger.info(
                f"Executed trade with result: profit={result.profit}, "
                f"success={result.success}, time={result.execution_time}ms"
            )

        except Exception as e:
            self.logger.error(f"Error processing opportunity: {e}", exc_info=True)

    async def get_order_books(self):
        """Get the latest order books for the relevant exchanges and pairs"""
        # Implementation will depend on your data access mechanism
        # This is a placeholder that would be implemented based on your system
        from src.arbirich.services.orderbook_service import get_order_books

        exchanges = self.exchange_params.exchanges
        pairs = self.exchange_params.trading_pairs

        return await get_order_books(exchanges, pairs)
