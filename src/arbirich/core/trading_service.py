# app/services/trading_system.py
import asyncio
import logging


class TradingService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.components = {
            "ingestion": {"active": False, "task": None},
            "arbitrage": {"active": False, "task": None},
            "execution": {"active": False, "task": None},
            "reporting": {"active": False, "task": None},
        }
        self.strategies = {}  # Will be populated from database
        self.db = None  # Will be set during initialization

    async def initialize(self, db=None):
        """Initialize the trading system with data from the database"""
        try:
            self.logger.info("TradingService initialization started")
            self._initialized = True  # Set flag to avoid repeated initialization

            # If no db is provided, try to create one
            if db is None:
                self.logger.info("No database provided, creating new connection")
                from src.arbirich.services.database.database_service import DatabaseService

                self.db = DatabaseService()
            else:
                self.db = db

            # Load strategies using try/except to handle both sync and async database interfaces
            try:
                self.logger.info("Loading strategies from database")
                # Try using async fetch_all if available
                if hasattr(db, "fetch_all") and callable(db.fetch_all):
                    if asyncio.iscoroutinefunction(db.fetch_all):
                        strategies = await db.fetch_all("SELECT * FROM strategies")
                    else:
                        strategies = db.fetch_all("SELECT * FROM strategies")
                # Fallback to direct query using DatabaseService
                elif hasattr(db, "engine"):
                    with db.engine.connect() as conn:
                        result = conn.execute(db.tables.strategies.select())
                        strategies = [dict(row._mapping) for row in result]
                else:
                    # Final fallback - just get active strategies from config
                    from src.arbirich.config.config import STRATEGIES

                    self.logger.warning("Could not load strategies from database, using config")
                    strategies = []
                    for name, config in STRATEGIES.items():
                        strategies.append(
                            {
                                "id": hash(name) % 10000,  # Use hash for ID
                                "name": name,
                                "is_active": True,  # Assume active
                            }
                        )
            except Exception as e:
                self.logger.warning(f"Error loading strategies from database: {e}", exc_info=True)
                strategies = []

            # Process loaded strategies
            for strategy in strategies:
                # Handle strategies from different sources
                strategy_id = strategy.get("id") or strategy.get("strategy_id")
                strategy_name = strategy.get("name") or strategy.get("strategy_name")
                is_active = strategy.get("is_active", False)

                if not strategy_id or not strategy_name:
                    self.logger.warning(f"Skipping invalid strategy: {strategy}")
                    continue

                self.strategies[strategy_id] = {
                    "id": strategy_id,
                    "name": strategy_name,
                    "active": is_active,
                    "task": None,
                }

            self.logger.info(f"Loaded {len(self.strategies)} strategies")
            self.logger.info("TradingService initialization completed successfully")
        except Exception as e:
            self.logger.error(f"Error initializing trading service: {e}", exc_info=True)
            # Set initialized flag to false to allow retry
            self._initialized = False
            # Don't re-raise the exception, just log it and continue

    async def get_status(self):
        """Get the current status of all system components"""
        status = {component: data["active"] for component, data in self.components.items()}
        status["overall"] = all(data["active"] for data in self.components.values())
        return status

    async def start_all(self, activate_strategies=False):
        """
        Start all system components.

        Args:
            activate_strategies: Whether to activate all strategies in the database
        """
        self.logger.info("Starting all system components")

        # If requested, activate strategies in database
        if activate_strategies:
            self.logger.info("Activating all strategies in database")
            try:
                from src.arbirich.services.database.database_service import DatabaseService

                with DatabaseService() as db:
                    # Get all strategy IDs
                    with db.engine.connect() as conn:
                        import sqlalchemy as sa

                        result = conn.execute(sa.text("UPDATE strategies SET is_active = TRUE"))
                        self.logger.info(f"Activated {result.rowcount} strategies in database")
            except Exception as e:
                self.logger.error(f"Failed to activate strategies: {e}")

        # Start components in the correct order
        await self.start_component("reporting")
        await self.start_component("ingestion")
        await self.start_component("arbitrage")
        await self.start_component("execution")

        self.logger.info("All system components started")

    async def stop_all(self):
        """Stop all system components"""
        self.logger.info("Stopping all system components")

        # Stop in reverse order
        await self.stop_component("execution")
        await self.stop_component("arbitrage")
        await self.stop_component("ingestion")
        await self.stop_component("reporting")

        self.logger.info("All system components stopped")

    async def start_component(self, component_name):
        """Start a specific component"""
        if component_name not in self.components:
            raise ValueError(f"Unknown component: {component_name}")

        component = self.components[component_name]
        if component["active"]:
            return {"status": "already_running"}

        self.logger.info(f"Starting component: {component_name}")

        # Start the appropriate background task based on component type
        if component_name == "ingestion":
            component["task"] = asyncio.create_task(self._run_ingestion())
        elif component_name == "arbitrage":
            component["task"] = asyncio.create_task(self._run_arbitrage())
        elif component_name == "execution":
            component["task"] = asyncio.create_task(self._run_execution())
        elif component_name == "reporting":
            component["task"] = asyncio.create_task(self._run_reporting())

        component["active"] = True
        return {"status": "started"}

    async def stop_component(self, component_name):
        """Stop a specific component"""
        if component_name not in self.components:
            raise ValueError(f"Unknown component: {component_name}")

        component = self.components[component_name]
        if not component["active"]:
            return {"status": "not_running"}

        self.logger.info(f"Stopping component: {component_name}")

        # Cancel the component task if it's running
        if component["task"] and not component["task"].done():
            component["task"].cancel()
            try:
                await component["task"]
            except asyncio.CancelledError:
                # This is expected when canceling a task
                pass
            except Exception as e:
                self.logger.error(f"Error while stopping {component_name}: {e}")

        component["active"] = False
        component["task"] = None

        return {"status": "stopped"}

    async def start_strategy(self, strategy_id):
        """Start a specific strategy"""
        if strategy_id not in self.strategies:
            raise ValueError(f"Unknown strategy: {strategy_id}")

        strategy = self.strategies[strategy_id]
        if strategy["active"]:
            return {"status": "already_running"}

        self.logger.info(f"Starting strategy: {strategy['name']} (ID: {strategy_id})")

        # Update database
        await self.db.execute("UPDATE strategies SET is_active = TRUE WHERE id = :id", {"id": strategy_id})

        # Start the strategy task
        strategy["task"] = asyncio.create_task(self._run_strategy(strategy_id))
        strategy["active"] = True

        return {"status": "started"}

    async def stop_strategy(self, strategy_id):
        """Stop a specific strategy"""
        if strategy_id not in self.strategies:
            raise ValueError(f"Unknown strategy: {strategy_id}")

        strategy = self.strategies[strategy_id]
        if not strategy["active"]:
            return {"status": "not_running"}

        self.logger.info(f"Stopping strategy: {strategy['name']} (ID: {strategy_id})")

        # Update database
        await self.db.execute("UPDATE strategies SET is_active = FALSE WHERE id = :id", {"id": strategy_id})

        # Cancel the strategy task
        if strategy["task"] and not strategy["task"].done():
            strategy["task"].cancel()
            try:
                await strategy["task"]
            except asyncio.CancelledError:
                pass

        strategy["active"] = False
        strategy["task"] = None

        return {"status": "stopped"}

    # Background task implementations
    async def _run_ingestion(self):
        """Run the ingestion process"""
        self.logger.info("Ingestion process started")
        try:
            # Get active exchanges and pairs from database
            from src.arbirich.services.database.database_service import DatabaseService

            exchanges_and_pairs = {}

            try:
                with DatabaseService() as db:
                    # Get active exchanges and pairs
                    exchanges = db.get_active_exchanges()
                    pairs = db.get_active_pairs()

                    # Build exchange_pairs mapping
                    for exchange in exchanges:
                        exchange_name = exchange.name if hasattr(exchange, "name") else exchange.get("name")
                        if not exchange_name:
                            continue

                        exchanges_and_pairs[exchange_name] = []

                        for pair in pairs:
                            pair_symbol = pair.symbol if hasattr(pair, "symbol") else pair.get("symbol")
                            if not pair_symbol:
                                continue

                            exchanges_and_pairs[exchange_name].append(pair_symbol)

                # Start ingestion with the configured exchanges and pairs
                self.logger.info(f"Starting ingestion pipeline with {len(exchanges_and_pairs)} exchanges...")
                self.logger.info(f"Exchanges and pairs: {exchanges_and_pairs}")

                # Import here to avoid circular imports
                from arbirich.flows.ingestion.ingestion_flow import run_ingestion_flow

                await run_ingestion_flow(exchanges_and_pairs)

            except Exception as e:
                self.logger.error(f"Error setting up ingestion configuration: {e}", exc_info=True)
                # Fall back to config
                from src.arbirich.config.config import EXCHANGES, PAIRS

                # Create exchange_pairs mapping from config
                fallback_exchanges_pairs = {}
                for exchange_name in EXCHANGES.keys():
                    fallback_exchanges_pairs[exchange_name] = []
                    for base, quote in PAIRS:
                        fallback_exchanges_pairs[exchange_name].append(f"{base}-{quote}")

                self.logger.info(f"Using fallback configuration: {fallback_exchanges_pairs}")

                # Import here to avoid circular imports
                from arbirich.flows.ingestion.ingestion_flow import run_ingestion_flow

                await run_ingestion_flow(fallback_exchanges_pairs)

        except asyncio.CancelledError:
            self.logger.info("Ingestion process stopped")
            raise
        except Exception as e:
            self.logger.error(f"Error in ingestion process: {e}", exc_info=True)
            # Sleep briefly to avoid rapid restart loop
            await asyncio.sleep(5)

    async def _run_arbitrage(self):
        """Run the arbitrage process"""
        self.logger.info("Arbitrage process started")
        try:
            # Launch arbitrage pipelines for all active strategies
            from arbirich.flows.arbitrage.arbitrage_flow import run_arbitrage_flow
            from src.arbirich.config.config import STRATEGIES

            # Find active strategies
            active_strategies = []
            for strategy_name, strategy_config in STRATEGIES.items():
                # Check if strategy is active in our loaded strategies list
                strategy_id = None
                for sid, strategy in self.strategies.items():
                    if strategy["name"] == strategy_name and strategy["active"]:
                        strategy_id = sid
                        active_strategies.append({"id": sid, "name": strategy_name})
                        break

            if not active_strategies:
                self.logger.warning("No active strategies found for arbitrage process")
                # Fall back to a default strategy
                self.logger.info("Starting arbitrage pipeline with default strategy")
                await run_arbitrage_flow(strategy_name="basic_arbitrage", debug_mode=True)
            else:
                # Start a pipeline for each active strategy
                self.logger.info(f"Starting arbitrage pipelines for {len(active_strategies)} strategies")

                # Create tasks for each strategy pipeline
                tasks = []
                for strategy in active_strategies:
                    strategy_name = strategy["name"]
                    self.logger.info(f"Starting arbitrage pipeline for strategy: {strategy_name}")
                    # Create and store the task
                    task = asyncio.create_task(
                        run_arbitrage_flow(strategy_name=strategy_name, debug_mode=True),
                        name=f"arbitrage-{strategy_name}",
                    )
                    tasks.append(task)

                # Wait for all strategy pipelines to complete or cancel
                try:
                    await asyncio.gather(*tasks)
                except asyncio.CancelledError:
                    # Cancel all tasks if this task is cancelled
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    # Wait for all cancellations
                    await asyncio.gather(*tasks, return_exceptions=True)
                    raise

        except asyncio.CancelledError:
            self.logger.info("Arbitrage process stopped")
            raise
        except Exception as e:
            self.logger.error(f"Error in arbitrage process: {e}", exc_info=True)
            # Sleep briefly to avoid rapid restart loop
            await asyncio.sleep(5)

    async def _run_execution(self):
        """Run the execution process"""
        self.logger.info("Execution process started")
        try:
            # Launch execution pipelines for all active strategies
            from arbirich.flows.execution.execution_flow import run_execution_flow
            from src.arbirich.config.config import STRATEGIES

            # Find active strategies
            active_strategies = []
            for strategy_name, strategy_config in STRATEGIES.items():
                # Check if strategy is active in our loaded strategies list
                strategy_id = None
                for sid, strategy in self.strategies.items():
                    if strategy["name"] == strategy_name and strategy["active"]:
                        strategy_id = sid
                        active_strategies.append({"id": sid, "name": strategy_name})
                        break

            if not active_strategies:
                self.logger.warning("No active strategies found for execution process")
                # Run a general execution flow for all strategies
                self.logger.info("Starting execution pipeline for all strategies")
                await run_execution_flow()
            else:
                # Start a pipeline for each active strategy
                self.logger.info(f"Starting execution pipelines for {len(active_strategies)} strategies")

                # Create tasks for each strategy pipeline
                tasks = []
                for strategy in active_strategies:
                    strategy_name = strategy["name"]
                    self.logger.info(f"Starting execution pipeline for strategy: {strategy_name}")
                    # Create and store the task
                    task = asyncio.create_task(
                        run_execution_flow(strategy_name),
                        name=f"execution-{strategy_name}",
                    )
                    tasks.append(task)

                # Wait for all strategy pipelines to complete or cancel
                try:
                    await asyncio.gather(*tasks)
                except asyncio.CancelledError:
                    # Cancel all tasks if this task is cancelled
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    # Wait for all cancellations
                    await asyncio.gather(*tasks, return_exceptions=True)
                    raise

        except asyncio.CancelledError:
            self.logger.info("Execution process stopped")
            raise
        except Exception as e:
            self.logger.error(f"Error in execution process: {e}", exc_info=True)
            # Sleep briefly to avoid rapid restart loop
            await asyncio.sleep(5)

    async def _run_reporting(self):
        """Run the reporting process for data persistence and health monitoring"""
        self.logger.info("Reporting process started")
        try:
            # Import the reporting flow function
            from arbirich.flows.reporting.reporting_flow import run_reporting_flow, stop_reporting_flow_async

            # Start the reporting flow task for persisting trading data
            reporting_flow_task = asyncio.create_task(run_reporting_flow(), name="reporting_flow")
            self.logger.info("Reporting flow for trade persistence started")

            # Periodically check database health while the flow runs
            while True:
                try:
                    # Check if the reporting flow task is still running
                    if reporting_flow_task.done():
                        exception = reporting_flow_task.exception()
                        if exception:
                            self.logger.error(f"Reporting flow task failed: {exception}")
                            # Restart the task
                            reporting_flow_task = asyncio.create_task(run_reporting_flow(), name="reporting_flow")
                            self.logger.info("Reporting flow task restarted")
                        else:
                            self.logger.warning("Reporting flow task completed unexpectedly")
                            # Restart the task
                            reporting_flow_task = asyncio.create_task(run_reporting_flow(), name="reporting_flow")
                            self.logger.info("Reporting flow task restarted")

                    # Run health check
                    import sqlalchemy as sa

                    from src.arbirich.services.database.database_service import DatabaseService

                    with DatabaseService() as db:
                        with db.engine.connect() as conn:
                            # Run a simple query to verify connection
                            result = conn.execute(sa.text("SELECT 1")).scalar()
                            self.logger.debug(f"Database health check: {result}")
                except Exception as e:
                    self.logger.error(f"Database health check failed: {e}")

                # Sleep before next check
                await asyncio.sleep(60)  # Check every minute

        except asyncio.CancelledError:
            self.logger.info("Reporting process stopping")
            # Cancel the reporting flow task if running
            if "reporting_flow_task" in locals() and not reporting_flow_task.done():
                try:
                    # Import here to avoid circular imports
                    from arbirich.flows.reporting.reporting_flow import stop_reporting_flow_async

                    # Clean stop of reporting flow
                    await stop_reporting_flow_async()

                    # Cancel the task
                    reporting_flow_task.cancel()
                    try:
                        await reporting_flow_task
                    except asyncio.CancelledError:
                        pass
                except Exception as e:
                    self.logger.error(f"Error stopping reporting flow: {e}")

            self.logger.info("Reporting process stopped")
            raise
        except Exception as e:
            self.logger.error(f"Error in reporting process: {e}", exc_info=True)
            # Sleep briefly to avoid rapid restart loop
            await asyncio.sleep(5)
            raise  # Re-raise to trigger restart
