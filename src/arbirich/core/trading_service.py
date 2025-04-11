import asyncio
import logging
import threading
import time

# Singleton instance and lock
_trading_service_instance = None
_instance_lock = threading.RLock()


def get_trading_service():
    """Get or create the singleton TradingService instance"""
    global _trading_service_instance
    with _instance_lock:
        if _trading_service_instance is None:
            _trading_service_instance = TradingService()
        return _trading_service_instance


class TradingService:
    def __new__(cls):
        """Ensure only one instance exists"""
        global _trading_service_instance
        with _instance_lock:
            if _trading_service_instance is None:
                _trading_service_instance = super(TradingService, cls).__new__(cls)
            return _trading_service_instance

    def __init__(self):
        # Only initialize once
        if hasattr(self, "logger"):
            return

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
                if hasattr(self.db, "fetch_all") and callable(self.db.fetch_all):
                    if asyncio.iscoroutinefunction(self.db.fetch_all):
                        strategies = await self.db.fetch_all("SELECT * FROM strategies")
                    else:
                        strategies = self.db.fetch_all("SELECT * FROM strategies")
                # Fallback to direct query using DatabaseService
                elif hasattr(self.db, "engine"):
                    with self.db.engine.connect() as conn:
                        result = conn.execute(self.db.tables.strategies.select())
                        strategies = [dict(row._mapping) for row in result]
                else:
                    raise ValueError("Invalid database connection or fetch_all method not available")
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
        """Start all system components"""
        try:
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

            await self.start_component("reporting")
            await self.start_component("execution")
            await self.start_component("arbitrage")
            await self.start_component("ingestion")

            self.logger.info("All system components started")
            return True  # Explicitly return True to indicate success
        except Exception as e:
            self.logger.error(f"Error starting all components: {e}")
            return False  # Return False if there was an error

    async def stop_all(self):
        """Stop all system components with extra cleanup"""
        self.logger.info("Stopping all system components")

        # Set system-wide shutdown flag
        try:
            from src.arbirich.core.system_state import mark_system_shutdown

            mark_system_shutdown(True)
            self.logger.info("System-wide shutdown flag set")
        except Exception as e:
            self.logger.error(f"Error setting system shutdown flag: {e}")

        # Then shutdown all processors
        try:
            from src.arbirich.services.exchange_processors.registry import shutdown_all_processors

            shutdown_all_processors()
            self.logger.info("All exchange processors signaled to stop")
        except Exception as e:
            self.logger.error(f"Error signaling exchange processors to stop: {e}")

        # Add a small delay to allow processors to notice the shutdown
        await asyncio.sleep(0.5)

        # Signal shutdown early to prevent new publishes
        try:
            from src.arbirich.flows.ingestion.ingestion_sink import mark_shutdown_started

            mark_shutdown_started()
        except Exception as e:
            self.logger.error(f"Error marking ingestion sink as shutting down: {e}")

        # 1. Stop components in the correct order
        for component in ["ingestion", "arbitrage", "execution", "reporting"]:
            try:
                await self.stop_component(component)
                self.logger.info(f"Stopped {component} component")
            except Exception as e:
                self.logger.error(f"Error stopping {component}: {e}")

        # 2. Give components time to completely finish their work
        self.logger.info("Waiting for components to finish cleanup...")
        await asyncio.sleep(2.5)

        # 3. Clean up any orphaned processes
        self._kill_orphaned_processes()

        # 4. Finally, close all Redis connections
        from src.arbirich.services.redis.redis_service import RedisService

        RedisService.close_all_connections()

        # 5. Ensure all components are marked as inactive
        self._reset_all_component_states()
        self.logger.info("All component states reset to inactive")

        self.logger.info("All system components stopped")
        return True

    # Add a new helper method to reset all component states
    def _reset_all_component_states(self):
        """Reset all component states to inactive"""
        for component_name in self.components:
            self.components[component_name]["active"] = False
            self.components[component_name]["task"] = None

        # Also mark all strategies as inactive
        for strategy_id in self.strategies:
            self.strategies[strategy_id]["active"] = False
            self.strategies[strategy_id]["task"] = None

        self.logger.info("All component and strategy states reset to inactive")

    async def restart_all_components(self):
        """Restart all trading components with clean state"""
        self.logger.info("Restarting all trading components with clean state...")

        # First stop everything
        await self.stop_all()

        # Reset processor states
        from src.arbirich.flows.ingestion.ingestion_source import reset_processor_states

        reset_processor_states()

        # Reset system state
        from src.arbirich.core.system_state import mark_system_shutdown, reset_notification_state

        mark_system_shutdown(False)
        reset_notification_state()

        self.logger.info("Refreshing trading service state from database...")
        # Clear existing strategy data
        self.strategies = {}
        # Re-initialize (will reload strategies from DB)
        await self.initialize()

        # Clear global flow manager registry
        from src.arbirich.flows.common.flow_manager import _flow_managers, _registry_lock

        with _registry_lock:
            self.logger.info("Clearing flow managers registry")
            _flow_managers.clear()

        # Reset Redis connections
        from src.arbirich.services.redis.redis_service import reset_redis_pool

        reset_redis_pool()
        self.logger.info("Redis connection pools reset")

        # Reset the channel maintenance service
        from arbirich.utils.channel_maintenance import reset_channel_maintenance_service

        reset_channel_maintenance_service()

        # Reset the background subscriber
        from arbirich.utils.background_subscriber import reset as reset_background_subscriber

        reset_background_subscriber()

        # Wait a moment to ensure clean state
        await asyncio.sleep(1)

        # Now start everything fresh
        self.logger.info("Starting all components with fresh state...")
        await self.start_all()
        self.logger.info("All components restarted successfully")

        return {"status": "restarted", "components": list(self.components.keys())}

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
            # Log more details for debugging
            self.logger.info(f"Cancelling task for component {component_name}")

            # Force cleanup based on component type
            if component_name == "reporting":
                from arbirich.flows.reporting.reporting_flow import stop_reporting_flow_async

                try:
                    await stop_reporting_flow_async()
                    self.logger.info("Reporting flow stopped via dedicated function")
                except Exception as e:
                    self.logger.error(f"Error stopping reporting flow: {e}")

            # Then cancel the task
            component["task"].cancel()
            try:
                # Wait with timeout for the task to cancel
                await asyncio.wait_for(asyncio.shield(component["task"]), timeout=3.0)
            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout waiting for {component_name} to stop, forcing cleanup")
            except asyncio.CancelledError:
                self.logger.info(f"Task for {component_name} cancelled successfully")
            except Exception as e:
                self.logger.error(f"Error while stopping {component_name}: {e}")

        # Always mark as inactive, even if there were errors
        component["active"] = False
        component["task"] = None

        self.logger.info(f"Component {component_name} marked as inactive")
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

        # After starting the strategy, verify its subscriptions
        subscription_verified = await self.verify_strategy_subscriptions(strategy["name"])

        if not subscription_verified:
            self.logger.warning(f"Strategy {strategy['name']} started but subscriptions could not be verified")

        # Refresh subscriptions if verification failed
        if not subscription_verified:
            self.logger.info(f"Attempting to refresh subscriptions for {strategy['name']}")
            # Trigger a restart of the specific flows for this strategy
            await asyncio.gather(self._restart_arbitrage_flow(), self._restart_execution_flow())

            # Verify again
            subscription_verified = await self.verify_strategy_subscriptions(strategy["name"], timeout=10)
            if subscription_verified:
                self.logger.info(f"Successfully refreshed subscriptions for {strategy['name']}")
            else:
                self.logger.error(f"Failed to establish subscriptions for {strategy['name']} after refresh")

        return True

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

    async def verify_strategy_subscriptions(self, strategy_name, timeout=30):
        """
        Verify that a strategy is properly subscribed to its required channels.

        Args:
            strategy_name: Name of the strategy to verify
            timeout: Maximum time to wait in seconds

        Returns:
            True if subscriptions are verified, False otherwise
        """
        from src.arbirich.config.config import get_strategy_config
        from src.arbirich.utils.channel_diagnostics import get_channel_subscribers

        self.logger.info(f"Verifying subscriptions for strategy: {strategy_name}")

        # Get the strategy config to determine which pairs it needs
        strategy_config = get_strategy_config(strategy_name)
        if not strategy_config:
            self.logger.error(f"Strategy config not found for {strategy_name}")
            return False

        # Get the exchanges and pairs this strategy needs
        exchanges = strategy_config.get("exchanges", [])
        pairs = strategy_config.get("pairs", [])

        # Calculate the expected channels
        expected_channels = []
        for exchange in exchanges:
            for base, quote in pairs:
                pair_symbol = f"{base}-{quote}"
                expected_channels.append(f"{exchange}:{pair_symbol}")

        self.logger.info(f"Expected channels for {strategy_name}: {expected_channels}")

        # Poll until we see subscriptions or timeout
        start_time = time.time()
        while time.time() - start_time < timeout:
            # Get current subscribers
            channel_subscribers = get_channel_subscribers()

            # Check if all expected channels have subscribers
            all_subscribed = True
            missing_channels = []

            for channel in expected_channels:
                if channel not in channel_subscribers or channel_subscribers[channel] == 0:
                    all_subscribed = False
                    missing_channels.append(channel)

            if all_subscribed:
                self.logger.info(f"All channels for {strategy_name} have subscribers")
                return True

            self.logger.warning(f"Still waiting for subscriptions to {missing_channels}")
            await asyncio.sleep(1.0)

        self.logger.error(f"Timeout waiting for {strategy_name} subscriptions. Missing: {missing_channels}")
        return False

    async def _run_ingestion(self):
        """Run the ingestion process"""
        self.logger.info("Ingestion process started")
        try:
            # Import here to avoid circular imports
            from arbirich.flows.ingestion.ingestion_flow import run_ingestion_flow
            from src.arbirich.config.config import EXCHANGES, PAIRS, get_strategy_config

            # Get exchanges and pairs for all active strategies
            exchanges_and_pairs = {}

            # First, get active strategies
            active_strategies = []
            for sid, strategy in self.strategies.items():
                if strategy.get("active", False):
                    active_strategies.append(strategy["name"])

            self.logger.info(f"Finding exchanges and pairs for active strategies: {active_strategies}")

            # For each active strategy, get the exchanges and pairs from config
            for strategy_name in active_strategies:
                # Get strategy config
                try:
                    strategy_config = get_strategy_config(strategy_name)
                    if not strategy_config:
                        self.logger.warning(f"No config found for strategy {strategy_name}")
                        continue

                    # Extract exchanges and pairs from config
                    strategy_exchanges = strategy_config.get("exchanges", [])
                    strategy_pairs = strategy_config.get("pairs", [])

                    # Use consistent format for all exchanges
                    for exchange in strategy_exchanges:
                        if exchange not in exchanges_and_pairs:
                            exchanges_and_pairs[exchange] = []

                        # Format pairs with consistent base-quote format
                        for base, quote in strategy_pairs:
                            # Always use the consistent format with hyphen
                            formatted_pair = f"{base}-{quote}"
                            if formatted_pair not in exchanges_and_pairs[exchange]:
                                exchanges_and_pairs[exchange].append(formatted_pair)

                        self.logger.info(
                            f"Added exchanges and pairs for strategy {strategy_name}: {exchange} with pairs {exchanges_and_pairs[exchange]}"
                        )
                except Exception as e:
                    self.logger.error(f"Error loading config for strategy {strategy_name}: {e}")

            # If no exchanges found from active strategies, use defaults
            if not exchanges_and_pairs:
                self.logger.warning("No exchanges found from active strategies, using defaults")

                # Use consistent format for all exchanges
                for exchange_name in EXCHANGES.keys():
                    exchanges_and_pairs[exchange_name] = []
                    # Format each pair with consistent base-quote format
                    for base, quote in PAIRS:
                        formatted_pair = f"{base}-{quote}"
                        exchanges_and_pairs[exchange_name].append(formatted_pair)

            # Start the ingestion pipeline
            self.logger.info(f"Starting ingestion pipeline with {len(exchanges_and_pairs)} exchanges...")
            self.logger.info(f"Exchanges and pairs: {exchanges_and_pairs}")

            # Run the ingestion flow with the gathered exchanges and pairs
            await run_ingestion_flow(exchanges_and_pairs=exchanges_and_pairs)

        except Exception as e:
            self.logger.error(f"Error in ingestion process: {e}", exc_info=True)
            # Sleep briefly to avoid rapid restart loop
            await asyncio.sleep(1)

    async def _run_arbitrage(self):
        """Run the arbitrage process"""
        self.logger.info("Arbitrage process started")
        try:
            # Reload strategies from the database to get current active status
            self.logger.info("Reloading strategies from database")
            try:
                # Check if we have a valid database connection
                if self.db is None:
                    self.logger.warning("Database connection not initialized, creating new connection")
                    from src.arbirich.services.database.database_service import DatabaseService

                    self.db = DatabaseService()

                # Get fresh strategy data from database
                with self.db.engine.connect() as conn:
                    result = conn.execute(self.db.tables.strategies.select())
                    strategies = [dict(row._mapping) for row in result]

                    # Update in-memory strategy data
                    for db_strategy in strategies:
                        strategy_id = db_strategy.get("id")
                        if strategy_id in self.strategies:
                            self.strategies[strategy_id]["active"] = db_strategy.get("is_active", False)
                            self.logger.info(
                                f"Strategy {self.strategies[strategy_id]['name']} active status: {self.strategies[strategy_id]['active']}"
                            )
            except Exception as e:
                self.logger.error(f"Error reloading strategies: {e}")

            # Launch arbitrage pipelines for all active strategies
            from arbirich.flows.arbitrage.arbitrage_flow import run_arbitrage_flow

            # Find active strategies
            active_strategies = []
            for sid, strategy in self.strategies.items():
                if strategy.get("active", False):
                    active_strategies.append({"id": sid, "name": strategy["name"]})
                    self.logger.info(f"Found active strategy: {strategy['name']} with ID {sid}")

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

                # Monitor the tasks instead of awaiting them directly
                try:
                    # Create a monitoring task
                    monitor_task = asyncio.create_task(
                        self._monitor_strategy_tasks(tasks, "arbitrage"), name="arbitrage-monitor"
                    )

                    # Wait for cancellation or any unexpected failure
                    await asyncio.gather(monitor_task)
                except asyncio.CancelledError:
                    # Cancel all tasks if this task is cancelled
                    self.logger.info("Arbitrage process stopping, cancelling all strategy tasks")
                    monitor_task.cancel()
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

    async def _monitor_strategy_tasks(self, tasks, process_name):
        """
        Monitor strategy tasks and restart them if they fail unexpectedly.

        Args:
            tasks: List of asyncio tasks to monitor
            process_name: Name of the process (e.g., "arbitrage", "execution")
        """
        while True:
            # Check each task
            for i, task in enumerate(tasks[:]):  # Use a copy of the list for iteration
                if task.done():
                    try:
                        # Get the result to see if there was an exception
                        result = task.result()
                        self.logger.warning(
                            f"{process_name} task {task.get_name()} completed unexpectedly with result: {result}"
                        )
                    except asyncio.CancelledError:
                        self.logger.info(f"{process_name} task {task.get_name()} was cancelled")
                    except Exception as e:
                        self.logger.error(f"{process_name} task {task.get_name()} failed with error: {e}")

                        # Try to restart the task
                        try:
                            # Extract strategy name from task name
                            task_name = task.get_name()
                            if "-" in task_name:
                                strategy_name = task_name.split("-", 1)[1]

                                if process_name == "arbitrage":
                                    from arbirich.flows.arbitrage.arbitrage_flow import run_arbitrage_flow

                                    new_task = asyncio.create_task(
                                        run_arbitrage_flow(strategy_name=strategy_name, debug_mode=True),
                                        name=f"arbitrage-{strategy_name}",
                                    )
                                elif process_name == "execution":
                                    from arbirich.flows.execution.execution_flow import run_execution_flow

                                    new_task = asyncio.create_task(
                                        run_execution_flow(strategy_name), name=f"execution-{strategy_name}"
                                    )

                                # Replace the task in the list
                                tasks[i] = new_task
                                self.logger.info(f"Restarted {process_name} task for strategy {strategy_name}")
                        except Exception as restart_error:
                            self.logger.error(f"Failed to restart {process_name} task: {restart_error}")

            # Check system shutdown
            from src.arbirich.core.system_state import is_system_shutting_down

            if is_system_shutting_down():
                self.logger.info(f"System shutdown detected in {process_name} monitor, stopping")
                break

            # Sleep before checking again
            await asyncio.sleep(5)

    async def _run_execution(self):
        """Run the execution process"""
        self.logger.info("Execution process started")
        try:
            # Ensure database connection is available
            if self.db is None:
                self.logger.warning("Database connection not initialized, creating new connection")
                from src.arbirich.services.database.database_service import DatabaseService

                self.db = DatabaseService()

            # Check for fresh strategy data
            try:
                with self.db.engine.connect() as conn:
                    result = conn.execute(self.db.tables.strategies.select())
                    strategies = [dict(row._mapping) for row in result]
                    self.logger.info(f"Loaded {len(strategies)} strategies from database")
                    # Update in-memory strategy data
                    for db_strategy in strategies:
                        strategy_id = db_strategy.get("id")
                        if strategy_id in self.strategies:
                            self.strategies[strategy_id]["active"] = db_strategy.get("is_active", False)
            except Exception as e:
                self.logger.error(f"Error updating strategies: {e}")

            # Launch execution pipelines for all active strategies
            from arbirich.flows.execution.execution_flow import run_execution_flow

            self.logger.info(f"STRATEGIES: {self.strategies}")
            # Find active strategies
            active_strategies = []
            for sid, strategy in self.strategies.items():
                if strategy.get("active", False):
                    active_strategies.append({"id": sid, "name": strategy["name"]})
                    self.logger.info(f"Found active strategy: {strategy['name']} with ID {sid}")

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

                # Monitor the tasks instead of awaiting them directly
                try:
                    # Create a monitoring task
                    monitor_task = asyncio.create_task(
                        self._monitor_strategy_tasks(tasks, "execution"), name="execution-monitor"
                    )

                    # Wait for cancellation or any unexpected failure
                    await asyncio.gather(monitor_task)
                except asyncio.CancelledError:
                    # Cancel all tasks if this task is cancelled
                    self.logger.info("Execution process stopping, cancelling all strategy tasks")
                    monitor_task.cancel()
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
                            # Check if system is shutting down before restarting
                            from src.arbirich.core.system_state import is_system_shutting_down

                            if is_system_shutting_down():
                                self.logger.info("System shutting down, not restarting reporting flow")
                                break

                            # Restart the task
                            reporting_flow_task = asyncio.create_task(run_reporting_flow(), name="reporting_flow")
                            self.logger.info("Reporting flow task restarted after error")
                        else:
                            self.logger.warning("Reporting flow task completed unexpectedly")
                            # Check if system is shutting down before restarting
                            from src.arbirich.core.system_state import is_system_shutting_down

                            if is_system_shutting_down():
                                self.logger.info("System shutting down, not restarting reporting flow")
                                break

                            # Restart the task
                            reporting_flow_task = asyncio.create_task(run_reporting_flow(), name="reporting_flow")
                            self.logger.info("Reporting flow task restarted after completion")

                    # Check if system is shutting down
                    from src.arbirich.core.system_state import is_system_shutting_down

                    if is_system_shutting_down():
                        self.logger.info("System shutdown detected in reporting monitor loop, stopping")
                        break

                    # Run health check
                    import sqlalchemy as sa

                    from src.arbirich.services.database.database_service import DatabaseService

                    with DatabaseService() as db:
                        with db.engine.connect() as conn:
                            # Run a simple query to verify connection
                            result = conn.execute(sa.text("SELECT 1")).scalar()
                            self.logger.debug(f"Database health check: {result}")
                except asyncio.CancelledError:
                    raise  # Re-raise to handle properly in the outer try/except
                except Exception as e:
                    self.logger.error(f"Error in reporting monitoring loop: {e}")

                # Sleep before next check
                await asyncio.sleep(60)  # Check every minute

        except asyncio.CancelledError:
            self.logger.info("Reporting process stopping")
            # Cancel the reporting flow task if running
            if "reporting_flow_task" in locals() and not reporting_flow_task.done():
                try:
                    # Import here to avoid circular imports
                    from arbirich.flows.reporting.reporting_flow import stop_reporting_flow, stop_reporting_flow_async

                    # Use sync version for more immediate effect
                    try:
                        stop_reporting_flow()
                        self.logger.info("Reporting flow stopped synchronously")
                    except Exception as e:
                        self.logger.error(f"Sync stop of reporting flow failed: {e}")
                        # Fall back to async version
                        await stop_reporting_flow_async()
                        self.logger.info("Reporting flow stopped asynchronously")

                    # Cancel the task after flow stop
                    reporting_flow_task.cancel()
                    try:
                        await asyncio.wait_for(reporting_flow_task, timeout=2.0)
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        self.logger.info("Reporting task cancellation completed or timed out")
                except Exception as e:
                    self.logger.error(f"Error stopping reporting flow: {e}")

            self.logger.info("Reporting process stopped")
            raise
        except Exception as e:
            self.logger.error(f"Error in reporting process: {e}", exc_info=True)
            # Sleep briefly to avoid rapid restart loop
            await asyncio.sleep(5)
            raise  # Re-raise to trigger restart

    def _kill_orphaned_processes(self):
        """Kill any orphaned bytewax processes that didn't shut down properly"""
        try:
            import os
            import signal
            import subprocess

            self.logger.info("Checking for orphaned bytewax processes")

            # Find all Python processes running bytewax
            result = subprocess.run(["ps", "-ef"], capture_output=True, text=True)
            killed_count = 0

            for line in result.stdout.splitlines():
                # Look for python processes that contain both "bytewax" and "arbirich"
                if "python" in line and "bytewax" in line and "arbirich" in line:
                    parts = line.split()
                    if len(parts) > 1:
                        try:
                            pid = int(parts[1])
                            # Don't kill our own process or parent processes
                            if pid != os.getpid() and pid != os.getppid():
                                self.logger.info(f"Killing orphaned bytewax process: {pid}")
                                # Use SIGKILL for guaranteed termination
                                os.kill(pid, signal.SIGKILL)
                                killed_count += 1
                        except (ValueError, ProcessLookupError) as e:
                            self.logger.error(f"Failed to parse or kill process from line: {line}, error: {e}")

            if killed_count > 0:
                self.logger.warning(f"Killed {killed_count} orphaned bytewax processes")
            else:
                self.logger.info("No orphaned bytewax processes found")

        except Exception as e:
            self.logger.error(f"Error checking for orphaned processes: {e}", exc_info=True)
