import asyncio
import logging
from typing import Any, Dict, List, Optional

from src.arbirich.constants import TRADE_EXECUTIONS_CHANNEL, TRADE_OPPORTUNITIES_CHANNEL
from src.arbirich.core.trading.components.base import Component
from src.arbirich.core.trading.flows.flow_manager import FlowManager
from src.arbirich.core.trading.flows.reporting.message_processor import process_redis_messages
from src.arbirich.core.trading.flows.reporting.redis_client import safe_close_pubsub
from src.arbirich.core.trading.flows.reporting.tasks import monitor_health, persist_data, report_performance
from src.arbirich.models.models import Strategy
from src.arbirich.services.database.repositories.strategy_repository import StrategyRepository
from src.arbirich.services.redis.redis_service import initialize_redis, reset_redis_connection

logger = logging.getLogger(__name__)


class ReportingComponent(Component):
    """
    Reporting component for data persistence and monitoring using simple async tasks.
    """

    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self.update_interval = config.get("update_interval", 60.0)
        self.flow_configs = config.get("flows", {})
        self.tasks = {}
        self.stop_event = asyncio.Event()
        self.strategy_repository = None
        self.strategies = {}
        self.active_channels = set()
        self.refresh_interval = config.get("refresh_interval", 300.0)  # 5 minutes default

    async def initialize(self) -> bool:
        """Initialize reporting configurations"""
        try:
            # Initialize the Database connection
            from src.arbirich.services.database.database_service import DatabaseService

            # Get a database connection from the service
            db_service = DatabaseService()

            # Create the strategy repository
            self.strategy_repository = StrategyRepository(engine=db_service.engine)

            # Get active strategies using the repository
            active_strategies = self._get_active_strategies()

            # Store strategies info
            for strategy in active_strategies:
                strategy_id = str(strategy.id)
                strategy_name = strategy.name

                self.strategies[strategy_id] = {"id": strategy_id, "name": strategy_name, "active": True}

            # Configure different types of reporting tasks
            self.reporting_types = [
                {"id": f"persistence_{self.name}", "type": "data_persistence"},
                {"id": f"health_{self.name}", "type": "health_monitoring"},
                {"id": f"performance_{self.name}", "type": "performance_reporting"},
                {"id": f"strategy_refresh_{self.name}", "type": "strategy_refresh"},
                {"id": f"stream_processor_{self.name}", "type": "redis_stream_processor"},  # Add stream processor
            ]

            # Initialize the reporting flow using the imported module
            from src.arbirich.core.trading.flows.reporting.reporting_flow import build_reporting_flow

            # Create a flow manager for reporting
            self.flow_manager = FlowManager.get_or_create(f"reporting_flow_{self.name}")

            # Configure the flow builder
            self.flow_manager.set_flow_builder(lambda: build_reporting_flow(flow_type="performance"))

            # Create flow configuration with dynamic channels based on active strategies
            self.flow_config = {
                "id": f"reporting_flow_{self.name}",
                "channels": self._get_strategy_channels(),
                "active": True,
            }

            # Initialize Redis connection for stream processing with retry logic
            max_retries = 3
            redis_init_success = False

            for attempt in range(1, max_retries + 1):
                try:
                    logger.info(f"Initializing Redis for reporting component (attempt {attempt}/{max_retries})")
                    self.redis_client, self.pubsub = await initialize_redis(self.flow_config.get("channels", []))

                    if self.redis_client and self.pubsub:
                        logger.info("Successfully initialized Redis for reporting component")
                        redis_init_success = True
                        break
                    else:
                        logger.warning(f"Redis initialization returned None values (attempt {attempt}/{max_retries})")
                        # Try alternative initialization if shared client failed
                        if attempt == max_retries - 1:
                            logger.info("Attempting alternative Redis initialization")
                            try:
                                # Import here to avoid circular imports
                                import redis

                                from src.arbirich.config.config import REDIS_CONFIG
                                from src.arbirich.services.redis.redis_service import RedisService

                                # Try direct initialization
                                direct_redis = redis.Redis(
                                    host=REDIS_CONFIG.get("host", "localhost"),
                                    port=REDIS_CONFIG.get("port", 6379),
                                    db=REDIS_CONFIG.get("db", 0),
                                    decode_responses=True,
                                )

                                if direct_redis.ping():
                                    # Create service wrapper
                                    self.redis_client = RedisService()
                                    self.redis_client.client = direct_redis

                                    # Create pubsub
                                    self.pubsub = self.redis_client.client.pubsub(ignore_subscribe_messages=True)

                                    # Subscribe to channels
                                    channels = self.flow_config.get("channels", [])
                                    for channel in channels:
                                        self.pubsub.subscribe(channel)

                                    logger.info("Successfully created direct Redis connection")
                                    redis_init_success = True
                                    break
                            except Exception as direct_error:
                                logger.error(f"Direct Redis initialization failed: {direct_error}")

                except Exception as e:
                    logger.error(f"Redis initialization error (attempt {attempt}): {e}")

                # Wait before retrying
                await asyncio.sleep(1)

            # Check if initialization succeeded
            if not redis_init_success:
                logger.error("Failed to initialize Redis connection after multiple attempts")
                return False

            # Store the current set of active channels
            self.active_channels = set(self.flow_config.get("channels", []))

            # Initialize timing attributes for health checks
            self._last_activity = 0
            self._next_log_time = 0

            logger.info(f"Initialized {len(self.reporting_types)} reporting tasks and flow")
            logger.info(
                f"Monitoring {len(self.strategies)} active strategies with {len(self.active_channels)} channels"
            )
            return True

        except Exception as e:
            logger.error(f"Error initializing reporting: {e}", exc_info=True)
            return False

    def _get_active_strategies(self) -> List[Strategy]:
        """Get active strategies from the repository"""
        try:
            if self.strategy_repository is None:
                logger.error("Strategy repository not initialized")
                return []

            return self.strategy_repository.get_active()
        except Exception as e:
            logger.error(f"Error retrieving active strategies: {e}", exc_info=True)
            return []

    def _get_strategy_by_name(self, strategy_name: str) -> Optional[Strategy]:
        """Get a strategy by name from the repository"""
        try:
            if self.strategy_repository is None:
                logger.error("Strategy repository not initialized")
                return None

            return self.strategy_repository.get_by_name(strategy_name)
        except Exception as e:
            logger.error(f"Error retrieving strategy by name {strategy_name}: {e}", exc_info=True)
            return None

    def _get_strategy_channels(self) -> List[str]:
        """Get Redis channels for all active strategies"""
        # Start with the main channels
        channels = [
            TRADE_OPPORTUNITIES_CHANNEL,
            TRADE_EXECUTIONS_CHANNEL,
        ]

        # Add strategy-specific channels for each active strategy
        for strategy_id, strategy_info in self.strategies.items():
            strategy_name = strategy_info["name"]
            channels.append(f"{TRADE_OPPORTUNITIES_CHANNEL}:{strategy_name}")
            channels.append(f"{TRADE_EXECUTIONS_CHANNEL}:{strategy_name}")
            logger.debug(f"Added channels for strategy: {strategy_name}")

        return channels

    async def run(self) -> None:
        """Run reporting tasks as simple async coroutines"""
        logger.info("Starting reporting tasks")
        self.active = True

        try:
            # Start the reporting flow using the flow manager
            try:
                await self.flow_manager.run_flow()
                logger.info("Started reporting flow")
            except Exception as e:
                logger.error(f"Error starting reporting flow: {e}", exc_info=True)

            # Start all reporting tasks
            for report_config in self.reporting_types:
                task = asyncio.create_task(self._run_reporting_task(report_config["id"], report_config["type"]))
                self.tasks[report_config["id"]] = task
                logger.info(f"Started reporting task: {report_config['id']}")

            # Wait for all tasks to complete (they shouldn't unless canceled)
            pending = list(self.tasks.values())
            while self.active and pending:
                done, pending = await asyncio.wait(pending, timeout=10, return_when=asyncio.FIRST_COMPLETED)

                # Check if any tasks completed (which is unexpected)
                for task in done:
                    task_id = next((k for k, v in self.tasks.items() if v == task), "unknown")
                    if task.exception():
                        logger.error(f"Reporting task {task_id} failed: {task.exception()}")
                    else:
                        logger.warning(f"Reporting task {task_id} completed unexpectedly")

                    # Restart the task if it failed
                    if self.active:
                        report_type = next((r["type"] for r in self.reporting_types if r["id"] == task_id), "unknown")
                        new_task = asyncio.create_task(self._run_reporting_task(task_id, report_type))
                        self.tasks[task_id] = new_task
                        pending.append(new_task)
                        logger.info(f"Restarted reporting task: {task_id}")

        except asyncio.CancelledError:
            logger.info("Reporting component cancelled")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in reporting: {e}", exc_info=True)
            if not self.handle_error(e):
                self.active = False
        finally:
            await self.cleanup()

    async def _run_reporting_task(self, task_id: str, report_type: str) -> None:
        """Run a specific reporting task"""
        logger.info(f"Starting reporting task {task_id} of type {report_type}")

        if report_type == "redis_stream_processor":
            logger.info(f"🔄 Initializing Redis stream processor task: {task_id}")

        while self.active and not self.stop_event.is_set():
            try:
                if report_type == "data_persistence":
                    await persist_data()
                elif report_type == "health_monitoring":
                    await monitor_health()
                elif report_type == "performance_reporting":
                    await report_performance()
                elif report_type == "strategy_refresh":
                    # This is a new task that refreshes the active strategies
                    await self._refresh_active_strategies()
                elif report_type == "redis_stream_processor":
                    # This replaces the Bytewax flow functionality
                    logger.info("⚡ Starting Redis stream processor run")
                    await process_redis_messages(
                        self.pubsub,
                        self.redis_client,
                        self.active,
                        self.stop_event,
                        self.flow_config.get("debug_mode", True),  # Set debug mode to True by default
                    )
                    # This task has its own loop, so it shouldn't reach the sleep below
                    continue
                else:
                    logger.warning(f"Unknown report type: {report_type}")

                # Use different intervals for different tasks
                timeout = self.refresh_interval if report_type == "strategy_refresh" else self.update_interval

                # Wait for next interval
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=timeout)
                except asyncio.TimeoutError:
                    # This is expected - it means we waited for update_interval and no stop event occurred
                    pass

            except asyncio.CancelledError:
                logger.info(f"Reporting task {task_id} cancelled")
                raise
            except Exception as e:
                logger.error(f"Error in reporting task {task_id}: {e}", exc_info=True)
                # Back off on errors to avoid rapid failure cycles
                await asyncio.sleep(min(self.update_interval * 2, 300))

    async def _refresh_active_strategies(self) -> None:
        """Refresh the list of active strategies and update Redis subscriptions if needed"""
        try:
            logger.debug("Refreshing active strategies for reporting")

            # Get current active strategies
            current_strategies = self._get_active_strategies()

            # Track changes
            strategies_changed = False

            # Create a dictionary of current strategy IDs for quick lookup
            current_strategy_ids = {str(strategy.id): strategy for strategy in current_strategies}

            # Check for strategies that have been deactivated
            strategies_to_remove = []
            for strategy_id in self.strategies:
                if strategy_id not in current_strategy_ids:
                    strategy_name = self.strategies[strategy_id]["name"]
                    logger.info(f"Strategy {strategy_name} has been deactivated")
                    strategies_to_remove.append(strategy_id)
                    strategies_changed = True

            # Remove deactivated strategies
            for strategy_id in strategies_to_remove:
                del self.strategies[strategy_id]

            # Check for new active strategies
            for strategy_id, strategy in current_strategy_ids.items():
                if strategy_id not in self.strategies:
                    strategy_name = strategy.name
                    logger.info(f"New active strategy detected: {strategy_name}")

                    # Add to strategies dict
                    self.strategies[strategy_id] = {"id": strategy_id, "name": strategy_name, "active": True}
                    strategies_changed = True

            # If strategies have changed, update Redis subscriptions
            if strategies_changed:
                await self._update_redis_subscriptions()

        except Exception as e:
            logger.error(f"Error refreshing active strategies: {e}", exc_info=True)

    async def _update_redis_subscriptions(self) -> None:
        """Update Redis subscriptions based on current active strategies"""
        try:
            # Get new channels based on current strategies
            new_channels = set(self._get_strategy_channels())

            # Nothing to do if channels haven't changed
            if new_channels == self.active_channels:
                return

            # Find channels to add and remove
            channels_to_add = new_channels - self.active_channels
            channels_to_remove = self.active_channels - new_channels

            logger.info(
                f"Updating Redis subscriptions: adding {len(channels_to_add)}, removing {len(channels_to_remove)}"
            )

            # If we have channels to remove, unsubscribe
            if channels_to_remove:
                for channel in channels_to_remove:
                    try:
                        await self.pubsub.unsubscribe(channel)
                        logger.debug(f"Unsubscribed from channel: {channel}")
                    except Exception as e:
                        logger.error(f"Error unsubscribing from channel {channel}: {e}")

            # If we have channels to add, subscribe
            if channels_to_add:
                for channel in channels_to_add:
                    try:
                        await self.pubsub.subscribe(channel)
                        logger.debug(f"Subscribed to channel: {channel}")
                    except Exception as e:
                        logger.error(f"Error subscribing to channel {channel}: {e}")

            # Update active channels
            self.active_channels = new_channels

            # Update flow config
            self.flow_config["channels"] = list(self.active_channels)

            logger.info(f"Updated Redis subscriptions, now monitoring {len(self.active_channels)} channels")

        except Exception as e:
            logger.error(f"Error updating Redis subscriptions: {e}", exc_info=True)

    async def cleanup(self) -> None:
        """Clean up resources used by the reporting component."""
        try:
            logger.info("Cleaning up reporting component")

            # First, safely close Redis PubSub connections
            try:
                await safe_close_pubsub()
            except Exception as e:
                logger.error(f"Error closing Redis PubSub: {e}")

            # Stop the reporting flow
            if hasattr(self, "flow_manager"):
                try:
                    await self.flow_manager.stop_flow_async()
                    logger.info("Stopped reporting flow")
                except Exception as e:
                    logger.error(f"Error stopping reporting flow: {e}", exc_info=True)

            # Cancel all tasks
            for task_id, task in self.tasks.items():
                if not task.done():
                    task.cancel()
                    logger.info(f"Cancelled reporting task: {task_id}")

            # Wait for tasks to be cancelled
            if self.tasks:
                await asyncio.wait(list(self.tasks.values()))

            # Close Redis connection
            if hasattr(self, "pubsub") and self.pubsub:
                try:
                    await self.pubsub.unsubscribe()
                    self.pubsub.close()
                    logger.info("Closed Redis PubSub connection")
                except Exception as e:
                    logger.error(f"Error closing Redis PubSub: {e}")

            # Reset shared Redis client
            reset_redis_connection()

            # Clear state
            self.tasks = {}
            self.strategies = {}
            self.active_channels = set()
            self.strategy_repository = None

        except Exception as e:
            logger.error(f"Error cleaning up reporting component: {e}")

    def handle_error(self, error: Exception) -> bool:
        """Handle component errors, return True if error was handled"""
        # Override the base class method
        logger.error(f"Error in reporting component: {error}", exc_info=True)
        # Always attempt to continue despite errors
        return True

    async def stop(self) -> bool:
        """
        Signal component to stop.
        Override the base class method but call it to ensure proper cleanup.

        Returns:
            bool: True if the component was stopped successfully
        """
        logger.info("Stopping reporting component")
        self.stop_event.set()

        # Set system shutdown flag if available
        try:
            from src.arbirich.core.state.system_state import mark_system_shutdown

            mark_system_shutdown(True)
            logger.info("System shutdown flag set")
        except Exception as e:
            logger.error(f"Error setting system shutdown flag: {e}")

        # Use the base class stop method for standard cleanup
        return await super().stop()
