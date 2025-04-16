import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional

from src.arbirich.services.database.database_service import DatabaseService


class ReportingService:
    """
    Service responsible for handling trade reporting, analytics, and performance metrics.
    This service acts as the core implementation shared by both ReportingFlow and ReportingComponent.
    """

    def __init__(self, config=None):
        """
        Initialize the reporting service.

        Args:
            config (dict, optional): Configuration for the reporting service.
        """
        self.logger = logging.getLogger(__name__)
        self.config = config or {}
        self.initialized = False
        self.db_service = None
        self.redis_client = None
        self.pubsub = None
        self.update_interval = self.config.get("update_interval", 60.0)
        self.tasks = {}
        self.stop_event = asyncio.Event()
        self.active = False
        self.active_channels = set()

        # Callbacks for integration with parent components
        self.on_error = None
        self.on_message_processed = None

    def initialize(self):
        """
        Initialize reporting components and connections.
        """
        self.logger.info("Initializing reporting service")

        # Initialize database service
        try:
            self.db_service = DatabaseService()
            self.logger.info("Database service initialized")
        except Exception as e:
            self.logger.error(f"Failed to initialize database service: {e}")
            return False

        # Initialize Redis connection if needed
        if self.config.get("use_redis", True):  # Default to True to maintain compatibility
            try:
                from src.arbirich.core.trading.flows.reporting.redis_client import get_redis_with_pubsub
                from src.arbirich.services.redis.redis_service import get_shared_redis_client

                # First try the specialized Redis client for reporting
                self.redis_client, self.pubsub = get_redis_with_pubsub()

                # Fall back to the shared client if needed
                if not self.redis_client:
                    self.redis_client = get_shared_redis_client()
                    if self.redis_client and hasattr(self.redis_client, "client"):
                        self.pubsub = self.redis_client.client.pubsub(ignore_subscribe_messages=True)

                # Log the result
                if self.redis_client and self.pubsub:
                    self.logger.info("Redis client and PubSub initialized")
                else:
                    self.logger.warning("Could not initialize Redis client or PubSub")
            except Exception as e:
                self.logger.error(f"Failed to initialize Redis connection: {e}")

        self.initialized = True
        self.logger.info("Reporting service initialized successfully")
        return True

    async def subscribe_to_channels(self, channels):
        """
        Subscribe to Redis channels.

        Args:
            channels: List of channel names to subscribe to

        Returns:
            bool: True if successful, False otherwise
        """
        if not self.pubsub:
            self.logger.warning("Cannot subscribe to channels - PubSub not initialized")
            return False

        try:
            # Track which new channels we're adding
            new_channels = set(channels) - self.active_channels

            # Subscribe to each new channel
            for channel in new_channels:
                try:
                    self.pubsub.subscribe(channel)
                    self.logger.debug(f"Subscribed to channel: {channel}")
                except Exception as e:
                    self.logger.error(f"Error subscribing to channel {channel}: {e}")

            # Update active channels
            self.active_channels.update(new_channels)
            self.logger.info(
                f"Subscribed to {len(new_channels)} new channels, now monitoring {len(self.active_channels)} total"
            )
            return True
        except Exception as e:
            self.logger.error(f"Error subscribing to channels: {e}")
            return False

    async def start(self):
        """
        Start the reporting service and all reporting tasks.
        """
        if not self.initialized:
            success = self.initialize()
            if not success:
                self.logger.error("Failed to initialize reporting service")
                return False

        self.active = True

        # Start reporting tasks based on configuration
        if self.config.get("enable_health_monitoring", True):
            self.tasks["health_monitoring"] = asyncio.create_task(self.monitor_health())

        if self.config.get("enable_performance_reporting", True):
            self.tasks["performance_reporting"] = asyncio.create_task(self.report_performance())

        if self.config.get("enable_data_persistence", True):
            self.tasks["data_persistence"] = asyncio.create_task(self.persist_data())

        if self.config.get("enable_redis_processing", True) and self.pubsub:
            self.tasks["redis_processing"] = asyncio.create_task(self.process_redis_messages())

        self.logger.info(f"Reporting service started with {len(self.tasks)} active tasks")
        return True

    async def stop(self):
        """
        Stop the reporting service and clean up resources.
        """
        self.logger.info("Stopping reporting service")
        self.active = False
        self.stop_event.set()

        # Cancel all tasks
        for task_name, task in self.tasks.items():
            if not task.done():
                task.cancel()
                self.logger.info(f"Cancelled {task_name} task")

        # Wait for tasks to complete with timeout
        if self.tasks:
            try:
                await asyncio.wait(list(self.tasks.values()), timeout=2.0)
            except Exception as e:
                self.logger.error(f"Error waiting for tasks to complete: {e}")

        # Clean up Redis resources
        if self.pubsub:
            try:
                self.pubsub.unsubscribe()
                self.pubsub.close()
                self.logger.info("Closed Redis PubSub")
            except Exception as e:
                self.logger.error(f"Error closing Redis PubSub: {e}")

        # Clean up the Redis client
        if self.redis_client and hasattr(self.redis_client, "close"):
            try:
                self.redis_client.close()
                self.logger.info("Closed Redis client")
            except Exception as e:
                self.logger.error(f"Error closing Redis client: {e}")

        self.tasks = {}
        self.logger.info("Reporting service stopped")
        return True

    async def monitor_health(self):
        """
        Monitor system health periodically.
        """
        while self.active and not self.stop_event.is_set():
            try:
                # Check database connection
                db_healthy = False
                with self.db_service as db:
                    try:
                        import sqlalchemy as sa

                        with db.engine.connect() as conn:
                            result = conn.execute(sa.text("SELECT 1")).scalar()
                            db_healthy = result == 1
                    except Exception as db_e:
                        self.logger.warning(f"Database connection test failed: {db_e}")

                # Check Redis connection if available
                redis_healthy = self.check_redis_health()

                # Log health status
                if not db_healthy or not redis_healthy:
                    self.logger.warning(f"Health check: DB={db_healthy}, Redis={redis_healthy}")
                else:
                    self.logger.debug("All systems healthy")

                # Wait for next check interval
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=self.update_interval)
                except asyncio.TimeoutError:
                    pass  # Expected when timeout occurs

            except asyncio.CancelledError:
                self.logger.info("Health monitoring task cancelled")
                raise
            except Exception as e:
                self.logger.error(f"Error in health monitoring: {e}")
                if self.on_error:
                    self.on_error(e, "health_monitoring")
                await asyncio.sleep(min(self.update_interval * 2, 300))  # Back off on errors

    async def report_performance(self):
        """
        Generate performance reports periodically.
        """
        while self.active and not self.stop_event.is_set():
            try:
                # Get basic stats from database
                with self.db_service as db:
                    # Get strategies
                    strategies = db.get_all_strategies()

                    # Log basic performance metrics
                    for strategy in strategies:
                        if hasattr(strategy, "net_profit"):
                            self.logger.info(f"Strategy {strategy.name}: Net profit = {strategy.net_profit}")

                        # Get executions for this strategy
                        try:
                            executions = db.get_executions_by_strategy(strategy.name)
                            self.logger.info(f"Strategy {strategy.name}: {len(executions)} executions")
                        except Exception as exec_e:
                            self.logger.warning(f"Error getting executions for {strategy.name}: {exec_e}")

                self.logger.debug("Performance reporting completed")

                # Wait for next report interval
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=self.update_interval)
                except asyncio.TimeoutError:
                    pass  # Expected when timeout occurs

            except asyncio.CancelledError:
                self.logger.info("Performance reporting task cancelled")
                raise
            except Exception as e:
                self.logger.error(f"Error in performance reporting: {e}")
                if self.on_error:
                    self.on_error(e, "performance_reporting")
                await asyncio.sleep(min(self.update_interval * 2, 300))  # Back off on errors

    async def persist_data(self):
        """
        Persist data to the database periodically.
        """
        while self.active and not self.stop_event.is_set():
            try:
                # Verify database connection
                with self.db_service as db:
                    if db.engine is None:
                        self.logger.error("Database connection not available for persistence task")
                        await asyncio.sleep(self.update_interval)
                        continue

                    # Log successful database connection
                    self.logger.info("Database connection successful for persistence task")

                self.logger.info("Data persistence task completed successfully")

                # Wait for next persistence interval
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=self.update_interval)
                except asyncio.TimeoutError:
                    pass  # Expected when timeout occurs

            except asyncio.CancelledError:
                self.logger.info("Data persistence task cancelled")
                raise
            except Exception as e:
                self.logger.error(f"Error in data persistence: {e}")
                if self.on_error:
                    self.on_error(e, "data_persistence")
                await asyncio.sleep(min(self.update_interval * 2, 300))  # Back off on errors

    async def process_redis_messages(self):
        """
        Process messages from Redis PubSub channels.
        """
        if not self.pubsub:
            self.logger.error("Cannot process Redis messages - PubSub not initialized")
            return

        # Initialize tracking variables
        last_activity = time.time()
        next_log_time = time.time() + 300  # First log after 5 minutes
        debug_mode = self.config.get("debug_mode", False)

        self.logger.info("Starting Redis message processing loop")

        try:
            from src.arbirich.core.trading.flows.reporting.message_processor import process_message
            from src.arbirich.services.redis.redis_service import check_redis_health

            while self.active and not self.stop_event.is_set():
                current_time = time.time()

                # Periodic health check
                if current_time - last_activity > 60:  # 1 minute timeout
                    self.logger.info("Performing Redis health check")
                    last_activity = current_time

                    channels = list(self.active_channels)

                    # Check Redis health
                    if self.redis_client:
                        if not await check_redis_health(self.redis_client, self.pubsub, channels):
                            # Wait before trying again
                            await asyncio.sleep(5.0)
                            continue
                    else:
                        self.logger.warning("Redis client is None, skipping health check")
                        # Try to reconnect
                        from src.arbirich.core.trading.flows.reporting.redis_client import get_shared_redis_client

                        self.redis_client = get_shared_redis_client()
                        if not self.redis_client:
                            await asyncio.sleep(5.0)
                            continue

                # Periodic logging
                if current_time > next_log_time:
                    self.logger.debug("Reporting service is active and waiting for messages")
                    next_log_time = current_time + 300  # Log every 5 minutes

                # Get the next message with a timeout
                message = await self._get_next_message()

                if message is None:
                    continue

                # Process the message
                try:
                    # Get channel and handle different types properly
                    channel = message.get("channel", "")
                    if isinstance(channel, bytes):
                        channel = channel.decode("utf-8")
                    elif not isinstance(channel, str):
                        # Convert to string if it's neither bytes nor string
                        channel = str(channel)

                    data = message.get("data")

                    if isinstance(data, bytes):
                        try:
                            # Process binary data - often JSON
                            data = json.loads(data.decode("utf-8"))
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            # If not JSON, just keep as string
                            data = data.decode("utf-8", errors="replace")

                    # Skip subscription confirmation messages
                    if isinstance(data, (int, str)) and data in (1, 2, "1", "2"):
                        continue

                    # Process the message
                    result = await process_message(channel, data)

                    # Update activity timestamp
                    last_activity = time.time()

                    # Notify parent component if callback is registered
                    if self.on_message_processed and result:
                        self.on_message_processed(channel, result)

                    # Debug output
                    if debug_mode and result:
                        self.logger.debug(f"Processed message from {channel}: {result}")

                except Exception as msg_error:
                    self.logger.error(f"Error processing message: {msg_error}")
                    if self.on_error:
                        self.on_error(msg_error, "redis_message_processing")

        except asyncio.CancelledError:
            self.logger.info("Redis message processing cancelled")
            raise
        except Exception as e:
            self.logger.error(f"Error in Redis message processing loop: {e}")
            if self.on_error:
                self.on_error(e, "redis_message_processing")

    async def _get_next_message(self) -> Optional[Dict]:
        """Get the next message from Redis PubSub with async support"""
        try:
            # Check if we have a message available
            message = self.pubsub.get_message(timeout=0.01)

            if message and message.get("type") == "message":
                return message

            # No message immediately available, use asyncio to wait a bit
            # without blocking the event loop
            await asyncio.sleep(0.1)
            return None

        except Exception as e:
            self.logger.error(f"Error getting Redis message: {e}")
            return None

    def generate_report(self, report_type, data, **kwargs):
        """
        Generate a report of the specified type.

        Args:
            report_type (str): Type of report to generate
            data (dict): Data to include in the report
            **kwargs: Additional parameters for report generation

        Returns:
            dict: The generated report
        """
        if not self.initialized:
            self.logger.warning("Reporting service not initialized")
            return None

        self.logger.info(f"Generating {report_type} report")

        # Implement specific report generation based on report_type
        if report_type == "strategy_performance":
            return self._generate_strategy_performance_report(data, **kwargs)
        elif report_type == "system_health":
            return self._generate_system_health_report(data, **kwargs)
        elif report_type == "trade_execution":
            return self._generate_trade_execution_report(data, **kwargs)
        else:
            # Generic report handling
            return {"type": report_type, "data": data}

    def _generate_strategy_performance_report(self, data, **kwargs):
        """Generate a strategy performance report"""
        # Implementation for strategy performance report
        report = {
            "type": "strategy_performance",
            "timestamp": datetime.now().isoformat(),
            "data": data,
        }

        # Add additional calculations or formatting here

        return report

    def _generate_system_health_report(self, data, **kwargs):
        """Generate a system health report"""
        # Implementation for system health report
        report = {
            "type": "system_health",
            "timestamp": datetime.now().isoformat(),
            "data": data,
        }

        return report

    def _generate_trade_execution_report(self, data, **kwargs):
        """Generate a trade execution report"""
        # Implementation for trade execution report
        report = {
            "type": "trade_execution",
            "timestamp": datetime.now().isoformat(),
            "data": data,
        }

        return report

    async def get_trading_statistics(self, start_date=None, end_date=None):
        """
        Get trading statistics for a specified time period.

        Args:
            start_date: The start date for statistics (default: 24 hours ago)
            end_date: The end date for statistics (default: now)

        Returns:
            Dictionary with trading statistics
        """
        if not start_date:
            start_date = datetime.now() - timedelta(days=1)
        if not end_date:
            end_date = datetime.now()

        try:
            # Verify database connection
            with self.db_service as db:
                # Collect statistics here
                stats = {
                    "total_profit": 0.0,
                    "total_trades": 0,
                    "successful_trades": 0,
                    "failed_trades": 0,
                    "start_time": start_date.timestamp(),
                    "end_time": end_date.timestamp(),
                    "strategy_performance": {},
                    "pair_performance": {},
                    "exchange_performance": {},
                }

                # Calculate statistics for each strategy
                strategies = db.get_all_strategies()
                for strategy in strategies:
                    strategy_id = strategy.id
                    strategy_name = strategy.name

                    # Get metrics for this strategy
                    metrics = db.get_latest_strategy_metrics(strategy_id)
                    if metrics:
                        stats["strategy_performance"][strategy_name] = {
                            "profit": getattr(metrics, "net_profit", 0.0),
                            "trades": getattr(metrics, "win_count", 0) + getattr(metrics, "loss_count", 0),
                            "win_rate": getattr(metrics, "win_rate", 0.0),
                            "volume": getattr(metrics, "total_volume", 0.0),
                            "profit_factor": getattr(metrics, "profit_factor", 0.0),
                            "period_start": getattr(metrics, "period_start", None),
                            "period_end": getattr(metrics, "period_end", None),
                        }

                        # Aggregate totals
                        stats["total_profit"] += float(getattr(metrics, "net_profit", 0.0))
                        stats["total_trades"] += getattr(metrics, "win_count", 0) + getattr(metrics, "loss_count", 0)
                        stats["successful_trades"] += getattr(metrics, "win_count", 0)
                        stats["failed_trades"] += getattr(metrics, "loss_count", 0)

                self.logger.info("Retrieved trading statistics")
                return stats
        except Exception as e:
            self.logger.error(f"Error retrieving trading statistics: {e}")
            return None

    def set_error_handler(self, handler: Callable):
        """
        Set a callback for error handling.

        Args:
            handler: Function that takes (exception, task_name) as parameters
        """
        self.on_error = handler

    def set_message_handler(self, handler: Callable):
        """
        Set a callback for message processing.

        Args:
            handler: Function that takes (channel, result) as parameters
        """
        self.on_message_processed = handler

    def get_or_build_channel_list(self) -> List[str]:
        """
        Build a list of Redis channels to subscribe to.

        Returns:
            List of channel names
        """
        from src.arbirich.constants import TRADE_EXECUTIONS_CHANNEL, TRADE_OPPORTUNITIES_CHANNEL

        channels = [
            TRADE_OPPORTUNITIES_CHANNEL,
            TRADE_EXECUTIONS_CHANNEL,
        ]

        # Add any other essential channels
        if self.config.get("add_essential_channels", True):
            channels.append("status_updates")
            channels.append("broadcast")

        return channels

    def check_redis_health(self) -> bool:
        """Check if Redis connection is healthy"""
        try:
            # Check the Redis client first - using self.redis_client instead of self.redis_service
            if not self.redis_client:
                self.logger.warning("Redis client not initialized")
                return False

            # Check if Redis client is healthy
            if hasattr(self.redis_client, "is_healthy") and callable(self.redis_client.is_healthy):
                if not self.redis_client.is_healthy():
                    self.logger.warning("Redis client is not healthy")
                    return False
            # Alternative check for raw Redis clients
            elif hasattr(self.redis_client, "ping"):
                try:
                    if not self.redis_client.ping():
                        self.logger.warning("Redis client ping failed")
                        return False
                except Exception as ping_error:
                    self.logger.warning(f"Redis client ping error: {ping_error}")
                    return False
            # Check client attribute if it's a service wrapper
            elif hasattr(self.redis_client, "client") and self.redis_client.client:
                try:
                    if not self.redis_client.client.ping():
                        self.logger.warning("Redis client ping failed")
                        return False
                except Exception as ping_error:
                    self.logger.warning(f"Redis client ping error: {ping_error}")
                    return False

            # Check the PubSub connection if it exists
            if hasattr(self, "pubsub") and self.pubsub:
                # Check if the pubsub is functional
                if hasattr(self.redis_client, "check_pubsub_health") and callable(
                    self.redis_client.check_pubsub_health
                ):
                    # Use the new method for checking PubSub health if available
                    if not self.redis_client.check_pubsub_health(self.pubsub):
                        self.logger.warning("Redis PubSub is not healthy")
                        return False
                else:
                    # Basic check if client doesn't have specialized method
                    if not self.pubsub:
                        self.logger.warning("PubSub object is None")
                        return False

                    # Check if it has a connection
                    if hasattr(self.pubsub, "connection") and self.pubsub.connection is None:
                        self.logger.warning("PubSub connection is None")
                        return False

            return True
        except Exception as e:
            self.logger.error(f"❌ Error checking Redis health: {e}")
            return False

    async def perform_health_check(self) -> dict:
        """Perform a comprehensive health check of all reporting services"""
        health_status = {"redis": False, "database": False, "pubsub": False, "overall": False}

        try:
            # Check Redis health
            health_status["redis"] = self.check_redis_health()

            # Check database health
            if self.db_service:
                try:
                    # Simple check - can we connect and query?
                    with self.db_service.engine.connect() as conn:
                        result = conn.execute("SELECT 1").scalar()
                        health_status["database"] = result == 1
                except Exception as e:
                    self.logger.warning(f"Database health check failed: {e}")
                    health_status["database"] = False

            # Set overall health
            health_status["overall"] = health_status["redis"] and health_status["database"]

            return health_status
        except Exception as e:
            self.logger.error(f"Error performing health check: {e}")
            return health_status
