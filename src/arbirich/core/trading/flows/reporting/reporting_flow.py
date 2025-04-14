# reporting/flow.py

import asyncio
import logging
import signal
import time
from typing import Any, Dict, Optional

from src.arbirich.config.config import REDIS_CONFIG, STRATEGIES
from src.arbirich.constants import TRADE_EXECUTIONS_CHANNEL, TRADE_OPPORTUNITIES_CHANNEL
from src.arbirich.core.state.system_state import mark_system_shutdown
from src.arbirich.core.trading.flows.flow_manager import FlowManager
from src.arbirich.core.trading.flows.reporting.redis_client import reset_shared_redis_client, safe_close_pubsub
from src.arbirich.services.redis.redis_service import RedisService, get_shared_redis_client

from .tasks import monitor_health, persist_data, process_redis_messages, report_performance

logger = logging.getLogger(__name__)


class ReportingFlow:
    """
    Represents a reporting data flow that handles Redis messages
    and performs various reporting tasks.
    """

    def __init__(self, flow_type: str = "performance", config: Optional[Dict[str, Any]] = None):
        """
        Initialize the reporting flow.

        Args:
            flow_type: Type of reporting flow ("performance", "health", etc.)
            config: Optional configuration override
        """
        self.flow_type = flow_type
        self.config = config or {}
        self.active = False
        self.stop_event = asyncio.Event()
        self.redis_client = None
        self.pubsub = None
        self.tasks = {}

        # Default configuration
        self.update_interval = self.config.get("update_interval", 60.0)
        self.debug_mode = self.config.get("debug_mode", False)

        # Initialize logger
        self.logger = logging.getLogger(f"{__name__}.{flow_type}")

    async def initialize(self) -> bool:
        """
        Initialize the reporting flow.

        Returns:
            bool: True if initialization was successful, False otherwise
        """
        try:
            # Build channel configuration
            self.channels = self._build_channel_list()

            # Initialize Redis client with retry logic
            self.redis_client = await self._initialize_redis_client()
            if not self.redis_client:
                self.logger.error("Failed to initialize Redis client after multiple attempts")
                return False

            # Initialize Redis PubSub with error handling
            try:
                self.pubsub = self.redis_client.client.pubsub(ignore_subscribe_messages=True)
                self.logger.info("Successfully created Redis PubSub client")
            except Exception as pubsub_error:
                self.logger.error(f"Failed to create Redis PubSub: {pubsub_error}")
                return False

            # Subscribe to channels with error handling
            try:
                # Subscribe to each channel individually with error handling
                for channel in self.channels:
                    try:
                        self.pubsub.subscribe(channel)
                        self.logger.debug(f"Subscribed to channel: {channel}")
                    except Exception as channel_error:
                        self.logger.error(f"Error subscribing to channel {channel}: {channel_error}")
                        # Continue with other channels even if one fails

                self.logger.info(f"Subscribed to {len(self.channels)} Redis channels")
            except Exception as subscribe_error:
                self.logger.error(f"Error during channel subscription: {subscribe_error}")
                # Try to continue even with subscription errors

            # Verify subscriptions worked
            try:
                active_channels = self.pubsub.channels
                if not active_channels:
                    self.logger.warning("No active channels after subscription attempts")
                else:
                    channel_count = len(active_channels)
                    self.logger.info(f"Successfully subscribed to {channel_count} channels")
            except Exception as verify_error:
                self.logger.error(f"Error verifying subscriptions: {verify_error}")

            # Initialize timing attributes for health checks
            self._last_activity = time.time()
            self._next_log_time = time.time() + 300  # First log after 5 minutes

            # Define reporting tasks
            self.reporting_tasks = [
                {"id": "data_persistence", "handler": persist_data},
                {"id": "health_monitoring", "handler": monitor_health},
                {"id": "performance_reporting", "handler": report_performance},
                {"id": "redis_stream_processor", "handler": self._process_messages},
            ]

            self.logger.info(f"Initialized reporting flow: {self.flow_type}")
            return True

        except Exception as e:
            self.logger.error(f"Error initializing reporting flow: {e}", exc_info=True)
            return False

    async def _initialize_redis_client(self, max_retries=3):
        """Initialize Redis client with retry logic"""
        # First try the shared client
        for attempt in range(1, max_retries + 1):
            try:
                self.logger.info(f"Attempting to get shared Redis client (attempt {attempt}/{max_retries})")
                redis_client = get_shared_redis_client()

                if redis_client and redis_client.client:
                    # Verify the client works by trying a ping
                    try:
                        if redis_client.client.ping():
                            self.logger.info("Successfully connected to Redis using shared client")
                            return redis_client
                        else:
                            self.logger.warning("Redis ping returned False, client may not be functional")
                    except Exception as ping_error:
                        self.logger.warning(f"Error pinging Redis: {ping_error}")
                else:
                    self.logger.warning("Shared Redis client is None or has no client attribute")

                # If we get here, the shared client didn't work, wait before retry
                await asyncio.sleep(1)

            except Exception as e:
                self.logger.error(f"Error getting shared Redis client (attempt {attempt}): {e}")
                await asyncio.sleep(1)

        # If shared client failed, try creating our own instance
        self.logger.info("Shared Redis client unavailable, creating direct Redis connection")
        try:
            # Create a direct Redis connection
            import redis

            # Get Redis config
            host = REDIS_CONFIG.get("host", "localhost")
            port = REDIS_CONFIG.get("port", 6379)
            db = REDIS_CONFIG.get("db", 0)

            # Create a direct Redis client
            direct_client = redis.Redis(host=host, port=port, db=db, decode_responses=True)

            # Test the connection
            if direct_client.ping():
                self.logger.info(f"Successfully created direct Redis connection to {host}:{port}")

                # Wrap in our RedisService class
                redis_service = RedisService()
                redis_service.client = direct_client

                return redis_service
            else:
                self.logger.error("Direct Redis connection failed ping test")
                return None

        except Exception as direct_error:
            self.logger.error(f"Failed to create direct Redis connection: {direct_error}")
            return None

    def _build_channel_list(self) -> list:
        """
        Build the list of Redis channels to subscribe to.

        Returns:
            list: List of channel names
        """
        channels = [
            TRADE_OPPORTUNITIES_CHANNEL,
            TRADE_EXECUTIONS_CHANNEL,
        ]

        # Add strategy-specific channels
        for strategy_name in STRATEGIES.keys():
            channels.append(f"{TRADE_OPPORTUNITIES_CHANNEL}:{strategy_name}")
            channels.append(f"{TRADE_EXECUTIONS_CHANNEL}:{strategy_name}")

        return channels

    async def start(self) -> bool:
        """
        Start the reporting flow.

        Returns:
            bool: True if started successfully, False otherwise
        """
        if self.active:
            self.logger.warning("Reporting flow already active")
            return True

        # Initialize if needed
        if not self.redis_client:
            success = await self.initialize()
            if not success:
                return False

        self.active = True
        self.logger.info(f"Starting reporting flow: {self.flow_type}")

        # Start all reporting tasks
        for task_config in self.reporting_tasks:
            task_id = task_config["id"]
            handler = task_config["handler"]

            if task_id == "redis_stream_processor":
                # Special case for the message processor
                task = asyncio.create_task(handler(), name=f"reporting_{task_id}")
            else:
                # Regular task with interval
                task = asyncio.create_task(self._run_interval_task(task_id, handler), name=f"reporting_{task_id}")

            self.tasks[task_id] = task
            self.logger.info(f"Started task: {task_id}")

        return True

    async def _run_interval_task(self, task_id: str, handler) -> None:
        """
        Run a task at regular intervals.

        Args:
            task_id: ID of the task
            handler: Async function to call
        """
        while self.active and not self.stop_event.is_set():
            try:
                self.logger.debug(f"Running task: {task_id}")
                await handler()

                # Wait for next interval or stop event
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=self.update_interval)
                except asyncio.TimeoutError:
                    # This is expected - it means we waited for the interval and no stop event occurred
                    pass

            except asyncio.CancelledError:
                self.logger.info(f"Task cancelled: {task_id}")
                raise
            except Exception as e:
                self.logger.error(f"Error in task {task_id}: {e}", exc_info=True)
                # Back off on errors
                await asyncio.sleep(min(self.update_interval * 2, 300))

    async def _process_messages(self) -> None:
        """
        Process Redis messages.
        This is the main message processing loop.
        """
        await process_redis_messages(
            self.pubsub, self.redis_client, lambda: self.active, self.stop_event, self.debug_mode
        )

    async def stop(self) -> bool:
        """
        Stop the reporting flow.

        Returns:
            bool: True if stopped successfully, False otherwise
        """
        if not self.active:
            self.logger.info("Reporting flow not active")
            return True

        self.logger.info(f"Stopping reporting flow: {self.flow_type}")

        # Set flags to stop tasks
        self.active = False
        self.stop_event.set()

        # First set system shutdown flag to ensure everything responds to shutdown
        try:
            from src.arbirich.core.state.system_state import mark_system_shutdown

            mark_system_shutdown(True)
        except Exception as e:
            self.logger.error(f"Error marking system shutdown: {e}")

        # Cancel all tasks
        for task_id, task in self.tasks.items():
            try:
                if not task.done():
                    task.cancel()
                    self.logger.info(f"Cancelled task: {task_id}")
            except Exception as e:
                self.logger.error(f"Error cancelling task {task_id}: {e}")

        # Wait for tasks to complete with a shorter timeout for faster response
        if self.tasks:
            try:
                # Wait with a timeout
                await asyncio.wait(list(self.tasks.values()), timeout=2.0)
            except Exception as e:
                self.logger.error(f"Error waiting for tasks to complete: {e}")

        # Clean up Redis resources
        try:
            await safe_close_pubsub()
            reset_shared_redis_client()
        except Exception as e:
            self.logger.error(f"Error during Redis cleanup: {e}")

        # Use more aggressive task cleanup if needed
        still_active_tasks = [t for t in self.tasks.values() if not t.done()]
        if still_active_tasks:
            self.logger.warning(f"{len(still_active_tasks)} tasks still active after stop attempt")
            for task in still_active_tasks:
                try:
                    task.cancel()
                except Exception:
                    pass

        self.tasks = {}
        self.logger.info(f"Reporting flow stopped: {self.flow_type}")
        return True


def build_reporting_flow(flow_type="performance"):
    """Build the reporting flow with the specified flow type"""
    import redis

    from src.arbirich.config.config import STRATEGIES
    from src.arbirich.constants import TRADE_EXECUTIONS_CHANNEL, TRADE_OPPORTUNITIES_CHANNEL
    from src.arbirich.services.redis.redis_service import get_shared_redis_client

    logger.info(f"Building reporting flow of type: {flow_type}")

    # Initialize Redis client with fallback to direct connection
    redis_client = get_shared_redis_client()
    if not redis_client:
        logger.warning("Failed to get shared Redis client, creating direct connection")
        try:
            # Get Redis config
            host = REDIS_CONFIG.get("host", "localhost")
            port = REDIS_CONFIG.get("port", 6379)
            db = REDIS_CONFIG.get("db", 0)

            # Create direct Redis client
            direct_client = redis.Redis(host=host, port=port, db=db, decode_responses=True)

            # Test connection
            if direct_client.ping():
                logger.info(f"Successfully created direct Redis connection to {host}:{port}")

                # Create and use RedisService
                redis_client = RedisService()
                redis_client.client = direct_client
            else:
                logger.error("Direct Redis connection failed ping test")
                return None
        except Exception as e:
            logger.error(f"Failed to create direct Redis connection: {e}")
            return None

    if not redis_client or not hasattr(redis_client, "client"):
        logger.error("Failed to get valid Redis client")
        return None

    # Set up channels to subscribe to
    channels = [
        TRADE_OPPORTUNITIES_CHANNEL,  # Main opportunities channel
        TRADE_EXECUTIONS_CHANNEL,  # Main executions channel
    ]

    # Add strategy-specific channels
    for strategy_name in STRATEGIES.keys():
        channels.append(f"{TRADE_OPPORTUNITIES_CHANNEL}:{strategy_name}")
        channels.append(f"{TRADE_EXECUTIONS_CHANNEL}:{strategy_name}")

    # Log channel subscriptions
    logger.info(f"Reporting flow subscribing to {len(channels)} channels:")
    for channel in channels:
        logger.info(f"  - {channel}")

    # Create PubSub and subscribe to all channels with error handling
    try:
        pubsub = redis_client.client.pubsub(ignore_subscribe_messages=True)
        for channel in channels:
            try:
                pubsub.subscribe(channel)
                logger.info(f"Subscribed to channel: {channel}")
            except Exception as e:
                logger.error(f"Error subscribing to channel {channel}: {e}")
                # Continue with other channels

        logger.info("Reporting flow subscriptions completed")
    except Exception as e:
        logger.error(f"Error creating pubsub: {e}")
        return None

    # Return the flow configuration
    return {
        "type": flow_type,
        "redis_client": redis_client,
        "pubsub": pubsub,
        "channels": channels,
        "debug_mode": True,  # Enable debug mode to see more details
    }


# Singleton flow instance
_reporting_flow = None


async def initialize_flow(flow_type: str = "performance", config: Optional[Dict[str, Any]] = None) -> ReportingFlow:
    """
    Initialize the reporting flow singleton.

    Args:
        flow_type: Type of reporting flow
        config: Optional configuration

    Returns:
        ReportingFlow: The initialized flow
    """
    global _reporting_flow

    if _reporting_flow is not None:
        return _reporting_flow

    flow = ReportingFlow(flow_type, config)
    success = await flow.initialize()

    if success:
        _reporting_flow = flow
        return flow
    else:
        logger.error(f"Failed to initialize reporting flow: {flow_type}")
        return None


async def start_flow() -> bool:
    """
    Start the reporting flow.

    Returns:
        bool: True if started successfully, False otherwise
    """
    flow = await get_flow()
    if flow:
        return await flow.start()
    return False


async def stop_flow() -> bool:
    """
    Stop the reporting flow.

    Returns:
        bool: True if stopped successfully, False otherwise
    """
    flow = await get_flow()
    if flow:
        return await flow.stop()
    return False


async def get_flow() -> Optional[ReportingFlow]:
    """
    Get the reporting flow singleton, initializing if needed.

    Returns:
        Optional[ReportingFlow]: The flow or None if initialization failed
    """
    global _reporting_flow

    if _reporting_flow is None:
        return await initialize_flow()

    return _reporting_flow


def stop_flow_sync() -> bool:
    """
    Stop the reporting flow synchronously.

    Returns:
        bool: True if stopped, False otherwise
    """
    loop = asyncio.get_event_loop()
    if loop.is_running():
        # Schedule the async stop
        asyncio.create_task(stop_flow())
        return True
    else:
        # Run the async stop directly
        return loop.run_until_complete(stop_flow())


# Add a ReportingFlowManager to better integrate with the flow_manager system
class ReportingFlowManager(FlowManager):
    """Manager for reporting flows that integrates with the flow manager system."""

    def __init__(self, flow_id: str = "reporting"):
        super().__init__(flow_id)
        self.reporting_flow = None
        self.config = {}

    async def build_flow(self) -> ReportingFlow:
        """Build a reporting flow instance"""
        self.reporting_flow = await initialize_flow(flow_type="performance", config=self.config)
        return self.reporting_flow

    async def run_flow(self) -> bool:
        """Run the reporting flow"""
        if self.reporting_flow is None:
            self.reporting_flow = await self.build_flow()

        if self.reporting_flow:
            success = await self.reporting_flow.start()
            self.running = success
            return success
        return False

    def stop_flow(self) -> bool:
        """Stop the reporting flow"""
        if self.reporting_flow:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(self.reporting_flow.stop())
                self.running = False
                return True
            else:
                result = loop.run_until_complete(self.reporting_flow.stop())
                self.running = False
                return result
        return True


# Initialize a singleton instance of the reporting flow manager
_flow_manager = None


def get_flow_manager() -> ReportingFlowManager:
    """Get or create a ReportingFlowManager instance"""
    global _flow_manager
    if _flow_manager is None:
        _flow_manager = ReportingFlowManager("reporting")
        # Register with the global flow manager registry
        FlowManager.get_or_create("reporting")
    return _flow_manager


# Main entry point for running the flow directly
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Main async function that keeps the context alive
    async def main():
        # Start the flow
        flow = await get_flow()
        if not flow:
            logger.error("Failed to get reporting flow, exiting")
            return

        # Start the flow
        if not await flow.start():
            logger.error("Failed to start reporting flow, exiting")
            return

        logger.info("Reporting flow started successfully, running indefinitely...")

        try:
            # Keep the async context alive
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info("Main task cancelled, shutting down gracefully...")
            await flow.stop()

    # Setup signal handlers
    def handle_exit_signal(sig, frame):
        logger.info(f"Received signal {sig}, initiating shutdown...")

        # Set system shutdown flag
        try:
            mark_system_shutdown(True)
        except Exception as e:
            logger.debug(f"Error marking system shutdown: {e}")

        # Cancel the main task if it's running
        for task in asyncio.all_tasks():
            if task is not asyncio.current_task():
                task.cancel()

    # Register signal handlers
    signal.signal(signal.SIGINT, handle_exit_signal)
    signal.signal(signal.SIGTERM, handle_exit_signal)

    # Run the main async function
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received in main thread, exiting...")
    except asyncio.CancelledError:
        logger.info("Main task cancelled in asyncio.run")
    finally:
        # Ensure we call stop_flow synchronously on exit
        stop_flow_sync()
        logger.info("Reporting flow stopped, exiting")
