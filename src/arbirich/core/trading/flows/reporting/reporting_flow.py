# reporting/flow.py

import asyncio
import logging
import signal
import sys
import time
from typing import Any, Dict, Optional

from src.arbirich.config.config import STRATEGIES
from src.arbirich.constants import TRADE_EXECUTIONS_CHANNEL, TRADE_OPPORTUNITIES_CHANNEL
from src.arbirich.core.state.system_state import mark_system_shutdown
from src.arbirich.services.redis.redis_service import get_shared_redis_client, reset_shared_redis_client

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

            # Initialize Redis client
            self.redis_client = get_shared_redis_client()
            if not self.redis_client:
                self.logger.error("Failed to get Redis client")
                return False

            # Initialize Redis PubSub
            self.pubsub = self.redis_client.pubsub(ignore_subscribe_messages=True)

            # Subscribe to channels
            await self.pubsub.subscribe(*self.channels)
            self.logger.info(f"Subscribed to {len(self.channels)} Redis channels")

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
        if self.pubsub:
            try:
                await self.pubsub.unsubscribe()
                self.pubsub.close()
                self.logger.info("Closed Redis PubSub")
            except Exception as e:
                self.logger.error(f"Error closing Redis PubSub: {e}")
                # If we couldn't close properly, add more aggressive cleanup
                try:
                    import redis

                    redis.Redis().connection_pool.disconnect()
                    self.logger.info("Force disconnected Redis connections")
                except Exception:
                    pass

        # Reset Redis client
        reset_shared_redis_client()

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


def build_reporting_flow(flow_type: str = "performance", config: Optional[Dict[str, Any]] = None) -> dict:
    """
    Build a reporting flow configuration based on the specified type.

    Args:
        flow_type: Type of reporting flow
        config: Optional configuration dictionary

    Returns:
        A flow configuration dictionary
    """
    logger.info(f"Building reporting flow of type: {flow_type}")

    # Start with the main channels
    channels = [
        TRADE_OPPORTUNITIES_CHANNEL,
        TRADE_EXECUTIONS_CHANNEL,
    ]

    # Add strategy-specific channels
    for strategy_name in STRATEGIES.keys():
        channels.append(f"{TRADE_OPPORTUNITIES_CHANNEL}:{strategy_name}")
        channels.append(f"{TRADE_EXECUTIONS_CHANNEL}:{strategy_name}")

    # Build the flow configuration
    flow = ReportingFlow(flow_type, config or {})

    logger.info(f"Created reporting flow with {len(channels)} channels")
    return flow


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


# Main entry point for running the flow directly
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Setup signal handlers
    def handle_exit_signal(sig, frame):
        logger.info(f"Received signal {sig}, initiating shutdown...")

        # Set system shutdown flag
        try:
            mark_system_shutdown(True)
        except Exception as e:
            logger.debug(f"Error marking system shutdown: {e}")
            pass

        # Stop the flow
        stop_flow_sync()

        # Exit after a short delay
        time.sleep(1)
        sys.exit(0)

    # Register signal handlers
    signal.signal(signal.SIGINT, handle_exit_signal)
    signal.signal(signal.SIGTERM, handle_exit_signal)

    # Run the flow
    try:
        # Create and start the flow
        asyncio.run(start_flow())

        # Keep the main thread alive
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, exiting...")
        asyncio.run(stop_flow())
        sys.exit(0)
