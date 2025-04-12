import asyncio
import logging
from typing import Any, Dict

from src.arbirich.config.config import STRATEGIES
from src.arbirich.constants import TRADE_EXECUTIONS_CHANNEL, TRADE_OPPORTUNITIES_CHANNEL
from src.arbirich.core.trading.components.base import Component
from src.arbirich.core.trading.flows.flow_manager import FlowManager
from src.arbirich.core.trading.flows.reporting.message_processor import process_message, process_redis_messages
from src.arbirich.core.trading.flows.reporting.tasks import monitor_health, persist_data, report_performance
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

    async def initialize(self) -> bool:
        """Initialize reporting configurations"""
        try:
            # Configure different types of reporting tasks
            self.reporting_types = [
                {"id": f"persistence_{self.name}", "type": "data_persistence"},
                {"id": f"health_{self.name}", "type": "health_monitoring"},
                {"id": f"performance_{self.name}", "type": "performance_reporting"},
            ]

            # Initialize the reporting flow using the imported module
            from src.arbirich.core.trading.flows.reporting.reporting_flow import build_reporting_flow

            # Create a flow manager for reporting
            self.flow_manager = FlowManager.get_or_create(f"reporting_flow_{self.name}")

            # Configure the flow builder
            self.flow_manager.set_flow_builder(lambda: build_reporting_flow(flow_type="performance"))

            # Create flow configuration
            self.flow_config = {
                "id": f"reporting_flow_{self.name}",
                "channels": [],  # Will be populated by the flow builder
                "active": True,
            }

            # Initialize Redis connection for stream processing
            self.redis_client, self.pubsub = await initialize_redis(self.flow_config.get("channels", []))

            if not self.redis_client or not self.pubsub:
                logger.error("Failed to initialize Redis connection")
                return False

            # Initialize timing attributes for health checks
            self._last_activity = 0
            self._next_log_time = 0

            logger.info(f"Initialized {len(self.reporting_types)} reporting tasks and flow")
            return True

        except Exception as e:
            logger.error(f"Error initializing reporting: {e}", exc_info=True)
            return False

    def build_reporting_flow(self, flow_type: str = "performance") -> Dict[str, Any]:
        """
        Build a reporting flow configuration based on the specified type.
        This replaces the Bytewax dataflow creation with a configuration object
        that will be used by our async tasks.
        """
        # Start with the main channels
        channels = [
            TRADE_OPPORTUNITIES_CHANNEL,
            TRADE_EXECUTIONS_CHANNEL,
        ]

        # Add strategy-specific channels
        for strategy_name in STRATEGIES.keys():
            channels.append(f"{TRADE_OPPORTUNITIES_CHANNEL}:{strategy_name}")
            channels.append(f"{TRADE_EXECUTIONS_CHANNEL}:{strategy_name}")

        logger.info(f"Building reporting flow with channels: {channels}")

        # Create a configuration that mimics the Bytewax flow
        flow_config = {
            "id": f"reporting_flow_{flow_type}",
            "type": flow_type,
            "channels": channels,
            "handlers": {
                "data_persistence": persist_data,
                "health_monitoring": monitor_health,
                "performance_reporting": report_performance,
                "message_processing": process_message,
            },
            "active": True,
        }

        # Add the Redis stream processor task
        redis_stream_task = {"id": f"redis_stream_{self.name}", "type": "redis_stream_processor"}

        if redis_stream_task not in self.reporting_types:
            self.reporting_types.append(redis_stream_task)

        logger.debug(f"Created flow config: {flow_config['id']}")
        return flow_config

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

        while self.active and not self.stop_event.is_set():
            try:
                if report_type == "data_persistence":
                    await persist_data()
                elif report_type == "health_monitoring":
                    await monitor_health()
                elif report_type == "performance_reporting":
                    await report_performance()
                elif report_type == "redis_stream_processor":
                    # This replaces the Bytewax flow functionality
                    await process_redis_messages(
                        self.pubsub,
                        self.redis_client,
                        self.active,
                        self.stop_event,
                        self.flow_config.get("debug_mode", False),
                    )
                    # This task has its own loop, so it shouldn't reach the sleep below
                    continue
                else:
                    logger.warning(f"Unknown report type: {report_type}")

                # Wait for next interval
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=self.update_interval)
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

    async def cleanup(self) -> None:
        """Cancel all reporting tasks and clean up resources"""
        logger.info("Cleaning up reporting component")

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
                self.pubsub.unsubscribe()
                self.pubsub.close()
                logger.info("Closed Redis PubSub connection")
            except Exception as e:
                logger.error(f"Error closing Redis PubSub: {e}")

        # Reset shared Redis client
        reset_redis_connection()

        self.tasks = {}

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
