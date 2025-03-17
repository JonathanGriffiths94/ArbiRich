import asyncio
import logging
import traceback

from src.arbirich.flows.database.database import run_database_flow
from src.arbirich.flows.execution.execution_source import set_stop_event
from src.arbirich.services.background_subscriber import get_background_subscriber
from src.arbirich.services.database.prefill_database import prefill_database

logger = logging.getLogger(__name__)


class FlowManager:
    """
    Manages the lifecycle of all data flows in the application.
    Implements async context manager interface for use in lifespan events.
    """

    def __init__(self):
        self.tasks = {}

    async def __aenter__(self):
        """Async context manager entry - starts all flows"""
        await self.startup()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - shuts down all flows"""
        await self.shutdown()
        return False  # Don't suppress exceptions

    async def startup(self):
        """Start all flows"""
        logger.info("FlowManager: Starting up flows...")

        try:
            # First prefill the database with configuration data
            logger.info("FlowManager: Prefilling database with configuration data...")
            try:
                prefill_database()
                logger.info("FlowManager: Database prefill completed successfully")
            except Exception as e:
                logger.error(f"FlowManager: Error prefilling database: {e}")
                logger.error(traceback.format_exc())
                # Continue with startup even if prefill fails

            # Start background Redis subscriber first to ensure channels are active
            subscriber = get_background_subscriber()
            subscriber.start()
            logger.info("FlowManager: Started background Redis subscriber")

            # Give subscriptions a moment to establish
            await asyncio.sleep(1)

            await self.start_arbitrage_flows()

            await self.start_ingestion_flow()

            await self.start_execution_flows()

            # Add database flow
            await self.start_database_flow()

            # Start the flow status checker in the background
            self.tasks["flow_status_checker"] = asyncio.create_task(
                self.check_flow_status(), name="flow_status_checker"
            )

            logger.info("FlowManager: All flows have been started.")
        except Exception as e:
            logger.error(f"FlowManager: Error during startup: {e}")
            logger.error(traceback.format_exc())

    # Update the shutdown method for better database flow handling
    async def shutdown(self):
        """Shutdown all flows"""
        logger.info("FlowManager: Shutting down flows...")

        # First disable flow-status checker
        if "flow_status_checker" in self.tasks and not self.tasks["flow_status_checker"].done():
            logger.info("FlowManager: Cancelling flow status checker")
            self.tasks["flow_status_checker"].cancel()
            try:
                await self.tasks["flow_status_checker"]
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"FlowManager: Error cancelling flow status checker: {e}")

        # First signal database flow to stop with a timeout
        try:
            from src.arbirich.flows.database.database import stop_database_flow_async

            logger.info("FlowManager: Signaling database flow to stop")
            await asyncio.wait_for(stop_database_flow_async(), timeout=5.0)
            logger.info("FlowManager: Database flow stop completed")

            # Also cancel the task if it exists
            if "database_flow" in self.tasks and not self.tasks["database_flow"].done():
                self.tasks["database_flow"].cancel()
                try:
                    await asyncio.wait_for(self.tasks["database_flow"], timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
        except Exception as e:
            logger.error(f"FlowManager: Error stopping database flow: {e}")

        # Then signal execution flows to stop
        try:
            logger.info("FlowManager: Signaling execution flows to stop")
            set_stop_event()
            await asyncio.sleep(1)  # Brief pause to allow stop signal to propagate
        except Exception as e:
            logger.error(f"FlowManager: Error signaling execution flows to stop: {e}")

        # Then cancel all remaining tasks
        for name, task in list(self.tasks.items()):
            if not task.done():
                logger.info(f"FlowManager: Cancelling task {name}")
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    logger.warning(f"FlowManager: Task {name} did not stop within timeout")
                except Exception as e:
                    logger.error(f"FlowManager: Error shutting down {name}: {e}")

        # Final cleanup
        self.tasks = {}
        logger.info("FlowManager: All flows have been shut down.")

    async def start_arbitrage_flows(self):
        """Start arbitrage flows for each strategy"""
        try:
            from src.arbirich.config import STRATEGIES
            from src.arbirich.flows.arbitrage.arbitrage import run_arbitrage_flow

            strategy_names = list(STRATEGIES.keys())
            logger.info(f"FlowManager: Starting arbitrage flows for strategies: {strategy_names}")

            for strategy_name in strategy_names:
                task_name = f"arbitrage_{strategy_name}"
                if task_name in self.tasks and not self.tasks[task_name].done():
                    logger.info(f"FlowManager: Task {task_name} already running")
                    continue

                logger.info(f"FlowManager: Started arbitrage flow for strategy: {strategy_name}")
                self.tasks[task_name] = asyncio.create_task(
                    run_arbitrage_flow(strategy_name, debug_mode=False), name=task_name
                )
        except Exception as e:
            logger.error(f"FlowManager: Error starting arbitrage flows: {e}")
            logger.error(traceback.format_exc())

    async def start_ingestion_flow(self):
        """Start the ingestion flow"""
        try:
            # Import here to avoid circular imports
            try:
                # Import from the current structure
                from src.arbirich.flows.ingestion.ingestion import run_ingestion_flow

                logger.info("Using ingestion flow from current location")
            except Exception as e:
                logger.error(f"Error importing ingestion flow: {e}")
                logger.error(traceback.format_exc())
                return

            from src.arbirich.config import EXCHANGES, PAIRS

            # Create exchange_pairs mapping
            exchange_pairs = {}
            for exchange in EXCHANGES:
                exchange_pairs[exchange] = []
                for base, quote in PAIRS:
                    exchange_pairs[exchange].append(f"{base}-{quote}")

            # Start ingestion flow
            task_name = "ingestion_flow"
            if task_name in self.tasks and not self.tasks[task_name].done():
                logger.info(f"FlowManager: Task {task_name} already running")
                return

            logger.info(f"FlowManager: Starting ingestion flow with exchange_pairs: {exchange_pairs}")
            self.tasks[task_name] = asyncio.create_task(run_ingestion_flow(exchange_pairs), name=task_name)
        except Exception as e:
            logger.error(f"FlowManager: Error starting ingestion flow: {e}")
            logger.error(traceback.format_exc())

    async def start_execution_flows(self):
        """Start execution flows for each strategy"""
        try:
            from src.arbirich.config import STRATEGIES
            from src.arbirich.flows.execution.execution import run_execution_flow

            strategy_names = list(STRATEGIES.keys())
            logger.info(f"FlowManager: Starting execution flows for strategies: {strategy_names}")

            for strategy_name in strategy_names:
                task_name = f"execution_{strategy_name}"
                if task_name in self.tasks and not self.tasks[task_name].done():
                    logger.info(f"FlowManager: Task {task_name} already running")
                    continue

                logger.info(f"FlowManager: Started execution flow for strategy: {strategy_name}")
                self.tasks[task_name] = asyncio.create_task(run_execution_flow(strategy_name), name=task_name)
        except Exception as e:
            logger.error(f"FlowManager: Error starting execution flows: {e}")
            logger.error(traceback.format_exc())

    async def start_database_flow(self):
        """Start the database flow for persisting data"""
        try:
            # Check if task already running
            task_name = "database_flow"
            if task_name in self.tasks and not self.tasks[task_name].done():
                logger.info(f"FlowManager: Task {task_name} already running")
                return

            logger.info("FlowManager: Starting database flow")
            self.tasks[task_name] = asyncio.create_task(run_database_flow(), name=task_name)
            logger.info("FlowManager: Database flow task created")
        except Exception as e:
            logger.error(f"FlowManager: Error starting database flow: {e}")
            logger.error(traceback.format_exc())

    async def check_flow_status(self):
        """Periodically check and report on the status of all flows"""
        while True:
            logger.info("Checking flow status...")

            # Check each task
            for name, task in list(self.tasks.items()):
                if task.done():
                    try:
                        # Get the result (or exception)
                        result = task.result()
                        logger.warning(f"Task {name} has completed with result: {result}")

                        # Restart flows if they've completed
                        if name.startswith("arbitrage_"):
                            strategy_name = name.replace("arbitrage_", "")
                            logger.info(f"Restarting arbitrage flow for strategy: {strategy_name}")
                            from src.arbirich.flows.arbitrage.arbitrage import run_arbitrage_flow

                            self.tasks[name] = asyncio.create_task(
                                run_arbitrage_flow(strategy_name, debug_mode=False), name=name
                            )
                    except Exception as e:
                        logger.error(f"Task {name} completed with error: {e}")
                else:
                    logger.info(f"Task {name} is still running")

            # Check every 30 seconds
            await asyncio.sleep(30)
