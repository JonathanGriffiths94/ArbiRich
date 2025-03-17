import asyncio
import logging
import traceback

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
            await self.start_arbitrage_flows()
            # await self.start_execution_flows()
            await self.start_ingestion_flow()

            # Start the flow status checker in the background
            self.tasks["flow_status_checker"] = asyncio.create_task(
                self.check_flow_status(), name="flow_status_checker"
            )

            logger.info("FlowManager: All flows have been started.")
        except Exception as e:
            logger.error(f"FlowManager: Error during startup: {e}")
            logger.error(traceback.format_exc())

    async def shutdown(self):
        """Shutdown all flows"""
        logger.info("FlowManager: Shutting down flows...")

        # Create a list of tasks to await
        shutdown_tasks = []

        for name, task in list(self.tasks.items()):
            if not task.done():
                logger.info(f"FlowManager: Cancelling task {name}")
                task.cancel()
                shutdown_tasks.append(task)

        if shutdown_tasks:
            # Wait for all tasks to complete with timeout
            try:
                # Wait for tasks with a timeout
                _, pending = await asyncio.wait(shutdown_tasks, timeout=10.0)

                # Force-cancel any pending tasks
                if pending:
                    logger.warning(
                        f"FlowManager: {len(pending)} tasks did not complete within timeout. Force cancelling..."
                    )
                    for task in pending:
                        task_name = task.get_name() if hasattr(task, "get_name") else str(task)
                        logger.warning(f"FlowManager: Force cancelling {task_name}")
                        # Try to forcefully terminate any remaining tasks
                        if not task.done():
                            task.cancel()
            except asyncio.CancelledError:
                logger.info("FlowManager: Shutdown itself was cancelled")
            except Exception as e:
                logger.error(f"FlowManager: Error during shutdown: {e}")

        # Clear all tasks
        self.tasks = {}

        # Ensure Redis connections are closed
        try:
            from src.arbirich.services.redis_service import RedisService

            RedisService.close_all_connections()
            logger.info("FlowManager: Closed all Redis connections")
        except Exception as e:
            logger.error(f"FlowManager: Error closing Redis connections: {e}")

        logger.info("FlowManager: All flows have been shut down.")

    async def start_arbitrage_flows(self):
        """Start arbitrage flows for each strategy"""
        try:
            # Import here to avoid circular imports
            from src.arbirich.config import STRATEGIES

            # Direct import from the module
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
            # Import directly from the module
            run_ingestion_flow = None
            try:
                from src.arbirich.flows.ingestion.ingestion import run_ingestion_flow

                logger.info("Using ingestion flow from current location")
            except Exception as e:
                logger.error(f"Error importing ingestion flow: {e}")
                logger.error(traceback.format_exc())
                return  # Exit early if we can't import

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
            # Import here to avoid circular imports
            from src.arbirich.config import STRATEGIES

            # Direct import from the module
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
                        elif name == "ingestion_flow":
                            # Restart ingestion flow if it has failed
                            logger.info("Restarting ingestion flow")
                            await self.start_ingestion_flow()
                    except Exception as e:
                        logger.error(f"Task {name} completed with error: {e}")
                else:
                    logger.info(f"Task {name} is still running")

            # Check every 30 seconds
            await asyncio.sleep(30)
