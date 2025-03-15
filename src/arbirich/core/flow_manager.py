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
            # Start arbitrage flows first (likely to be less problematic)
            await self.start_arbitrage_flows()

            # Start ingestion flow
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

        for name, task in list(self.tasks.items()):
            if not task.done():
                logger.info(f"FlowManager: Cancelling task {name}")
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"FlowManager: Error shutting down {name}: {e}")

        self.tasks = {}
        logger.info("FlowManager: All flows have been shut down.")

    async def start_arbitrage_flows(self):
        """Start arbitrage flows for each strategy"""
        try:
            from src.arbirich.config import STRATEGIES
            from src.arbirich.flows.arbitrage import run_arbitrage_flow

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
                from arbirich.flows.ingestion import run_ingestion_flow

                logger.info("Using main ingestion flow")
            except Exception as e:
                logger.error(f"Error importing ingestion flow: {e}")
                logger.error(traceback.format_exc())
                from src.arbirich.flows.ingestion_ws_base_class import run_ingestion_flow

                logger.info("Using base ingestion flow")

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

                        # Restart arbitrage flows if they've completed
                        if name.startswith("arbitrage_"):
                            strategy_name = name.replace("arbitrage_", "")
                            logger.info(f"Restarting arbitrage flow for strategy: {strategy_name}")
                            from src.arbirich.flows.arbitrage import run_arbitrage_flow

                            self.tasks[name] = asyncio.create_task(
                                run_arbitrage_flow(strategy_name, debug_mode=False), name=name
                            )
                    except Exception as e:
                        logger.error(f"Task {name} completed with error: {e}")
                else:
                    logger.info(f"Task {name} is still running")

            # Check every 30 seconds
            await asyncio.sleep(30)
