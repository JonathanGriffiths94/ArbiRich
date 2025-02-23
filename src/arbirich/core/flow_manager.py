import asyncio
import logging
import os

from src.arbirich.flows.arbitrage import run_arbitrage_flow
from src.arbirich.flows.execution import run_executor_flow
from src.arbirich.flows.ingestion import run_ingestion_flow
from src.arbirich.market_data_service import MarketDataService

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

redis_client = MarketDataService(os.getenv("REDIS_HOST"), port=6379, db=0)


class FlowManager:
    """
    Asynchronous context manager to manage the lifecycle of the ingestion and execution flows.
    When entered, it starts both flows concurrently.
    When exited, it cancels and awaits their termination.
    """

    def __init__(self):
        self.ingestion_task = None
        self.arbitrage_task = None
        self.execution_task = None

    async def __aenter__(self):
        logger.info("FlowManager: Starting up flows...")
        self.ingestion_task = asyncio.create_task(run_ingestion_flow())
        # self.arbitrage_task = asyncio.create_task(run_arbitrage_flow(redis_client))
        # self.execution_task = asyncio.create_task(run_executor_flow())
        logger.info("FlowManager: All flows have been started.")
        return self

    async def __aexit__(self, exc_type, exc_val, tb):
        logger.info("FlowManager: Shutting down flows...")
        tasks = []
        if self.ingestion_task:
            self.ingestion_task.cancel()
            tasks.append(self.ingestion_task)
        if self.execution_task:
            self.execution_task.cancel()
            tasks.append(self.execution_task)
        if self.arbitrage_task:
            self.arbitrage_task.cancel()
            tasks.append(self.arbitrage_task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("FlowManager: All flows have been shut down.")
