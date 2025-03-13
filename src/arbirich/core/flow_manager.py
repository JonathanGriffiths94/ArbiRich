import asyncio
import logging

from src.arbirich.flows.arbitrage import run_arbitrage_flow
from src.arbirich.flows.database import run_database_flow
from src.arbirich.flows.execution import run_execution_flow
from src.arbirich.flows.ingestion_ws_base_class import run_ingestion_flow

logger = logging.getLogger(__name__)


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
        self.database_task = None

    async def __aenter__(self):
        logger.info("FlowManager: Starting up flows...")
        self.database_task = asyncio.create_task(run_database_flow())
        self.ingestion_task = asyncio.create_task(run_ingestion_flow())
        self.arbitrage_task = asyncio.create_task(run_arbitrage_flow())
        self.execution_task = asyncio.create_task(run_execution_flow())

        logger.info("FlowManager: All flows have been started.")
        return self

    async def __aexit__(self, exc_type, exc_val, tb):
        logger.info("FlowManager: Shutting down flows...")
        tasks = []
        if self.ingestion_task:
            self.ingestion_task.cancel()
            tasks.append(self.ingestion_task)
        if self.arbitrage_task:
            self.arbitrage_task.cancel()
            tasks.append(self.arbitrage_task)
        if self.execution_task:
            self.execution_task.cancel()
            tasks.append(self.execution_task)
        if self.database_task:
            self.database_task.cancel()
            tasks.append(self.database_task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("FlowManager: All flows have been shut down.")
