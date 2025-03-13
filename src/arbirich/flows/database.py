import asyncio
import logging

from bytewax.dataflow import Dataflow
from bytewax.run import cli_main

from src.arbirich.database_manager import DatabaseManager
from src.arbirich.sinks.database_sink import db_sink
from src.arbirich.sources.database_source import RedisDatabaseSource

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

db_manager = DatabaseManager()


def build_database_flow():
    flow = Dataflow()

    channels = ["trade_opportunities", "trade_executions"]
    source = RedisDatabaseSource(channels)
    flow.input("redis_input", source)

    flow.sink("db_sink", db_sink)

    return flow


async def run_database_flow():
    try:
        logger.info("Starting database pipeline...")
        flow = build_database_flow()

        logger.info("Running cli_main in a separate thread.")
        execution_task = asyncio.create_task(asyncio.to_thread(cli_main, flow, workers_per_process=1))

        try:
            await execution_task
        except asyncio.CancelledError:
            logger.info("Database task cancelled")
            raise
        logger.info("cli_main has finished running.")
    except asyncio.CancelledError:
        logger.info("Database task cancelled")
        raise
    finally:
        logger.info("Database flow shutdown")
        db_manager.close()


flow = run_database_flow()

if __name__ == "__main__":
    asyncio.run(cli_main(flow, workers_per_process=1))
