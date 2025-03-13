import asyncio
import logging

from bytewax.dataflow import Dataflow
from bytewax.run import cli_main

from arbirich.database_manager import DatabaseManager
from arbirich.sinks.database_sink import db_sink

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

db_manager = DatabaseManager()


def build_database_flow():
    flow = Dataflow()

    # Assume you have a source that emits messages from Redis or any other source.
    # For demonstration, we use a simple list of JSON strings.
    messages = [
        '{"trading_pair_id":1, "buy_exchange_id":1, "sell_exchange_id":2, "buy_price":50000, "sell_price":50500, "spread":0.001, "volume":0.1, "opportunity_timestamp": "2023-03-01T12:00:00"}',
        '{"trading_pair_id":1, "buy_exchange_id":1, "sell_exchange_id":2, "executed_buy_price":50000, "executed_sell_price":50500, "spread":0.001, "volume":0.1, "execution_timestamp": "2023-03-01T12:00:05", "execution_id": "exec123"}',
    ]

    # For a real-world scenario, replace this with your actual source
    flow.input("inp", messages)

    # Here we directly send the messages to our sink
    flow.sink("db_sink", db_sink)

    return flow


async def run_database_flow():
    try:
        logger.info("Starting database pipeline...")
        flow = build_database_flow()

        logger.info("Running cli_main in a separate thread.")
        execution_task = asyncio.create_task(asyncio.to_thread(cli_main, flow, workers_per_process=1))

        # Allow interruption to propagate
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
