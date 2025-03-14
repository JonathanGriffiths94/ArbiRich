import asyncio
import logging

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.run import cli_main

from src.arbirich.services.redis_service import RedisService
from src.arbirich.sinks.execution_sink import execute_trade
from src.arbirich.sources.execution_source import RedisExecutionSource

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

redis_client = RedisService()


def build_flow():
    """
    Build the execution flow.
    This flow uses a custom Redis source that yields trade opportunities,
    applies the execute_trade operator, and outputs the result.
    """
    logger.info("Building execution flow...")
    flow = Dataflow("execution")

    source = RedisExecutionSource()
    stream = op.input("redis_input", flow, source)
    logger.debug("Input stream created from RedisExecutionSource.")

    executed = op.map("execute_trade", stream, execute_trade)
    logger.debug("Applied execute_trade operator on stream.")

    op.output("stdout", executed, StdOutSink())
    logger.info("Output operator attached (StdOutSink).")
    return flow


async def run_execution_flow():
    try:
        logger.info("Starting execution pipeline...")
        flow = build_flow()

        logger.info("Running cli_main in a separate thread.")
        execution_task = asyncio.create_task(asyncio.to_thread(cli_main, flow, workers_per_process=1))

        # Allow interruption to propagate
        try:
            await execution_task
        except asyncio.CancelledError:
            logger.info("Execution task cancelled")
            raise
        logger.info("cli_main has finished running.")
    except asyncio.CancelledError:
        logger.info("Execution task cancelled")
        raise
    finally:
        logger.info("Execution flow shutdown")
        redis_client.close()


# Expose the flow for CLI usage.
flow = build_flow()

if __name__ == "__main__":
    asyncio.run(cli_main(flow, workers_per_process=1))
