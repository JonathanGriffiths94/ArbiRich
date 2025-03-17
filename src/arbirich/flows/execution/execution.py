import asyncio
import logging

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.run import cli_main

from src.arbirich.flows.execution.execution_sink import execute_trade
from src.arbirich.flows.execution.execution_source import RedisExecutionSource
from src.arbirich.services.redis.redis_service import RedisService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

redis_client = RedisService()


def build_flow(strategy_name=None):
    """
    Build the execution flow.
    This flow uses a custom Redis source that yields trade opportunities,
    applies the execute_trade operator, and outputs the result.

    Parameters:
        strategy_name: Optional strategy name to filter opportunities
    """
    logger.info(f"Building execution flow for strategy: {strategy_name or 'all'}")
    flow = Dataflow("execution")

    # Create a RedisExecutionSource with the strategy_name
    source = RedisExecutionSource(strategy_name=strategy_name)
    stream = op.input("redis_input", flow, source)
    logger.debug("Input stream created from RedisExecutionSource.")

    executed = op.map("execute_trade", stream, execute_trade)
    logger.debug("Applied execute_trade operator on stream.")

    # Add a filter to remove empty results (failed executions)
    valid_executions = op.filter("filter_valid", executed, lambda x: x and len(x) > 0)
    logger.debug("Filtered out failed executions.")

    op.output("stdout", valid_executions, StdOutSink())
    logger.info("Output operator attached (StdOutSink).")
    return flow


async def run_execution_flow(strategy_name=None):
    """
    Run the execution flow for a specific strategy or all strategies.

    Parameters:
        strategy_name: Optional strategy name to filter opportunities
    """
    try:
        logger.info(f"Starting execution pipeline for strategy: {strategy_name or 'all'}...")
        flow = build_flow(strategy_name)

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
    import sys

    # Allow specifying a strategy name via command line
    strategy_name = sys.argv[1] if len(sys.argv) > 1 else None
    asyncio.run(run_execution_flow(strategy_name))
