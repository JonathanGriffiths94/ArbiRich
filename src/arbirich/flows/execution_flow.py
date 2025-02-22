import asyncio
import logging
import os
import time

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.run import cli_main

from src.arbirich.market_data_service import MarketDataService

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Instantiate the Redis service.
redis_host = os.getenv("REDIS_HOST", "localhost")
redis_client = MarketDataService(host=redis_host, port=6379, db=0)


def execute_trade(opportunity):
    """
    Simulate trade execution for a given opportunity.
    This is where you'd call your exchange's REST API.
    """
    # Ensure numeric fields are proper floats.
    try:
        opportunity["buy_price"] = float(opportunity["buy_price"])
        opportunity["sell_price"] = float(opportunity["sell_price"])
        opportunity["spread"] = float(opportunity["spread"])
    except Exception as e:
        logger.error(f"Error converting numeric fields: {e}")

    trade_msg = (
        f"Executing trade for {opportunity['asset']}: "
        f"Buy from {opportunity['buy_exchange']} at {opportunity['buy_price']}, "
        f"Sell on {opportunity['sell_exchange']} at {opportunity['sell_price']}, "
        f"Spread: {opportunity['spread']:.4f}"
    )
    logger.critical(trade_msg)

    try:
        redis_client.store_trade_execution(opportunity)
        logger.debug("Trade execution stored successfully in Redis.")
    except Exception as e:
        logger.error(f"Error storing trade execution: {e}")
    return trade_msg


# Define a custom partition that wraps your Redis subscription generator.
class RedisOpportunityPartition(StatefulSourcePartition):
    def __init__(self):
        # Initialize the generator from the Redis subscription.
        logger.debug("Initializing RedisOpportunityPartition.")
        self.gen = redis_client.subscribe_to_trade_opportunities(
            lambda opp: logger.debug(f"Received: {opp}")
        )
        self._running = True
        self._last_activity = time.time()

    def next_batch(self) -> list:
        # Check if we've been idle too long (safety timeout)
        if time.time() - self._last_activity > 60:  # 1 minute timeout
            logger.warning("Safety timeout reached, checking Redis connection")
            self._last_activity = time.time()
            # Ensure redis_client.is_healthy() is implemented appropriately
            if not redis_client.is_healthy():
                logger.warning("Redis connection appears unhealthy")
                return []

        # If partition is marked as not running, return empty list.
        if not self._running:
            logger.info("Partition marked as not running, stopping")
            return []

        try:
            # Try to get one message (non-blocking thanks to our generator design).
            result = next(self.gen, None)
            if result:
                self._last_activity = time.time()
                logger.debug(f"next_batch obtained message: {result}")
                return [result]
            return []
        except StopIteration:
            logger.info("Generator exhausted")
            self._running = False
            return []
        except Exception as e:
            logger.error(f"Error in next_batch: {e}")
            return []

    def snapshot(self) -> None:
        # Return None for now (or implement snapshot logic if needed)
        logger.debug("Snapshot requested for RedisOpportunityPartition (returning None).")
        return None


# Define a custom source that produces partitions.
class RedisOpportunitySource(FixedPartitionedSource):
    def list_parts(self):
        parts = ["1"]
        logger.info(f"List of partition keys: {parts}")
        return parts

    def build_part(self, step_id, for_key, _resume_state):
        logger.info(f"Building partition for key: {for_key}")
        return RedisOpportunityPartition()


def build_flow():
    """
    Build the execution flow.
    This flow uses a custom Redis source that yields trade opportunities,
    applies the execute_trade operator, and outputs the result.
    """
    logger.info("Building execution flow...")
    flow = Dataflow("execution")

    # Use the custom RedisOpportunitySource.
    source = RedisOpportunitySource()
    stream = op.input("redis_input", flow, source)
    logger.debug("Input stream created from RedisOpportunitySource.")

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
        execution_task = asyncio.create_task(
            asyncio.to_thread(cli_main, flow, workers_per_process=1)
        )

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
