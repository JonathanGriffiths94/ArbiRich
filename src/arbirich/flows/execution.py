import asyncio
import logging
import os
import time

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.run import cli_main

from src.arbirich.models.dtos import TradeExecution, TradeOpportunity
from src.arbirich.redis_manager import MarketDataService

logger = logging.getLogger(__name__)

# Instantiate the Redis service.
redis_host = os.getenv("REDIS_HOST", "localhost")
redis_client = MarketDataService(host=redis_host, port=6379, db=0)


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


def execute_trade(opportunity_raw) -> dict:
    """
    Convert raw trade opportunity data into a TradeOpportunity model,
    simulate a trade execution, and return a TradeExecution model (as dict).
    """
    try:
        # Assume opportunity_raw is a dict that can be parsed by TradeOpportunity.
        opp = TradeOpportunity(**opportunity_raw)
    except Exception as e:
        logger.error(f"Error parsing trade opportunity: {e}")
        return {}

    # Simulate trade execution (for example, call an exchange API here).
    # In this example, we simply mimic execution by copying prices and adding an execution timestamp.
    execution_ts = time.time()

    trade_exec = TradeExecution(
        asset=opp.asset,
        buy_exchange=opp.buy_exchange,
        sell_exchange=opp.sell_exchange,
        executed_buy_price=opp.buy_price,
        executed_sell_price=opp.sell_price,
        spread=opp.spread,
        volume=opp.volume,
        execution_timestamp=execution_ts,
        execution_id=f"{opp.asset}-{int(execution_ts)}",  # for example, a simple id
    )

    trade_msg = (
        f"Executed trade for {trade_exec.asset}: "
        f"Buy from {trade_exec.buy_exchange} at {trade_exec.executed_buy_price}, "
        f"Sell on {trade_exec.sell_exchange} at {trade_exec.executed_sell_price}, "
        f"Spread: {trade_exec.spread:.4f}, Volume: {trade_exec.volume}, "
        f"Timestamp: {trade_exec.execution_timestamp}"
    )
    logger.critical(trade_msg)

    try:
        # Publish or store the trade execution in Redis (as an example).
        redis_client.publish_trade_execution(trade_exec.dict())
        logger.debug("Trade execution stored successfully in Redis.")
    except Exception as e:
        logger.error(f"Error storing trade execution: {e}")

    return trade_exec.dict()  # Return as a dict for downstream serialization.


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
