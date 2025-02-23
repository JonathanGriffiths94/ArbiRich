import asyncio
import logging
import os
import threading
import time
from typing import List

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.run import cli_main

from src.arbirich.market_data_service import MarketDataService

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


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


class RedisOpportunityPartition(StatefulSourcePartition):
    def __init__(self, redis_client):
        logger.debug("Initializing RedisOpportunityPartition.")
        self.redis_client = redis_client
        self._queue = asyncio.Queue()
        self._running = True
        self._last_activity = time.time()
        self._task = None

        # Create a new event loop for the background thread.
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._start_loop, daemon=True)
        self._thread.start()

        # Schedule the async subscription coroutine to run on the background loop.
        asyncio.run_coroutine_threadsafe(self._run_subscription(), self._loop)

    def _start_loop(self):
        """Set up and run the event loop in a separate thread."""
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    async def _run_subscription(self) -> None:
        # Iterate over the async generator from subscribe_to_trade_opportunities.
        # Note: Ensure that redis_client.subscribe_to_trade_opportunities is an async generator.
        async for opp in self.redis_client.subscribe_to_trade_opportunities(
            lambda opp: logger.debug(f"Received: {opp}")
        ):
            if opp is not None:
                await self._queue.put(opp)

    def next_batch(self) -> list:
        # Synchronously attempt to retrieve an item from the asyncio queue.
        try:
            item = self._queue.get_nowait()
            self._last_activity = time.time()
            logger.debug(f"next_batch obtained message: {item}")
            return [item]
        except asyncio.QueueEmpty:
            return []

    def snapshot(self) -> dict:
        logger.debug(
            "Snapshot requested for RedisOpportunityPartition (returning empty dict)."
        )
        return {}


class RedisOpportunitySource(FixedPartitionedSource):
    def __init__(self, redis_client):
        self.redis_client = redis_client

    def list_parts(self) -> List[str]:
        parts = ["1"]
        logger.info(f"List of partition keys: {parts}")
        return parts

    def build_part(self, step_id, for_key, _resume_state):
        logger.info(f"Building partition for key: {for_key}")
        return RedisOpportunityPartition(self.redis_client)


def build_flow():
    """
    Build the execution flow.
    This flow uses a custom Redis source that yields trade opportunities,
    applies the execute_trade operator, and outputs the result.
    """
    logger.info("Building execution flow...")
    flow = Dataflow("execution")

    # Use the custom RedisOpportunitySource.
    source = RedisOpportunitySource(redis_client)
    stream = op.input("redis_opportunity_input", flow, source)
    logger.debug("Input stream created from RedisOpportunitySource.")

    # Risk management

    # Slippage check

    # Execute trade
    executed = op.map("execute_trade", stream, execute_trade)
    logger.debug("Applied execute_trade operator on stream.")

    op.output("stdout", executed, StdOutSink())
    return flow


async def run_executor_flow():
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
