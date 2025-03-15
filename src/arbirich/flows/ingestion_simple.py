import asyncio
import logging
import time

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.run import cli_main

from src.arbirich.sources.arbitrage_source import get_shared_redis_client

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Use shared Redis client
redis_client = get_shared_redis_client()


class DummySource:
    """A simple dummy source that produces minimal data for testing"""

    def list_parts(self):
        """List the partitions for this source"""
        return ["dummy1", "dummy2"]

    def build_part(self, step_id, for_key, resume_state):
        """Build a partition"""
        return DummyPartition(for_key)


class DummyPartition:
    """A simple dummy partition that produces minimal data"""

    def __init__(self, name):
        self.name = name
        self.counter = 0

    def next_batch(self):
        """Generate a small batch of data"""
        time.sleep(0.5)  # Sleep to avoid CPU spinning
        self.counter += 1
        if self.counter % 10 == 0:
            logger.info(f"Generated batch {self.counter} for {self.name}")
        return [("exchange", "product", {"bids": {"100.0": "1.0"}, "asks": {"101.0": "1.0"}, "timestamp": time.time()})]

    def snapshot(self):
        """Return snapshot state"""
        return None


def dummy_process(item):
    """A simple processing function"""
    try:
        exchange, product, data = item
        logger.debug(f"Processing {exchange}:{product}")
        return {
            "exchange": exchange,
            "symbol": product,
            "bids": data.get("bids", {}),
            "asks": data.get("asks", {}),
            "timestamp": data.get("timestamp", time.time()),
        }
    except Exception as e:
        logger.error(f"Error in dummy_process: {e}")
        return None


def dummy_sink(item):
    """A simple sink function"""
    if item:
        logger.debug(f"Sink received: {item}")
    return item


def build_simple_flow():
    """Build a simple flow for testing"""
    logger.info("Building simple ingestion flow")
    flow = Dataflow("simple_ingestion")

    # Use a dummy source to avoid potential issues with exchange processors
    source = DummySource()
    stream = op.input("dummy_input", flow, source)

    # Apply simple processing
    processed = op.map("dummy_process", stream, dummy_process)

    # Apply simple sink
    sink_output = op.map("dummy_sink", processed, dummy_sink)

    # Output to stdout
    op.output("stdout", sink_output, StdOutSink())

    logger.info("Simple ingestion flow built successfully")
    return flow


async def run_simple_flow():
    """Run the simple flow"""
    try:
        logger.info("Starting simple ingestion pipeline...")
        flow = build_simple_flow()

        logger.info("Running simple flow in a separate thread")
        execution_task = asyncio.create_task(
            asyncio.to_thread(cli_main, flow, workers_per_process=1), name="simple-ingestion-flow"
        )

        try:
            await execution_task
        except asyncio.CancelledError:
            logger.info("Simple ingestion task cancelled")
            raise
        logger.info("Simple ingestion flow has finished running")
    except asyncio.CancelledError:
        logger.info("Simple ingestion flow cancelled")
        raise
    except Exception as e:
        logger.exception(f"Error in simple ingestion flow: {e}")
    finally:
        logger.info("Simple ingestion flow shutdown")
