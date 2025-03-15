import asyncio
import logging
import time

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.run import cli_main

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class SuperSimpleSource:
    """
    A very simple source that requires no external dependencies.
    It produces one message per second per partition.
    """

    def __init__(self, exchange_pairs):
        self.exchange_pairs = exchange_pairs

    def list_parts(self):
        """Return a single partition"""
        return ["dummy_part"]

    def build_part(self, step_id, for_key, resume_state):
        """Build the dummy partition"""
        return DummyPartition(self.exchange_pairs)


class DummyPartition:
    """
    A dummy partition that produces synthetic order book data.
    """

    def __init__(self, exchange_pairs):
        self.exchange_pairs = exchange_pairs
        self.counter = 0

    def next_batch(self):
        """
        Produce a batch of synthetic data.
        One for each exchange-pair combination.
        """
        self.counter += 1
        batch = []

        # Sleep to avoid producing too much data
        time.sleep(1)

        timestamp = time.time()

        # Log every 10 batches
        if self.counter % 10 == 0:
            logger.info(f"Produced batch #{self.counter} at {timestamp}")

        # Create one item per exchange-pair combination
        for exchange, pairs in self.exchange_pairs.items():
            for pair in pairs:
                batch.append(
                    {
                        "exchange": exchange,
                        "symbol": pair,
                        "bids": {"100.0": "1.0"},
                        "asks": {"101.0": "1.0"},
                        "timestamp": timestamp,
                    }
                )

        return batch

    def snapshot(self):
        """No snapshot needed"""
        return None


def identity_map(item):
    """Identity function for mapping"""
    return item


def build_minimal_flow(exchange_pairs):
    """
    Build a minimal ingestion flow that is guaranteed to work.

    Parameters:
        exchange_pairs: A dictionary mapping exchanges to lists of pairs
    """
    logger.info("Building minimal ingestion flow")
    flow = Dataflow("minimal_ingestion")

    # Create the source
    source = SuperSimpleSource(exchange_pairs)
    input_stream = op.input("dummy_input", flow, source)

    # Pass through
    output = op.map("identity", input_stream, identity_map)

    # Output to stdout
    op.output("stdout", output, StdOutSink())

    logger.info("Minimal ingestion flow built successfully")
    return flow


async def run_minimal_flow(exchange_pairs):
    """
    Run the minimal ingestion flow.

    Parameters:
        exchange_pairs: A dictionary mapping exchanges to lists of pairs
    """
    try:
        logger.info("Starting minimal ingestion flow...")
        logger.info(f"Exchange pairs: {exchange_pairs}")

        flow = build_minimal_flow(exchange_pairs)

        logger.info("Running minimal flow in a separate thread")
        execution_task = asyncio.create_task(
            asyncio.to_thread(cli_main, flow, workers_per_process=1), name="minimal-ingestion-flow"
        )

        try:
            await execution_task
        except asyncio.CancelledError:
            logger.info("Minimal ingestion flow cancelled")
            raise
        except Exception as e:
            logger.exception(f"Error in minimal flow execution: {e}")

        logger.info("Minimal ingestion flow completed")
    except asyncio.CancelledError:
        logger.info("Minimal ingestion flow cancelled from outside")
        raise
    except Exception as e:
        logger.exception(f"Error in minimal ingestion flow: {e}")
    finally:
        logger.info("Minimal ingestion flow shutdown")
