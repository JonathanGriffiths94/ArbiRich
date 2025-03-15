import asyncio
import logging

from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax.run import cli_main

from arbirich.sources.ingestion_source import MultiExchangeSource
from src.arbirich.io.websockets.load_websocket_processor import load_processor
from src.arbirich.processing.ingestion_process import process_order_book
from src.arbirich.services.redis_service import RedisService
from src.arbirich.sinks.ingestion_sink import store_order_book
from src.arbirich.utils.bytewax_sinks import NullSink
from src.arbirich.utils.helpers import build_exchanges_dict

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

redis_client = RedisService()


def build_flow(debug_mode=False):
    """
    Build the Bytewax dataflow:
      1. Input from MultiExchangeSource.
      2. Transformation to process the raw order book.
      3. Storing (and publishing) the processed order book.

    Parameters:
        debug_mode: Whether to show output (True) or discard it (False)
    """
    flow = Dataflow("ingestion")

    # Load exchanges and assets from config
    exchanges = build_exchanges_dict()
    logger.info(f"Exchanges and assets: {exchanges}")

    # Processor configurations for each exchange.
    source = MultiExchangeSource(exchanges=exchanges, processor_loader=load_processor)
    raw_stream = op.input("extract", flow, source)
    processed = op.map("transform", raw_stream, process_order_book)
    redis_sync = op.map("load", processed, store_order_book)

    # Use NullSink to avoid stdout output
    op.output("output", redis_sync, NullSink())
    return flow


async def run_ingestion_flow(debug_mode=False):
    try:
        logger.info("Starting ingestion pipeline...")
        flow = build_flow(debug_mode)

        logger.info("Running cli_main in a separate thread.")
        execution_task = asyncio.create_task(asyncio.to_thread(cli_main, flow, workers_per_process=1))

        try:
            await execution_task
        except asyncio.CancelledError:
            logger.info("Ingestion task cancelled")
            raise
        logger.info("cli_main has finished running.")
    except asyncio.CancelledError:
        logger.info("Execution task cancelled")
        raise
    finally:
        logger.info("Ingestion flow shutdown")
        redis_client.close()
