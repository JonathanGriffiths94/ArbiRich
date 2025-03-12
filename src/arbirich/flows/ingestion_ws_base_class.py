import asyncio
import logging

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.run import cli_main

from arbirich.processing.process_ingestion import process_order_book
from arbirich.sinks.order_book import store_order_book
from arbirich.sources.websocket_partition import MultiExchangeSource
from src.arbirich.config import REDIS_CONFIG
from src.arbirich.io.websockets.load_websocket_processor import load_processor
from src.arbirich.redis_manager import MarketDataService
from src.arbirich.utils.helpers import build_exchanges_dict

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

redis_client = MarketDataService(
    host=REDIS_CONFIG["host"], port=REDIS_CONFIG["port"], db=REDIS_CONFIG["db"]
)


def build_flow():
    """
    Build the Bytewax dataflow:
      1. Input from MultiExchangeSource.
      2. Transformation to process the raw order book.
      3. Storing (and publishing) the processed order book.
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
    op.output("stdout", redis_sync, StdOutSink())
    return flow


async def run_ingestion_flow():
    try:
        logger.info("Starting ingestion pipeline...")
        flow = build_flow()

        logger.info("Running cli_main in a separate thread.")
        execution_task = asyncio.create_task(
            asyncio.to_thread(cli_main, flow, workers_per_process=1)
        )

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
