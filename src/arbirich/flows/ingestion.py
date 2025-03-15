import asyncio
import logging
from typing import Dict, List

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.run import cli_main

from src.arbirich.exchange_processors.registry import register_all_processors
from src.arbirich.processing.ingestion_process import process_order_book
from src.arbirich.sinks.ingestion_sink import store_order_book
from src.arbirich.sources.arbitrage_source import get_shared_redis_client
from src.arbirich.sources.ingestion_source import MultiExchangeSource

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Use shared Redis client
redis_client = get_shared_redis_client()

# Register all processors at module load time
registered_processors = register_all_processors()
logger.info(f"Registered processors: {registered_processors}")


def get_processor_class(exchange):
    """
    Get the appropriate processor class for an exchange.
    Import is done here to avoid circular imports.
    """
    try:
        from src.arbirich.exchange_processors.processor_factory import get_processor_for_exchange

        processor = get_processor_for_exchange(exchange)
        logger.info(f"Got processor for {exchange}: {processor.__name__}")
        return processor
    except Exception as e:
        logger.error(f"Error getting processor for {exchange}: {e}")
        # Even in case of error, return a default processor
        from src.arbirich.exchange_processors.default_processor import DefaultOrderBookProcessor

        return DefaultOrderBookProcessor


def build_ingestion_flow(exchanges_and_pairs: Dict[str, List[str]]):
    """
    Build the ingestion flow for processing order book data from multiple exchanges.

    Parameters:
        exchanges_and_pairs: Dict mapping exchange names to lists of asset pairs
    """
    logger.info(f"Building ingestion flow with exchanges: {exchanges_and_pairs}")
    flow = Dataflow("ingestion")

    # Create the multi-exchange source
    source = MultiExchangeSource(exchanges_and_pairs, get_processor_class)

    # Create the input stream
    input_stream = op.input("exchange_input", flow, source)

    # Process order book updates
    processed_stream = op.map("process_order_book", input_stream, process_order_book)

    # Store and publish order book updates
    stored_stream = op.map("store_order_book", processed_stream, store_order_book)

    # Add a simple formatter for stdout (optional)
    def format_for_output(order_book):
        if order_book:
            return f"{order_book.exchange}:{order_book.symbol} - Bids: {len(order_book.bids)}, Asks: {len(order_book.asks)}"
        return "None"

    formatted_stream = op.map("format_output", stored_stream, format_for_output)

    # Output to stdout for debugging
    op.output("stdout", formatted_stream, StdOutSink())

    logger.info("Ingestion flow built successfully")
    return flow


async def run_ingestion_flow(exchanges_and_pairs: Dict[str, List[str]]):
    """
    Run the ingestion flow asynchronously.

    Parameters:
        exchanges_and_pairs: Dict mapping exchange names to lists of asset pairs
    """
    try:
        logger.info("Starting ingestion pipeline...")
        logger.info(f"Exchanges and assets: {exchanges_and_pairs}")

        flow = build_ingestion_flow(exchanges_and_pairs)

        logger.info("Running cli_main in a separate thread.")
        execution_task = asyncio.create_task(
            asyncio.to_thread(cli_main, flow, workers_per_process=1), name="ingestion-flow"
        )

        # Allow interruption to propagate
        try:
            await execution_task
        except asyncio.CancelledError:
            logger.info("Ingestion task cancelled")
            raise
        logger.info("Ingestion flow has finished running.")
    except asyncio.CancelledError:
        logger.info("Ingestion flow cancelled")
        raise
    except Exception as e:
        logger.exception(f"Error in ingestion flow: {e}")
    finally:
        logger.info("Ingestion flow shutdown")
        # Note: Don't close Redis client here as it might be shared among multiple flows
