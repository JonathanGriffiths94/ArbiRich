import asyncio
import logging
from typing import Dict, List

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.run import cli_main

from src.arbirich.flows.ingestion.ingestion_process import process_order_book
from src.arbirich.flows.ingestion.ingestion_sink import publish_order_book
from src.arbirich.flows.ingestion.ingestion_source import MultiExchangeSource
from src.arbirich.services.exchange_processors.registry import register_all_processors
from src.arbirich.services.redis.redis_service import RedisService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create shared Redis client
_redis_client = None


def get_redis_client():
    global _redis_client
    if _redis_client is None:
        _redis_client = RedisService()
    return _redis_client


# Register all processors at module load time
registered_processors = register_all_processors()
logger.info(f"Registered processors: {list(registered_processors.keys())}")


def get_processor_class(exchange):
    """
    Get the appropriate processor class for an exchange.
    Import is done here to avoid circular imports.
    """
    try:
        from src.arbirich.services.exchange_processors.processor_factory import get_processor_for_exchange

        processor = get_processor_for_exchange(exchange)
        logger.info(f"Got processor for {exchange}: {processor.__name__}")
        return processor
    except Exception as e:
        logger.error(f"Error getting processor for {exchange}: {e}")
        # Even in case of error, return a default processor
        from src.arbirich.services.exchange_processors.default_processor import DefaultOrderBookProcessor

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

    # Process order book updates with error handling
    def safe_process_order_book(data):
        try:
            result = process_order_book(data)
            logger.debug(f"Processed order book: {result}")
            return result
        except Exception as e:
            logger.error(f"Error in process_order_book: {e}", exc_info=True)
            # Log the input data to help diagnose the issue
            logger.error(f"Input data: {data}")
            return None

    processed_stream = op.map("process_order_book", input_stream, safe_process_order_book)

    # Store and publish order book updates with error handling and null check
    def safe_publish_order_book(order_book):
        try:
            # Skip None values
            if order_book is None:
                return None

            # For debug: check type and log it
            logger.debug(f"Publishing type: {type(order_book)}")

            # Handle both tuples and objects
            if isinstance(order_book, tuple):
                # If it's a tuple, get the first element (assuming it's the actual order book)
                if len(order_book) > 0:
                    actual_order_book = order_book[0]
                    logger.debug(f"Converting tuple to order book: {actual_order_book}")
                    return publish_order_book(actual_order_book.exchange, actual_order_book.symbol, actual_order_book)
                else:
                    logger.warning("Received empty tuple as order book")
                    return None
            else:
                # Regular order book object
                logger.debug(f"Publishing order book: exchange={order_book.exchange}, symbol={order_book.symbol}")
                return publish_order_book(order_book.exchange, order_book.symbol, order_book)
        except Exception as e:
            logger.error(f"Error in publish_order_book: {e}", exc_info=True)
            logger.error(f"order_book type: {type(order_book)}, value: {order_book}")
            return None

    stored_stream = op.map("store_order_book", processed_stream, safe_publish_order_book)

    # Filter out None results AFTER publishing
    result_stream = op.filter("filter_result", stored_stream, lambda x: x is not None)

    # Add a simple formatter for stdout (optional)
    def format_for_output(result):
        return f"Published: {result}"

    formatted_stream = op.map("format_output", result_stream, format_for_output)

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
    logger.info("Starting ingestion pipeline...")
    logger.info(f"Exchanges and assets: {exchanges_and_pairs}")

    try:
        # Build and run the ingestion flow
        logger.info(f"Building ingestion flow with exchanges: {exchanges_and_pairs}")

        flow = build_ingestion_flow(exchanges_and_pairs)

        logger.info("Ingestion flow built successfully")
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
        logger.error(f"Error in ingestion flow: {e}", exc_info=True)
    finally:
        logger.info("Ingestion flow shutdown")
        # Note: Don't close Redis client here as it might be shared among multiple flows


# For CLI usage
if __name__ == "__main__":
    from src.arbirich.config.config import EXCHANGES, PAIRS

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Create exchange_pairs mapping
    exchange_pairs = {}
    for exchange in EXCHANGES:
        exchange_pairs[exchange] = []
        for base, quote in PAIRS:
            exchange_pairs[exchange].append(f"{base}-{quote}")

    # Run ingestion flow
    asyncio.run(run_ingestion_flow(exchange_pairs))
