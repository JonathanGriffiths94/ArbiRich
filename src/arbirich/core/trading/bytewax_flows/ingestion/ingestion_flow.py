import logging
import signal
import sys
import time
from typing import Dict, List

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow

from src.arbirich.flows.common.flow_manager import BytewaxFlowManager
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


# Create the flow manager for ingestion
flow_manager = BytewaxFlowManager("ingestion")

# Store the current exchanges_and_pairs configuration
_current_exchanges_and_pairs = None


def build_ingestion_flow(exchange=None, trading_pair=None):
    """
    Build the ingestion flow for a specific exchange and trading pair.

    Args:
        exchange (str): The exchange to pull data from
        trading_pair (str): The trading pair to monitor

    Returns:
        Dataflow: The configured Bytewax dataflow
    """
    global _current_exchanges_and_pairs

    # Configure for a specific exchange and pair if provided
    if exchange and trading_pair:
        exchanges_and_pairs = {exchange: [trading_pair]}
        logger.info(f"Building ingestion flow for {exchange}:{trading_pair}")
    elif _current_exchanges_and_pairs:
        # Use previously configured exchanges and pairs
        exchanges_and_pairs = _current_exchanges_and_pairs
        logger.info(f"Building ingestion flow with existing configuration: {exchanges_and_pairs}")
    else:
        # Use default configuration if none was provided
        from src.arbirich.config.config import EXCHANGES, PAIRS

        exchanges_and_pairs = {}
        for exch in EXCHANGES:
            exchanges_and_pairs[exch] = []
            for base, quote in PAIRS:
                exchanges_and_pairs[exch].append(f"{base}-{quote}")

        _current_exchanges_and_pairs = exchanges_and_pairs
        logger.info(f"Building ingestion flow with default configuration: {exchanges_and_pairs}")

    # Create the dataflow
    flow = Dataflow("ingestion_flow")

    # Create the multi-exchange source
    source = MultiExchangeSource(exchanges_and_pairs, get_processor_class, stop_event=flow_manager.stop_event)

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

    logger.info(f"Ingestion flow built successfully for {'specific exchange' if exchange else 'multiple exchanges'}")
    return flow


# Set the build_flow method on the manager
flow_manager.build_flow = build_ingestion_flow


async def run_ingestion_flow(exchanges_and_pairs: Dict[str, List[str]] = None):
    """
    Run the ingestion flow asynchronously.

    Parameters:
        exchanges_and_pairs: Dict mapping exchange names to lists of asset pairs
    """
    global _current_exchanges_and_pairs

    # Update the configuration if provided
    if exchanges_and_pairs:
        _current_exchanges_and_pairs = exchanges_and_pairs

    logger.info("Starting ingestion pipeline...")
    logger.info(f"Exchanges and assets: {_current_exchanges_and_pairs}")

    # Run the flow using the manager
    return await flow_manager.run_flow()


def stop_ingestion_flow():
    """Signal the ingestion flow to stop synchronously"""
    # Ensure redis client gets closed
    try:
        _redis_client.close()
    except Exception as e:
        logger.error(f"Error closing Redis client: {e}")

    return flow_manager.stop_flow()


async def stop_ingestion_flow_async():
    """Signal the ingestion flow to stop asynchronously"""
    logger.info("Stopping ingestion flow asynchronously...")
    try:
        _redis_client.close()
    except Exception as e:
        logger.error(f"Error closing Redis client: {e}")

    await flow_manager.stop_flow_async()
    logger.info("Ingestion flow stopped")


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

    # Set the current configuration
    _current_exchanges_and_pairs = exchange_pairs

    # Setup signal handlers for better shutdown
    def handle_exit_signal(sig, frame):
        logger.info(f"Received signal {sig}, initiating shutdown...")
        stop_ingestion_flow()
        # Give a moment for cleanup
        time.sleep(1)
        sys.exit(0)

    # Register signal handlers
    signal.signal(signal.SIGINT, handle_exit_signal)
    signal.signal(signal.SIGTERM, handle_exit_signal)

    # Run the flow using the manager
    flow_manager.run_flow_with_direct_api()
