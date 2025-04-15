import logging
import signal
import time
from typing import Dict, List

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow

from src.arbirich.core.state.system_state import mark_system_shutdown
from src.arbirich.core.trading.flows.bytewax_flows.common.redis_utils import close_redis_client, get_redis_client
from src.arbirich.core.trading.flows.bytewax_flows.common.shutdown_utils import setup_force_exit_timer
from src.arbirich.core.trading.flows.bytewax_flows.ingestion.ingestion_process import process_order_book
from src.arbirich.core.trading.flows.bytewax_flows.ingestion.ingestion_sink import publish_order_book
from src.arbirich.core.trading.flows.bytewax_flows.ingestion.ingestion_source import (
    MultiExchangeSource,
    mark_ingestion_force_kill,
)
from src.arbirich.core.trading.flows.flow_manager import BytewaxFlowManager
from src.arbirich.services.exchange_processors.registry import register_all_processors

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create shared Redis client
_redis_client = None


def get_flow_redis_client():
    global _redis_client
    if _redis_client is None:
        _redis_client = get_redis_client("ingestion_flow")
    return _redis_client


# Register all processors at module load time
registered_processors = register_all_processors()
logger.info(f"Registered processors: {list(registered_processors.keys())}")


def get_processor_class(exchange):
    """
    Get the appropriate processor class for an exchange using the ExchangeType enum.
    """
    try:
        from src.arbirich.models.enums import ExchangeType

        processor_class = ExchangeType.get_processor_class(exchange)
        logger.info(f"Got processor for {exchange}: {processor_class.__name__}")
        return processor_class
    except Exception as e:
        logger.error(f"Error getting processor for {exchange} from enum: {e}")
        # Even in case of error, return a default processor
        from src.arbirich.services.exchange_processors.default_processor import DefaultOrderBookProcessor

        return DefaultOrderBookProcessor


# Create the flow manager for ingestion
flow_manager = BytewaxFlowManager("ingestion")

# Store the current exchanges_and_pairs configuration
_current_exchanges_and_pairs = None


def build_ingestion_flow(exchange=None, trading_pair=None, debug_mode=False):
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
        flow_id = f"ingestion_{exchange}_{trading_pair}"
        logger.info(f"Building specific ingestion flow '{flow_id}' for {exchange}:{trading_pair}")
    elif _current_exchanges_and_pairs:
        # Use previously configured exchanges and pairs
        exchanges_and_pairs = _current_exchanges_and_pairs
        flow_id = "ingestion_multiple"
        logger.info(f"Building multi-exchange ingestion flow with existing configuration: {exchanges_and_pairs}")
    else:
        # Use default configuration if none was provided
        from src.arbirich.config.config import EXCHANGES, PAIRS

        exchanges_and_pairs = {}
        for exch in EXCHANGES:
            exchanges_and_pairs[exch] = []
            for base, quote in PAIRS:
                exchanges_and_pairs[exch].append(f"{base}-{quote}")

        _current_exchanges_and_pairs = exchanges_and_pairs
        flow_id = "ingestion_default"
        logger.info(f"Building default ingestion flow with configuration: {exchanges_and_pairs}")

    # Create the dataflow
    flow = Dataflow(flow_id)

    # Create the multi-exchange source
    source = MultiExchangeSource(exchanges_and_pairs, get_processor_class, stop_event=flow_manager.stop_event)

    # Create the input stream
    input_stream = op.input("exchange_input", flow, source)

    # Process order book updates with error handling
    def safe_process_order_book(data):
        try:
            if data is None:
                logger.warning(f"[{flow_id}] Received None data from source")
                return None

            logger.debug(f"[{flow_id}] Processing data: {data}")

            # Add additional validation and debugging
            if isinstance(data, tuple) and len(data) == 2:
                exchange, book_data = data
                logger.info(f"[{flow_id}] Processing order book from {exchange}")

                # Validate the book data has required fields
                if hasattr(book_data, "bids") and hasattr(book_data, "asks"):
                    bid_count = len(book_data.bids) if book_data.bids else 0
                    ask_count = len(book_data.asks) if book_data.asks else 0
                    logger.info(f"[{flow_id}] Order book from {exchange} has {bid_count} bids and {ask_count} asks")

                    # Check if bids/asks are in the right format (dict)
                    if not isinstance(book_data.bids, dict):
                        logger.warning(f"[{flow_id}] Bids is not a dict: {type(book_data.bids)}")
                        # Convert to dict if it's a list of [price, qty] pairs
                        if isinstance(book_data.bids, list):
                            book_data.bids = {str(item[0]): float(item[1]) for item in book_data.bids if len(item) >= 2}
                            logger.info(f"[{flow_id}] Converted bids to dict: {len(book_data.bids)} entries")

                    if not isinstance(book_data.asks, dict):
                        logger.warning(f"[{flow_id}] Asks is not a dict: {type(book_data.asks)}")
                        # Convert to dict if it's a list of [price, qty] pairs
                        if isinstance(book_data.asks, list):
                            book_data.asks = {str(item[0]): float(item[1]) for item in book_data.asks if len(item) >= 2}
                            logger.info(f"[{flow_id}] Converted asks to dict: {len(book_data.asks)} entries")
                else:
                    logger.warning(f"[{flow_id}] Order book missing bids or asks: {book_data}")

            # Process the order book
            result = process_order_book(data)
            logger.debug(f"[{flow_id}] Processed order book: {result}")
            return result
        except Exception as e:
            logger.error(f"[{flow_id}] Error in process_order_book: {e}", exc_info=True)
            # Log the input data to help diagnose the issue
            logger.error(f"[{flow_id}] Input data: {data}")
            return None

    processed_stream = op.map("process_order_book", input_stream, safe_process_order_book)

    # Store and publish order book updates with error handling and null check
    def safe_publish_order_book(order_book):
        try:
            # Skip None values
            if order_book is None:
                return None

            # For debug: check type and log it
            logger.debug(f"[{flow_id}] Publishing type: {type(order_book)}")

            # Handle both tuples and objects
            if isinstance(order_book, tuple):
                # If it's a tuple, get the first element (assuming it's the actual order book)
                if len(order_book) > 0:
                    actual_order_book = order_book[0]
                    logger.info(
                        f"[{flow_id}] Publishing tuple order book: {actual_order_book.exchange}:{actual_order_book.symbol}"
                    )
                    return publish_order_book(actual_order_book.exchange, actual_order_book.symbol, actual_order_book)
                else:
                    logger.warning(f"[{flow_id}] Received empty tuple as order book")
                    return None
            else:
                # Regular order book object
                logger.info(f"[{flow_id}] Publishing order book: {order_book.exchange}:{order_book.symbol}")
                return publish_order_book(order_book.exchange, order_book.symbol, order_book)
        except Exception as e:
            logger.error(f"[{flow_id}] Error in publish_order_book: {e}", exc_info=True)
            logger.error(f"[{flow_id}] order_book type: {type(order_book)}, value: {order_book}")
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
    # Set force kill flag
    mark_ingestion_force_kill()

    # Close the Redis client
    close_redis_client("ingestion_flow")

    # Return the flow manager's stop result
    return flow_manager.stop_flow()


async def stop_ingestion_flow_async():
    """Signal the ingestion flow to stop asynchronously"""
    logger.info("Stopping ingestion flow asynchronously...")

    # Set force kill flag
    mark_ingestion_force_kill()

    # Close Redis client
    close_redis_client("ingestion_flow")

    # Stop the flow
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

        try:
            # First check if we are the runner thread to avoid self-joining
            import threading

            current_thread_id = threading.get_ident()

            # Set system shutdown flag to notify all components
            mark_system_shutdown(True)
            mark_ingestion_force_kill()

            # Just set the stop event first
            flow_manager.stop_event.set()

            # Give a moment for resources to clean up
            time.sleep(0.5)

            # Close Redis client early to avoid connection issues
            close_redis_client("ingestion_flow")

            # Only try to join the thread if we're not the same thread
            if hasattr(flow_manager, "runner_thread") and flow_manager.runner_thread:
                if flow_manager.runner_thread.ident != current_thread_id:
                    logger.info("Waiting for flow runner thread to complete...")
                    flow_manager.runner_thread.join(timeout=2.0)
                else:
                    logger.info("Running in flow thread, skipping thread join")

            # In case the thread didn't exit, force exit
            logger.info("Exiting application...")
            # Use a small delay to allow logging to complete
            time.sleep(0.1)

            # Set up a failsafe to force exit if needed
            setup_force_exit_timer(3.0)
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
            # Force exit as a last resort
            import os

            os._exit(1)

    # Register signal handlers
    signal.signal(signal.SIGINT, handle_exit_signal)
    signal.signal(signal.SIGTERM, handle_exit_signal)

    # Run the flow using the manager
    flow_manager.run_flow_with_direct_api()
