import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.run import cli_main

from src.arbirich.services.redis_service import RedisService

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Reset to INFO level

redis_client = RedisService()


def build_ingestion_flow(exchange_pairs: Dict[str, List[str]]):
    """
    Build the ingestion flow for market data.

    Parameters:
        exchange_pairs: Dictionary mapping exchange names to lists of pairs to monitor
    """
    logger.info(f"Building ingestion flow for {len(exchange_pairs)} exchanges")
    flow = Dataflow("ingestion")

    # Import here to avoid circular imports
    from src.arbirich.factories.processor_factory import get_processor_for_exchange
    from src.arbirich.flows.ingestion.ingestion_process import process_order_book
    from src.arbirich.flows.ingestion.ingestion_sink import RedisOrderBookSink
    from src.arbirich.flows.ingestion.ingestion_source import MultiExchangeSource

    # Create source that fetches market data from exchanges
    source = MultiExchangeSource(
        exchanges_and_pairs=exchange_pairs,
        processor_factory=get_processor_for_exchange,
    )
    stream = op.input("exchange_input", flow, source)

    # Basic pipeline without complex transformations: input -> process -> filter -> output
    processed = op.map("process_market_data", stream, process_order_book)
    valid_data = op.filter("filter_valid", processed, lambda x: x is not None)

    # Use Redis sink to publish to Redis
    redis_sink = RedisOrderBookSink()
    op.output("redis_output", valid_data, redis_sink)

    # Output to stdout for debugging
    op.output("stdout", valid_data, StdOutSink())

    return flow


async def run_ingestion_flow(exchange_pairs: Dict[str, List[str]]):
    """
    Run the ingestion flow for fetching market data.

    Parameters:
        exchange_pairs: Dictionary mapping exchange names to lists of pairs to monitor
    """
    executor = None
    stop_event = threading.Event()

    try:
        logger.info(f"Starting ingestion pipeline for {len(exchange_pairs)} exchanges")
        flow = build_ingestion_flow(exchange_pairs)

        # Create executor for running bytewax
        executor = ThreadPoolExecutor(max_workers=1)

        # Setup a thread to monitor for cancellation - with minimal command-line args
        def run_bytewax():
            try:
                # Configure command line args - but WITHOUT signal handling flags
                import sys

                original_argv = sys.argv
                sys.argv = [original_argv[0], "--workers", "1", "--no-recovery"]  # Added --no-recovery to avoid signals

                try:
                    # We still have to use cli_main, but with minimal options
                    cli_main(flow)
                except Exception as e:
                    if "signal only works" in str(e):
                        logger.warning("Ignoring signal handling error in thread")
                    else:
                        logger.error(f"Error running bytewax: {e}")
                finally:
                    # Restore original argv
                    sys.argv = original_argv
            except Exception as e:
                logger.error(f"Error in bytewax thread: {e}")
            finally:
                logger.info("Bytewax ingestion thread completed")

        # Start bytewax in separate thread
        future = executor.submit(run_bytewax)

        # Monitor for cancellation
        while not future.done() and not stop_event.is_set():
            await asyncio.sleep(0.1)

            # Check if we should stop based on task cancellation
            if asyncio.current_task().cancelled():
                logger.info("Task cancellation detected, stopping ingestion flow...")
                stop_event.set()
                break

        if stop_event.is_set() and not future.done():
            logger.info("Cancellation detected, attempting to interrupt bytewax...")
            # We can't directly interrupt bytewax, but we can shut down the executor
            executor.shutdown(wait=False)

        # We won't wait for the future since it might be uninterruptible
        # Just log that we're moving on
        if not future.done():
            logger.info("Bytewax still running, proceeding with cleanup anyway")

    except asyncio.CancelledError:
        logger.info("Ingestion task cancelled")
        stop_event.set()  # Set the stop event to signal the thread to stop
        if executor:
            executor.shutdown(wait=False)
        raise
    except Exception as e:
        logger.error(f"Error in ingestion flow: {e}")
    finally:
        # Clean up resources
        if executor and not executor._shutdown:
            executor.shutdown(wait=False)
        redis_client.close()
        logger.info("Ingestion flow shutdown completed")


# Export the flow for CLI usage
if __name__ == "__main__":
    import json
    import sys

    # Example exchange_pairs
    default_exchange_pairs = {"bybit": ["BTC-USDT", "ETH-USDT"], "cryptocom": ["BTC-USDT", "ETH-USDT"]}

    # Allow specifying exchange_pairs via command line
    exchange_pairs_json = sys.argv[1] if len(sys.argv) > 1 else json.dumps(default_exchange_pairs)
    exchange_pairs = json.loads(exchange_pairs_json)

    asyncio.run(run_ingestion_flow(exchange_pairs))
