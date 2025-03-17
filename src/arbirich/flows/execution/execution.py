import asyncio
import logging
import signal
import threading
from concurrent.futures import ThreadPoolExecutor

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.run import cli_main

from src.arbirich.flows.execution.execution_sink import execute_trade
from src.arbirich.flows.execution.execution_source import RedisExecutionSource
from src.arbirich.services.redis_service import RedisService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

redis_client = RedisService()


def build_flow(strategy_name=None):
    """
    Build the execution flow.
    This flow uses a custom Redis source that yields trade opportunities,
    applies the execute_trade operator, and outputs the result.

    Parameters:
        strategy_name: Optional strategy name to filter opportunities
    """
    logger.info(f"Building execution flow for strategy: {strategy_name or 'all'}")
    flow = Dataflow("execution")

    # Create a RedisExecutionSource with the strategy_name
    source = RedisExecutionSource(strategy_name=strategy_name)
    stream = op.input("redis_input", flow, source)
    logger.debug("Input stream created from RedisExecutionSource.")

    executed = op.map("execute_trade", stream, execute_trade)
    logger.debug("Applied execute_trade operator on stream.")

    # Add a filter to remove empty results (failed executions)
    valid_executions = op.filter("filter_valid", executed, lambda x: x and len(x) > 0)
    logger.debug("Filtered out failed executions.")

    op.output("stdout", valid_executions, StdOutSink())
    logger.info("Output operator attached (StdOutSink).")
    return flow


async def run_execution_flow(strategy_name=None):
    """
    Run the execution flow for a specific strategy or all strategies.

    Parameters:
        strategy_name: Optional strategy name to filter opportunities
    """
    executor = None
    stop_event = threading.Event()

    def handle_stop_signal():
        logger.info("Stopping execution flow due to signal...")
        stop_event.set()

    try:
        logger.info(f"Starting execution pipeline for strategy: {strategy_name or 'all'}...")
        flow = build_flow(strategy_name)

        # Create our own executor to have better control
        executor = ThreadPoolExecutor(max_workers=1)

        # Setup a thread to monitor for cancellation
        def run_bytewax():
            # Setup signal handlers within the thread
            signal.signal(signal.SIGINT, lambda *args: handle_stop_signal())
            signal.signal(signal.SIGTERM, lambda *args: handle_stop_signal())

            # Use command line args that match our needs
            import sys

            original_argv = sys.argv
            sys.argv = [original_argv[0], "--workers", "1", "--process"]

            try:
                # Run until stop event is set
                if not stop_event.is_set():
                    cli_main(flow)
            except Exception as e:
                logger.error(f"Error in bytewax thread: {e}")
            finally:
                logger.info("Bytewax execution thread completed")
                sys.argv = original_argv

        # Start bytewax in a separate thread
        future = executor.submit(run_bytewax)

        # Monitor the future and wait for cancellation
        while not future.done() and not stop_event.is_set():
            await asyncio.sleep(0.1)

        if stop_event.is_set() and not future.done():
            logger.info("Cancellation detected, attempting to interrupt bytewax...")
            # We need to let the thread handle its own cancellation
            # via the stop_event we set earlier

        # Wait for the thread to finish with timeout
        try:
            await asyncio.wait_for(asyncio.to_thread(future.result), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("Timeout waiting for bytewax to finish")
        except Exception as e:
            logger.error(f"Error waiting for bytewax to finish: {e}")

    except asyncio.CancelledError:
        logger.info("Execution task cancelled")
        handle_stop_signal()
        raise
    except Exception as e:
        logger.error(f"Error in execution flow: {e}")
    finally:
        # Clean up resources
        if executor:
            executor.shutdown(wait=False)
        redis_client.close()
        logger.info("Execution flow shutdown completed")


# Expose the flow for CLI usage.
flow = build_flow()

if __name__ == "__main__":
    import sys

    # Allow specifying a strategy name via command line
    strategy_name = sys.argv[1] if len(sys.argv) > 1 else None
    asyncio.run(run_execution_flow(strategy_name))
