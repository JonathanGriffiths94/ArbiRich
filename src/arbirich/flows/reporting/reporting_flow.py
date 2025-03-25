import asyncio
import logging
import signal
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor

from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax.run import cli_main

from arbirich.flows.reporting.reporting_sink import db_sink
from arbirich.flows.reporting.reporting_source import RedisReportingSource
from src.arbirich.config.config import STRATEGIES
from src.arbirich.constants import TRADE_EXECUTIONS_CHANNEL, TRADE_OPPORTUNITIES_CHANNEL
from src.arbirich.services.database.database_service import DatabaseService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

db_manager = DatabaseService()

# Create a stop event for graceful shutdown
_stop_event = threading.Event()
# Daemon flag to control flow continuation
_daemon_running = True
# Create a thread pool executor
_executor = ThreadPoolExecutor(max_workers=1)
# Main flow thread
_flow_thread = None


def build_reporting_flow():
    """Build the  dataflow"""
    # Create a flow with a specific ID
    flow = Dataflow("reporting_flow")

    # Start with the main channels
    channels = [
        TRADE_OPPORTUNITIES_CHANNEL,
        TRADE_EXECUTIONS_CHANNEL,
    ]

    # Add strategy-specific channels
    for strategy_name in STRATEGIES.keys():
        channels.append(f"{TRADE_OPPORTUNITIES_CHANNEL}:{strategy_name}")
        channels.append(f"{TRADE_EXECUTIONS_CHANNEL}:{strategy_name}")

    logger.info(f"Building reporting flow with channels: {channels}")

    # Create the source with stop event
    source = RedisReportingSource(channels, stop_event=_stop_event)

    # Create the input stream
    input_stream = op.input("redis_input", flow, source)

    # Map the stream through the sink function (treating it as a map not a sink)
    # This avoids bytewax crashes from sink functions that return None
    output_stream = op.map("db_process", input_stream, db_sink)

    # Add a simple stdout sink that doesn't do any processing
    op.inspect("results", output_stream)

    return flow


def run_flow_with_direct_api():
    """Run the flow using cli_main directly with better shutdown handling"""
    global _daemon_running, _flow_thread

    logger.info("Starting reporting flow with direct API")
    try:
        flow = build_reporting_flow()

        # Create a function that periodically checks for stop flag
        def check_stop_condition():
            global _daemon_running
            while _daemon_running and not _stop_event.is_set():
                time.sleep(0.5)
            # If we get here, we should exit
            if _stop_event.is_set():
                logger.info("Stop condition detected, terminating reporting flow")
                # Force exit the process if running in a subprocess
                if _flow_thread and _flow_thread.is_alive():
                    logger.info("Terminating flow thread")
                    sys.exit(0)

        # Start the checking thread
        check_thread = threading.Thread(target=check_stop_condition, daemon=True)
        check_thread.start()

        # Use cli_main with the flow
        logger.info("Running reporting flow with CLI main")
        cli_main(flow, workers_per_process=1)
        logger.info("Reporting flow completed normally")
    except SystemExit:
        logger.info("SystemExit received in reporting flow - this is expected during shutdown")
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received in reporting flow")
    except Exception as e:
        logger.error(f"Error running reporting flow: {e}")
        logger.error(traceback.format_exc())
    finally:
        _daemon_running = False
        logger.info("Reporting flow direct API exited")


async def run_reporting_flow():
    """Run the reporting flow with robust shutdown handling"""
    global _stop_event, _daemon_running, _flow_thread

    # Reset state
    _stop_event.clear()
    _daemon_running = True

    try:
        logger.info("Starting reporting pipeline...")

        # Start the flow in a separate thread that we can monitor and kill
        _flow_thread = threading.Thread(target=run_flow_with_direct_api, daemon=True)
        _flow_thread.start()
        logger.info("Reporting flow thread started")

        # Monitor thread status and handle cancellation
        while _flow_thread.is_alive() and _daemon_running:
            try:
                # Check periodically if we should continue
                await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                logger.info("Reporting flow task received cancellation signal")
                await stop_reporting_flow_async()
                # Re-raise to signal we're cancelled
                raise

        logger.info("Reporting flow monitor exited")
        return True

    except asyncio.CancelledError:
        logger.info("Reporting flow cancelled")
        raise
    except Exception as e:
        logger.error(f"Error in reporting flow: {e}")
        logger.error(traceback.format_exc())
        return False
    finally:
        logger.info("Reporting flow shutdown initiated")
        await stop_reporting_flow_async()


def stop_reporting_flow():
    """Signal the reporting flow to stop synchronously"""
    global _stop_event, _daemon_running, _flow_thread

    logger.info("Stop signal sent to reporting flow")
    _stop_event.set()
    _daemon_running = False

    # Give the thread a moment to clean up
    if _flow_thread and _flow_thread.is_alive():
        logger.info("Waiting for reporting flow thread to exit (max 3 seconds)...")
        _flow_thread.join(timeout=3.0)

        # If still alive, we need more aggressive shutdown
        if _flow_thread.is_alive():
            logger.warning("Reporting flow thread didn't exit cleanly, forcing shutdown")
            # We can't forcibly terminate a thread in Python, so we'll have to rely
            # on the daemon=True flag we set earlier

    # Clean up the executor
    logger.info("Shutting down thread pool executor")
    try:
        _executor.shutdown(wait=False)
    except Exception as e:
        logger.error(f"Error shutting down executor: {e}")


async def stop_reporting_flow_async():
    """Signal the reporting flow to stop asynchronously"""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, stop_reporting_flow)
    logger.info("Async reporting flow stop completed")


# For CLI usage
flow = build_reporting_flow()

if __name__ == "__main__":
    # Setup signal handlers for better shutdown
    def handle_exit_signal(sig, frame):
        logger.info(f"Received signal {sig}, initiating shutdown...")
        stop_reporting_flow()
        # Give a moment for cleanup
        time.sleep(1)
        sys.exit(0)

    # Register signal handlers
    signal.signal(signal.SIGINT, handle_exit_signal)
    signal.signal(signal.SIGTERM, handle_exit_signal)

    # Use the direct API method
    run_flow_with_direct_api()
