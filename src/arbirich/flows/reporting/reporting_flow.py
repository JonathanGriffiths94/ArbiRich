"""
Reporting dataflow for trade opportunities and executions.
"""

import logging
import signal
import sys
import threading
import time

from bytewax import operators as op
from bytewax.dataflow import Dataflow

from arbirich.flows.reporting.reporting_sink import db_sink
from src.arbirich.config.config import STRATEGIES
from src.arbirich.constants import TRADE_EXECUTIONS_CHANNEL, TRADE_OPPORTUNITIES_CHANNEL
from src.arbirich.flows.common.flow_manager import BytewaxFlowManager
from src.arbirich.flows.reporting.reporting_source import RedisReportingSource
from src.arbirich.services.database.database_service import DatabaseService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

db_manager = DatabaseService()
flow_manager = BytewaxFlowManager.get_or_create("reporting")

# Global variable to track the active flow
_active_flow = None
_flow_lock = threading.Lock()


def get_active_flow():
    """Get the currently active reporting flow."""
    global _active_flow
    with _flow_lock:
        return _active_flow


def set_active_flow(flow):
    """Set the active flow reference."""
    global _active_flow
    with _flow_lock:
        _active_flow = flow
    return flow


def stop_reporting_flow():
    """
    Stop the currently active reporting flow.
    Returns True if a flow was stopped, False otherwise.
    """
    global _active_flow
    with _flow_lock:
        flow = _active_flow
        if flow is None:
            logger.info("No active reporting flow to stop")
            return False

    # Set system shutdown flag
    from src.arbirich.core.system_state import mark_system_shutdown

    mark_system_shutdown(True)

    # Now stop the flow - this code depends on how your flow is structured
    try:
        # First try to stop using the flow manager
        logger.info("Stopping reporting flow using flow manager")
        flow_manager.stop_flow()

        # Also terminate all reporting partitions explicitly
        from src.arbirich.flows.reporting.reporting_source import terminate_all_partitions

        terminate_all_partitions()

        # Close Redis connections
        from src.arbirich.flows.reporting.reporting_source import reset_shared_redis_client

        reset_shared_redis_client()

        # Wait for a short time to allow cleanup
        time.sleep(0.5)

        # Clear reference
        with _flow_lock:
            _active_flow = None

        return True
    except Exception as e:
        logger.error(f"Error stopping reporting flow: {e}")
        return False


def build_reporting_flow():
    """Build the reporting dataflow"""
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
    source = RedisReportingSource(channels, stop_event=flow_manager.stop_event)

    # Create the input stream
    input_stream = op.input("redis_input", flow, source)

    # Map the stream through the sink function (treating it as a map not a sink)
    # This avoids bytewax crashes from sink functions that return None
    output_stream = op.map("db_process", input_stream, db_sink)

    # Add a simple stdout sink that doesn't do any processing
    op.inspect("results", output_stream)

    # Set this as the active flow
    flow = set_active_flow(flow)

    return flow


# Set up flow building in the manager
flow_manager.build_flow = build_reporting_flow


async def run_reporting_flow():
    """Run the reporting flow with robust shutdown handling"""
    return await flow_manager.run_flow()


def stop_reporting_flow_sync():
    """Signal the reporting flow to stop synchronously"""
    return flow_manager.stop_flow()


async def stop_reporting_flow_async():
    """Signal the reporting flow to stop asynchronously"""
    return await flow_manager.stop_flow_async()


# For CLI usage
if __name__ == "__main__":
    # Setup signal handlers for better shutdown
    def handle_exit_signal(sig, frame):
        logger.info(f"Received signal {sig}, initiating shutdown...")
        stop_reporting_flow_sync()
        # Give a moment for cleanup
        time.sleep(1)
        sys.exit(0)

    # Register signal handlers
    signal.signal(signal.SIGINT, handle_exit_signal)
    signal.signal(signal.SIGTERM, handle_exit_signal)

    # Build and run the flow directly when called as script
    flow = build_reporting_flow()
    try:
        import asyncio

        asyncio.run(run_reporting_flow())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, exiting...")
        stop_reporting_flow_sync()
