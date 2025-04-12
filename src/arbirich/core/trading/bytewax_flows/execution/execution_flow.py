import logging
import signal
import sys
import time

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow

from src.arbirich.flows.common.flow_manager import BytewaxFlowManager
from src.arbirich.flows.execution.execution_sink import execute_trade
from src.arbirich.flows.execution.execution_source import RedisExecutionSource
from src.arbirich.services.redis.redis_service import RedisService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

redis_client = RedisService()

# Create the flow manager for execution
flow_manager = BytewaxFlowManager.get_or_create("execution")

# Store the current strategy_name configuration
_current_strategy_name = None


def build_execution_flow(strategy_name=None):
    """
    Build the execution flow.

    Args:
        strategy_name (str): Optional name of strategy to filter opportunities

    Returns:
        Dataflow: The configured Bytewax dataflow
    """
    global _current_strategy_name

    # Use provided parameter or fall back to stored value
    strategy = strategy_name if strategy_name is not None else _current_strategy_name

    # Update stored value for future use
    _current_strategy_name = strategy

    logger.info(f"Building execution flow for strategy: {strategy or 'all'}")
    flow = Dataflow("execution_flow")

    # Create a RedisExecutionSource with the strategy_name and stop_event
    source = RedisExecutionSource(strategy_name=strategy, stop_event=flow_manager.stop_event)
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


# Set the build_flow method on the manager
flow_manager.build_flow = build_execution_flow


async def run_execution_flow(strategy_name=None):
    """
    Run the execution flow for a specific strategy or all strategies.

    Parameters:
        strategy_name: Optional strategy name to filter opportunities
    """
    global _current_strategy_name

    # Update the configuration if provided
    if strategy_name is not None:
        _current_strategy_name = strategy_name

    logger.info(f"Starting execution pipeline for strategy: {_current_strategy_name or 'all'}...")

    # Run the flow using the manager
    return await flow_manager.run_flow()


def stop_execution_flow():
    """Signal the execution flow to stop synchronously"""
    # Ensure redis client gets closed
    try:
        redis_client.close()
    except Exception as e:
        logger.error(f"Error closing Redis client: {e}")

    return flow_manager.stop_flow()


async def stop_execution_flow_async():
    """Signal the execution flow to stop asynchronously"""
    logger.info("Stopping execution flow asynchronously...")
    await flow_manager.stop_flow_async()
    logger.info("Execution flow stopped")


# For CLI usage
if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Get the strategy name from command line args
    if len(sys.argv) > 1:
        strategy_name = sys.argv[1]
    else:
        # Get the first strategy from config
        from src.arbirich.config.config import STRATEGIES

        strategy_name = next(iter(STRATEGIES.keys()))

    # Set the current strategy configuration
    _current_strategy_name = strategy_name

    logger.info(f"Running execution flow for strategy: {strategy_name}")

    # Setup signal handlers for better shutdown
    def handle_exit_signal(sig, frame):
        logger.info(f"Received signal {sig}, initiating shutdown...")
        stop_execution_flow()
        # Give a moment for cleanup
        time.sleep(1)
        sys.exit(0)

    # Register signal handlers
    signal.signal(signal.SIGINT, handle_exit_signal)
    signal.signal(signal.SIGTERM, handle_exit_signal)

    # Run the flow using the manager
    flow_manager.run_flow_with_direct_api()
