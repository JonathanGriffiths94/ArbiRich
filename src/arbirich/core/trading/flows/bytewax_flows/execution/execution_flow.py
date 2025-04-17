import logging
import signal
import sys
import time
from typing import Optional

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow

from src.arbirich.core.state.system_state import mark_system_shutdown
from src.arbirich.core.trading.flows.bytewax_flows.common.redis_utils import reset_all_redis_connections
from src.arbirich.core.trading.flows.bytewax_flows.common.shutdown_utils import mark_force_kill, setup_force_exit_timer
from src.arbirich.core.trading.flows.bytewax_flows.execution.execution_process import (
    execute_trade,
    filter_for_strategy,
    log_opportunity,
)
from src.arbirich.core.trading.flows.bytewax_flows.execution.execution_source import RedisExecutionSource
from src.arbirich.core.trading.flows.flow_manager import BytewaxFlowManager
from src.arbirich.models import TradeExecution, TradeOpportunity

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Set up flow manager
flow_manager = BytewaxFlowManager.get_or_create("execution")

# Store current configuration
_current_strategy_name = None
_current_debug_mode = False


def build_execution_flow(strategy_name=None, debug_mode=False):
    """
    Build the execution flow for the specified strategy.

    Args:
        strategy_name (str): Name of the strategy to build the flow for
        debug_mode (bool): Whether to enable debug mode with additional logging

    Returns:
        Dataflow: The configured Bytewax dataflow
    """
    global _current_strategy_name, _current_debug_mode

    # Update the configuration
    _current_strategy_name = strategy_name
    _current_debug_mode = debug_mode

    flow_id = f"execution_{strategy_name}" if strategy_name else "execution_default"
    logger.info(f"Building execution flow: {flow_id} (debug={debug_mode})")

    flow = Dataflow(flow_id)

    # Set up source for opportunities from Redis
    # Pass the stop event to allow clean shutdown
    redis_source = RedisExecutionSource(strategy_name=strategy_name, stop_event=flow_manager.stop_event)
    input_stream = op.input("redis_input", flow, redis_source)

    # Add logging for incoming opportunities
    def log_incoming_opportunity(opportunity: TradeOpportunity):
        logger.info(f"EXECUTION FLOW: Received opportunity {opportunity.id} for {opportunity.pair}")
        return opportunity

    logged_input = op.map("log_incoming", input_stream, log_incoming_opportunity)

    # Filter for opportunities that match our strategy if strategy_name is provided
    if strategy_name:
        # Create filtered stream based on strategy name
        opportunity_stream = op.filter(
            "filter_opportunities", logged_input, lambda opp: filter_for_strategy(opp, strategy_name)
        )

        # Log filtered opportunities
        def log_filtered(opportunity: TradeOpportunity):
            logger.info(f"EXECUTION FLOW: Opportunity {opportunity.id} matches strategy {strategy_name}")
            return opportunity

        opportunity_stream = op.map("log_filtered", opportunity_stream, log_filtered)
    else:
        # If no strategy is specified, use all opportunities
        opportunity_stream = logged_input

    # Log opportunities for debugging
    if debug_mode:
        logged_stream = op.map("log_opportunity", opportunity_stream, log_opportunity)
    else:
        logged_stream = opportunity_stream

    # Execute trade with additional logging
    def execute_with_logging(opportunity: TradeOpportunity) -> Optional[TradeExecution]:
        logger.info(f"EXECUTION FLOW: Executing trade for opportunity {opportunity.id}")
        result = execute_trade(opportunity, strategy_name)
        if result:
            logger.info(f"EXECUTION FLOW: Trade execution successful for {opportunity.id}")
        else:
            logger.warning(f"EXECUTION FLOW: Trade execution failed for {opportunity.id}")
        return result

    execution_stream = op.map("execute_trade", logged_stream, execute_with_logging)

    # Filter out None results with logging
    def filter_with_logging(result):
        if result is None:
            logger.warning("EXECUTION FLOW: Filtering out None execution result")
            return False
        return True

    filtered_stream = op.filter("execution_filter", execution_stream, filter_with_logging)

    # Just pass through the executions as strings for the output sink
    def format_execution(execution: TradeExecution) -> str:
        try:
            if debug_mode:
                logger.info(f"EXECUTION FLOW: Formatted execution result: {execution}")
                return f"EXECUTION: {execution}"
            return f"executed: {execution.id}"
        except Exception as e:
            logger.error(f"Error formatting execution: {e}")
            return "error"

    formatted_stream = op.map("format_execution", filtered_stream, format_execution)

    # Add output using positional arguments instead of named parameters
    op.output("exec_result", formatted_stream, StdOutSink())

    logger.info(f"Execution flow {flow_id} built successfully")
    return flow


# Set the build_flow method on the manager
flow_manager.build_flow = build_execution_flow


async def run_execution_flow(strategy_name=None, debug_mode=False):
    """
    Run the execution flow with the given strategy name

    Args:
        strategy_name: Name of the strategy to use (optional)
        debug_mode: Whether to enable debug mode
    """
    logger.info(f"Starting execution flow with strategy={strategy_name}, debug={debug_mode}")

    # Update the current configuration
    global _current_strategy_name, _current_debug_mode
    _current_strategy_name = strategy_name
    _current_debug_mode = debug_mode

    # Make sure the flow builder uses the current configuration
    flow_manager.build_flow = lambda: build_execution_flow(
        strategy_name=_current_strategy_name, debug_mode=_current_debug_mode
    )

    # Run the flow using the manager
    return await flow_manager.run_flow()


def mark_execution_force_kill():
    """Mark the execution flow for force kill"""
    mark_force_kill("execution")

    # Additional cleanup
    reset_all_redis_connections()
    logger.info("Execution Redis connections reset")


def stop_execution_flow():
    """Stop the execution flow synchronously"""
    logger.info("Stopping execution flow synchronously")

    # Mark for force kill
    mark_execution_force_kill()

    # Make sure reporting system is notified of the execution stop
    try:
        import asyncio

        from src.arbirich.core.trading.flows.reporting.reporting_flow import get_flow

        # Try to get the reporting flow and notify it
        loop = asyncio.get_event_loop()
        if loop.is_running():

            async def notify_reporting():
                reporting_flow = await get_flow()
                if reporting_flow:
                    logger.info("Notifying reporting flow of execution stop")
                    # We don't need to wait for this

            # Create task but don't wait for it
            asyncio.create_task(notify_reporting())
        else:
            # In synchronous context, just log
            logger.info("Synchronous context - skipping reporting notification")
    except Exception as e:
        logger.warning(f"Could not notify reporting flow: {e}")

    # Now stop the execution flow
    return flow_manager.stop_flow()


async def stop_execution_flow_async():
    """Stop the execution flow asynchronously"""
    logger.info("Stopping execution flow asynchronously")

    # Mark for force kill
    mark_execution_force_kill()

    # Make sure reporting system is notified of the execution stop
    try:
        from src.arbirich.core.trading.flows.reporting.reporting_flow import get_flow

        # Try to get the reporting flow and notify it
        reporting_flow = await get_flow()
        if reporting_flow:
            logger.info("Notifying reporting flow of execution stop")
            # We don't need to wait for this
    except Exception as e:
        logger.warning(f"Could not notify reporting flow: {e}")

    # Now stop the execution flow
    return await flow_manager.stop_flow_async()


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
        strategy_name = None

    # Set current configuration
    _current_strategy_name = strategy_name
    _current_debug_mode = True

    # Set up signal handlers
    def handle_exit_signal(sig, frame):
        logger.info(f"Received signal {sig}, stopping execution flow")

        # Set system shutdown flag and mark for force kill
        mark_system_shutdown(True)
        mark_execution_force_kill()

        # Set up failsafe timer
        setup_force_exit_timer(3.0)

        # Try normal shutdown
        stop_execution_flow()
        time.sleep(1)  # Give a moment for cleanup
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_exit_signal)
    signal.signal(signal.SIGTERM, handle_exit_signal)

    # Update the build_flow method to include the required parameters
    flow_manager.build_flow = lambda: build_execution_flow(
        strategy_name=_current_strategy_name, debug_mode=_current_debug_mode
    )

    # Run the flow
    flow_manager.run_flow_with_direct_api()
