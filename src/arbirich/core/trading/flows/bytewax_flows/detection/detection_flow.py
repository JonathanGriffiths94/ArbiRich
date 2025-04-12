import logging
import signal
import sys
import time

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow

from src.arbirich.core.trading.flows.bytewax_flows.detection.detection_process import (
    detect_arbitrage,
    key_by_asset,
    update_asset_state,
)
from src.arbirich.core.trading.flows.bytewax_flows.detection.detection_sink import (
    debounce_opportunity,
    publish_trade_opportunity,
)
from src.arbirich.core.trading.flows.bytewax_flows.detection.detection_source import use_redis_opportunity_source
from src.arbirich.core.trading.flows.flow_manager import BytewaxFlowManager
from src.arbirich.services.redis.redis_service import RedisService
from src.arbirich.utils.strategy_manager import StrategyManager

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

redis_client = RedisService()

# Create the flow manager for detection
flow_manager = BytewaxFlowManager.get_or_create("detection")

# Store the current configuration
_current_strategy_name = None
_current_debug_mode = False


def build_detection_flow(strategy_name=None, debug_mode=False):
    """
    Build the detection flow for the specified strategy.

    Args:
        strategy_name (str): Name of the strategy to build the flow for
        debug_mode (bool): Whether to enable debug mode with extra logging

    Returns:
        Dataflow: The configured Bytewax dataflow
    """

    logger.info(f"Building detection flow for strategy: {strategy_name}...")
    flow = Dataflow(f"detection-{strategy_name}")  # Include strategy name in flow name

    # Get strategy-specific exchanges and channels
    exchange_channels = StrategyManager.get_exchange_channels(strategy_name)
    logger.info(f"Using exchanges for {strategy_name}: {list(exchange_channels.keys())}")

    # Get strategy-specific pairs
    pairs = StrategyManager.get_pairs_for_strategy(strategy_name)
    logger.info(f"Using trading pairs for {strategy_name}: {pairs}")

    input_stream = use_redis_opportunity_source(
        flow,
        "redis_input",
        exchange_channels,
        pairs,
        stop_event=flow_manager.stop_event,
    )

    # Wrap key_by_asset with error handling
    def safe_key_by_asset(record):
        try:
            result = key_by_asset(record)
            # Extra sanity check to ensure we always return a tuple with two elements
            if result is None or not isinstance(result, tuple) or len(result) != 2:
                logger.error(f"key_by_asset returned invalid result: {result}")
                # Return a placeholder that won't cause errors
                return "error", {"exchange": "error", "symbol": "error", "timestamp": time.time()}
            return result
        except Exception as e:
            logger.error(f"Error in safe_key_by_asset: {e}", exc_info=True)
            # Return a placeholder that won't cause errors
            return "error", {"exchange": "error", "symbol": "error", "timestamp": time.time()}

    # Group order books by trading pair
    keyed_stream = op.map("key_by_asset", input_stream, safe_key_by_asset)

    # Filter out error placeholders
    valid_stream = op.filter("filter_errors", keyed_stream, lambda x: x[0] != "error")

    asset_state_stream = op.stateful_map("asset_state", valid_stream, update_asset_state)

    # Filter not ready states
    ready_state = op.filter("ready", asset_state_stream, lambda kv: kv[1] is not None)

    # Get threshold from strategy manager
    threshold = StrategyManager.get_threshold(strategy_name)
    logger.info(f"Using threshold {threshold} for strategy {strategy_name}")

    # Create a detector function closure that includes the strategy name and threshold
    def arbitrage_detector(kv):
        # Call detect_arbitrage with the asset, state, threshold, and strategy name
        return detect_arbitrage(kv[0], kv[1], threshold, strategy_name)

    # Detect arbitrage on the state
    arb_stream = op.map(
        "detect_arbitrage",
        ready_state,
        arbitrage_detector,
    )

    # Filter out None opportunities
    arb_opportunities = op.filter("arb_filter", arb_stream, lambda x: x is not None)

    # Add strategy name to the opportunity key for deduplication
    def add_strategy_to_debounce(opportunity):
        return debounce_opportunity(redis_client, opportunity, strategy_name=strategy_name)

    debounced_opportunities = op.map(
        "debounce_opportunity",
        arb_opportunities,
        add_strategy_to_debounce,
    )

    # Filter out None values from debouncer
    final_opp = op.filter("final_filter", debounced_opportunities, lambda x: x is not None)

    # Publish to Redis with strategy name in channel
    def publish_with_strategy(opportunity):
        # Log the opportunity here instead of sending to stdout
        if debug_mode:
            logger.info(f"OPPORTUNITY: {opportunity}")
        return publish_trade_opportunity(opportunity, strategy_name=strategy_name)

    redis_sync = op.map("push_trade_opportunity", final_opp, publish_with_strategy)

    # Use a noop function to prevent anything from reaching output
    def noop_formatter(item):
        # We've already logged the item if debug_mode=True
        # Just return a simple string to satisfy the sink's needs
        return "processed"

    filtered_output = op.map("noop_format", redis_sync, noop_formatter)

    # Use standard StdOutSink without any customization to avoid the unknown sink type error
    op.output("flow_output", filtered_output, StdOutSink())

    logger.info(f"Detection flow for {strategy_name} built successfully")
    return flow


# Set the build_flow method on the manager
flow_manager.build_flow = build_detection_flow


async def run_detection_flow(strategy_name="detection", debug_mode=False):
    """
    Run the detection flow for a specific strategy using the flow manager.

    Parameters:
        strategy_name: The name of the strategy to use
        debug_mode: Whether to log outputs (True) or discard them (False)
    """
    global _current_strategy_name, _current_debug_mode

    # Update the configuration
    _current_strategy_name = strategy_name
    _current_debug_mode = debug_mode

    logger.info(f"Starting detection pipeline with strategy {strategy_name} (debug={debug_mode})...")

    # Run the flow using the manager
    return await flow_manager.run_flow()


def stop_detection_flow():
    """Signal the detection flow to stop synchronously"""
    # Don't close redis_client here as it might be shared
    return flow_manager.stop_flow()


async def stop_detection_flow_async():
    """Signal the detection flow to stop asynchronously"""
    # Don't close redis_client here as it might be shared
    return await flow_manager.stop_flow_async()


# For CLI usage
if __name__ == "__main__":
    import sys

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

    # Set current configuration
    _current_strategy_name = strategy_name
    _current_debug_mode = True

    logger.info(f"Running detection flow for strategy: {strategy_name}")

    # Setup signal handlers for better shutdown
    def handle_exit_signal(sig, frame):
        logger.info(f"Received signal {sig}, initiating shutdown...")
        stop_detection_flow()
        # Give a moment for cleanup
        time.sleep(1)
        sys.exit(0)

    # Register signal handlers
    signal.signal(signal.SIGINT, handle_exit_signal)
    signal.signal(signal.SIGTERM, handle_exit_signal)

    # Run the flow using the manager
    flow_manager.run_flow_with_direct_api()
