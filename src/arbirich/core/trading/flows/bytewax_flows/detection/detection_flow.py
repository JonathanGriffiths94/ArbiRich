import logging
import signal
import sys
import time

from bytewax.dataflow import Dataflow
from bytewax.operators import (
    filter_map,
    inspect,
    key_on,
    stateful_map,
)
from bytewax.operators import (
    input as op_input,
)

from src.arbirich.core.trading.flows.bytewax_flows.detection.detection_process import (
    detect_arbitrage,
    format_opportunity,
    key_by_asset,
    update_asset_state,
)
from src.arbirich.core.trading.flows.bytewax_flows.detection.detection_sink import publish_trade_opportunity
from src.arbirich.core.trading.flows.bytewax_flows.detection.detection_source import RedisOrderBookSource
from src.arbirich.core.trading.flows.flow_manager import BytewaxFlowManager

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Global state dictionary to persist state between flow runs
_global_asset_states = {}

# Create the flow manager for detection
flow_manager = BytewaxFlowManager.get_or_create("detection")

# Store the current configuration
_current_strategy_name = None
_current_debug_mode = False


def build_detection_flow(strategy_name: str, debug_mode: bool = False, threshold: float = 0.001) -> Dataflow:
    """
    Create a dataflow for detecting arbitrage opportunities.

    Args:
        strategy_name: Name of the strategy to use
        debug_mode: Whether to enable debug logging
        threshold: Minimum profit threshold

    Returns:
        Bytewax dataflow
    """
    flow = Dataflow("arbitrage_detection")

    # Get exchange channels from configuration
    # Default to a dictionary mapping exchange names to default "order_book" channel
    from src.arbirich.config.config import EXCHANGES

    exchange_channels = {exchange: "order_book" for exchange in EXCHANGES.keys()}

    # Get trading pairs to monitor from strategy configuration
    # Default to an empty list which means monitor all pairs
    from src.arbirich.config.config import STRATEGIES

    pairs = STRATEGIES.get(strategy_name, {}).get("pairs", [])

    # Create the detection source with required parameters
    # Pass the flow manager's stop event to allow clean shutdown
    source = RedisOrderBookSource(exchange_channels=exchange_channels, pairs=pairs, stop_event=flow_manager.stop_event)

    # Input from Redis with properly configured source
    inp = op_input("order_books", flow, source)

    # Log input data
    inspected = inspect("inspect_input", inp, lambda x: logger.debug(f"Received order book: {x}"))

    # Group by asset
    keyed = key_on("key_by_asset", inspected, key_by_asset)

    # Update state for each asset with state persistence
    def update_with_global_state(state, item):
        if state is None:
            state = {}
        key, data = item
        existing_state = state.get(key, None)
        asset, new_state = update_asset_state(key, [data], existing_state)
        state[key] = new_state
        return (asset, new_state)

    reduced = stateful_map(
        "update_state",  # step_id
        keyed,  # input stream
        update_with_global_state,  # mapper function
    )

    # Detect opportunities
    def detect_opps(item):
        asset, state = item
        if asset not in state.symbols or len(state.symbols[asset]) < 2:
            logger.info(f"Not enough exchanges for {asset}, skipping detection")
            return None

        opportunity = detect_arbitrage(asset, state, threshold, strategy_name)
        if opportunity:
            if isinstance(opportunity, dict) and "strategy" not in opportunity:
                opportunity["strategy"] = strategy_name
            logger.info(f"Found opportunity for {asset}!")
        return opportunity

    opportunities = filter_map("detect_opportunities", reduced, detect_opps)

    # Format opportunities
    formatted = filter_map("format_opportunities", opportunities, format_opportunity)

    # Publish opportunities to Redis
    def publish_logic(state, item):
        publish_trade_opportunity(item)
        return state, None  # Return the unchanged state and no output

    stateful_map(
        "publish_opportunities",  # step_id
        formatted,  # input stream
        publish_logic,  # logic function
    )

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
    return flow_manager.stop_flow()


async def stop_detection_flow_async():
    """Signal the detection flow to stop asynchronously"""
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
