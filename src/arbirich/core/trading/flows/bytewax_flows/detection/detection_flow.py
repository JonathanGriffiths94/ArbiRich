import logging
import signal
import sys
import time
from decimal import Decimal
from typing import Optional

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

from arbirich.core.state.system_state import mark_system_shutdown
from src.arbirich.core.trading.flows.bytewax_flows.common.redis_utils import reset_all_redis_connections
from src.arbirich.core.trading.flows.bytewax_flows.common.shutdown_utils import mark_force_kill, setup_force_exit_timer
from src.arbirich.core.trading.flows.bytewax_flows.detection.detection_process import (
    detect_arbitrage,
    extract_asset_key,
    format_opportunity,
    update_asset_state,
)
from src.arbirich.core.trading.flows.bytewax_flows.detection.detection_sink import publish_trade_opportunity
from src.arbirich.core.trading.flows.bytewax_flows.detection.detection_source import (
    RedisOrderBookSource,
    mark_detection_force_kill,
)
from src.arbirich.core.trading.flows.flow_manager import BytewaxFlowManager
from src.arbirich.models.models import OrderBookState, TradeOpportunity

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

flow_manager = BytewaxFlowManager.get_or_create("detection")

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
    # Set logging level based on debug mode
    if debug_mode:
        logger.setLevel(logging.DEBUG)
        logger.info(f"🔍 DEBUG MODE ENABLED for detection flow: {strategy_name}")
    else:
        logger.setLevel(logging.INFO)
        logger.info(f"ℹ️ Normal logging mode for detection flow: {strategy_name}")

    logger.info(f"🚀 Building detection flow for strategy: {strategy_name}, threshold: {threshold}")
    flow = Dataflow("arbitrage_detection")

    from src.arbirich.config.config import EXCHANGES

    exchange_channels = {exchange: "order_book" for exchange in EXCHANGES.keys()}
    logger.info(f"Configured exchanges: {list(exchange_channels.keys())}")

    from src.arbirich.config.config import STRATEGIES

    pairs = STRATEGIES.get(strategy_name, {}).get("pairs", [])
    logger.info(f"Monitoring pairs for {strategy_name}: {pairs if pairs else 'All pairs'}")

    source = RedisOrderBookSource(
        exchange_channels=exchange_channels,
        pairs=pairs,
        strategy_name=strategy_name,
        stop_event=flow_manager.stop_event,
    )

    inp = op_input("order_books", flow, source)

    # Inspect input order books for debugging
    def log_order_book(step_id, item):
        """Log order book details for debugging."""

        exchange, order_book = item
        symbol = order_book.symbol if hasattr(order_book, "symbol") else "unknown"

        bid_count = len(order_book.bids) if hasattr(order_book, "bids") else 0
        ask_count = len(order_book.asks) if hasattr(order_book, "asks") else 0

        # Get best bid using get_best_bid() method or by finding max bid price
        best_bid = order_book.get_best_bid() if hasattr(order_book, "get_best_bid") else None
        # Get best ask using get_best_ask() method or by finding min ask price
        best_ask = order_book.get_best_ask() if hasattr(order_book, "get_best_ask") else None

        # Extract top prices from the best bid and ask or use dictionary directly
        top_bid_price = (
            Decimal(str(best_bid.price))
            if best_bid
            else (Decimal(str(max(order_book.bids.keys()))) if order_book.bids else Decimal("0"))
        )
        top_ask_price = (
            Decimal(str(best_ask.price))
            if best_ask
            else (Decimal(str(min(order_book.asks.keys()))) if order_book.asks else Decimal("0"))
        )

        # Calculate spread if both bid and ask exist
        spread = None
        if top_bid_price > 0 and top_ask_price > 0:
            spread = (top_ask_price - top_bid_price) / top_bid_price

        logger.info(f"📥 RECEIVED ORDER BOOK: {exchange}:{symbol} - {bid_count} bids, {ask_count} asks")

        # Only log detailed prices if we have both bids and asks
        if bid_count > 0 and ask_count > 0:
            spread_str = f", Spread: {spread:.6%}" if spread is not None else ""
            logger.info(f"💰 Top Bid: {top_bid_price}, Top Ask: {top_ask_price}{spread_str}")

    inspected = inspect("inspect_input", inp, log_order_book)

    # Group by asset - use the extract_asset_key function that returns just a string
    keyed = key_on("key_by_asset", inspected, extract_asset_key)

    # Periodically log state for debugging
    def debug_state_and_update(state, item):
        start_time = time.time()

        # In a key-partitioned stateful_map, state is already specific to this key
        # No need to do state.get(key, None) as each key has its own state
        if state is None:
            logger.info("🆕 Creating new OrderBookState for this key")
            state = OrderBookState()

        # item is the data for this key
        data = item

        # Extract the key from the item for logging - remove fallback to UNKNOWN and raise error
        try:
            if isinstance(data, tuple) and len(data) == 2:
                # If data is a tuple, extract from the second element (OrderBookUpdate)
                _, order_book = data
                if not hasattr(order_book, "symbol") or not order_book.symbol:
                    raise ValueError(f"Missing symbol attribute in order book: {order_book}")
                asset = order_book.symbol
            else:
                # Otherwise try to get it directly from the data object
                if not hasattr(data, "symbol") or not data.symbol:
                    raise ValueError(f"Missing symbol attribute in data: {data}")
                asset = data.symbol

            logger.info(f"🔑 Using asset key for state update: {asset}")
        except Exception as e:
            logger.error(f"❌ Failed to extract asset key: {e}")
            raise  # Re-raise to halt processing

        # Update the state with this single data item - Pass the asset explicitly
        _, updated_state = update_asset_state(asset, [data], state)

        # Periodically log comprehensive state information
        current_time = int(time.time())
        if current_time % 10 == 0:  # Every ~10 seconds
            exchange_count = 0
            if asset in updated_state.symbols:
                exchange_count = len(updated_state.symbols[asset])
                exchanges = list(updated_state.symbols[asset].keys())
                logger.info(f"🔍 STATE DEBUG - Asset {asset} has {exchange_count} exchanges: {exchanges}")

                # Check data quality for each exchange
                for exchange in exchanges:
                    book = updated_state.symbols[asset][exchange]
                    bid_count = len(book.bids) if hasattr(book, "bids") else 0
                    ask_count = len(book.asks) if hasattr(book, "asks") else 0
                    logger.info(f"  • {exchange}: {bid_count} bids, {ask_count} asks")
            else:
                logger.warning(f"⚠️ Asset {asset} not found in state symbols")

        process_time = time.time() - start_time
        logger.debug(f"⏱️ State update for {asset} completed in {process_time:.6f}s")

        # Return both the updated state and the data as a tuple
        # This is required for stateful_map - it needs (new_state, output)
        # IMPORTANT: Return the correct asset key, not UNKNOWN
        return updated_state, (asset, updated_state)

    reduced = stateful_map(
        step_id="update_state",
        up=keyed,
        mapper=debug_state_and_update,
    )

    # Enhanced detect_opps to better track exchanges compared - fix asset extraction
    def detect_opps(input_data) -> Optional[TradeOpportunity]:
        # Add debugging to understand the input structure
        logger.info(f"detect_opps received input type: {type(input_data)}")

        # Handle different possible input formats to extract asset and state
        if isinstance(input_data, tuple):
            logger.info(f"Input is tuple with {len(input_data)} elements")

            if len(input_data) == 2:
                # Basic tuple unpacking
                asset, state = input_data
                logger.info(f"Unpacked asset={asset}, state type={type(state)}")

                # Handle case where state is also a tuple (double wrapping)
                if isinstance(state, tuple) and len(state) == 2:
                    logger.info("State is also a tuple - unwrapping a level")
                    # Unwrap one more level - taking the second element which should be the state
                    inner_asset, state = state
                    # No special handling for "UNKNOWN" - always use the inner asset if it exists
                    asset = inner_asset
                    logger.info(f"Unwrapped again to asset={asset}, state type={type(state)}")
            else:
                error_msg = f"Unexpected tuple length: {len(input_data)}"
                logger.error(error_msg)
                raise ValueError(error_msg)
        else:
            error_msg = f"Input is not a tuple: {type(input_data)}"
            logger.error(error_msg)
            raise TypeError(error_msg)

        # Verify state is valid
        if not hasattr(state, "symbols"):
            logger.error(f"State {type(state)} has no symbols attribute")
            return None

        # Find the asset key in the state
        if not state.symbols:
            logger.warning("❌ DETECT SKIP: Empty state symbols")
            return None

        # Check if we have data for this asset
        if asset not in state.symbols:
            logger.info(f"❌ DETECT SKIP: Asset {asset} not found in state symbols")
            return None

        # Check if we have enough exchanges
        exchanges = list(state.symbols[asset].keys())
        if len(exchanges) < 2:
            logger.info(f"❌ DETECT SKIP: Not enough exchanges for {asset}, have {len(exchanges)}, need at least 2")
            return None

        # Log the exchange comparisons we'll be making
        logger.info(f"🔍 DETECTING: Asset {asset} across {len(exchanges)} exchanges: {exchanges}")

        # Check each exchange has valid data
        valid_exchanges = []
        for exchange in exchanges:
            book = state.symbols[asset][exchange]
            bid_count = len(book.bids) if hasattr(book, "bids") else 0
            ask_count = len(book.asks) if hasattr(book, "asks") else 0

            if bid_count > 0 and ask_count > 0:
                valid_exchanges.append(exchange)
                logger.info(f"✅ Valid data: {exchange} for {asset} has {bid_count} bids, {ask_count} asks")
            else:
                logger.info(f"❌ Invalid data: {exchange} for {asset} has {bid_count} bids, {ask_count} asks")

        if len(valid_exchanges) < 2:
            logger.info(
                f"❌ DETECT SKIP: Not enough valid exchanges for {asset}, have {len(valid_exchanges)}/{len(exchanges)}"
            )
            return None

        logger.info(f"🚀 RUNNING DETECTION: Asset {asset} with {len(valid_exchanges)} valid exchanges")
        start_time = time.time()

        # Run detection
        opportunity = detect_arbitrage(asset, state, threshold, strategy_name)

        process_time = time.time() - start_time
        logger.info(f"⏱️ Detection for {asset} completed in {process_time:.6f}s")

        if opportunity:
            # Ensure opportunity is a TradeOpportunity Pydantic model
            if not isinstance(opportunity, TradeOpportunity):
                logger.warning(f"⚠️ Opportunity is not a TradeOpportunity model, type: {type(opportunity)}")

                # If it's a dictionary, convert it to a TradeOpportunity model
                if isinstance(opportunity, dict):
                    try:
                        # If strategy is missing, add it
                        if "strategy" not in opportunity:
                            opportunity["strategy"] = strategy_name

                        # Convert to Pydantic model
                        opportunity = TradeOpportunity(**opportunity)
                        logger.info("✅ Successfully converted opportunity dict to TradeOpportunity model")
                    except Exception as e:
                        logger.error(f"❌ Failed to convert opportunity dict to TradeOpportunity model: {e}")
                        return None
                else:
                    logger.error(f"❌ Opportunity is not a dict or TradeOpportunity model: {type(opportunity)}")
                    return None

            logger.info(f"💰 FOUND OPPORTUNITY for {asset}!")
            logger.info(
                f"  • Buy: {opportunity.buy_exchange}, Sell: {opportunity.sell_exchange}, Spread: {opportunity.spread:.4%}"
            )
        else:
            logger.info(f"❌ No opportunity found for {asset}")

        return opportunity

    opportunities = filter_map("detect_opportunities", reduced, detect_opps)

    # Format opportunities
    formatted = filter_map("format_opportunities", opportunities, format_opportunity)

    # Publish opportunities to Redis
    def publish_logic(state, item):
        start_time = time.time()

        # Will raise error instead of returning None if it fails
        publish_trade_opportunity(item)

        process_time = time.time() - start_time

        if debug_mode:
            logger.debug(f"Opportunity publish completed in {process_time:.6f}s")

        return state, None  # Return the unchanged state and no output

    stateful_map(
        step_id="publish_opportunities",
        up=formatted,
        mapper=publish_logic,
    )

    logger.info(f"Detection flow built successfully for strategy: {strategy_name}")
    return flow


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

    # Set the logger level based on debug mode - directly rather than relying on another function
    if debug_mode:
        logger.setLevel(logging.DEBUG)
        # Enable DEBUG for all bytewax loggers
        logging.getLogger("bytewax").setLevel(logging.DEBUG)
        logging.getLogger("src.arbirich").setLevel(logging.DEBUG)

    logger.info(f"🚀 Starting detection pipeline with strategy {strategy_name} (debug={debug_mode})...")
    logger.debug("Debug logging is enabled - you should see this message if debug mode is working")
    start_time = time.time()

    # Run the flow using the manager
    result = await flow_manager.run_flow()

    runtime = time.time() - start_time
    logger.info(f"Detection flow for {strategy_name} ran for {runtime:.2f}s")

    return result


def stop_detection_flow():
    """Signal the detection flow to stop synchronously with enhanced force-kill"""
    # First mark the detection force-kill flag to ensure quick exit of all partitions
    mark_detection_force_kill()

    # Also mark in the common registry
    mark_force_kill("detection")

    # Set system shutdown flag to notify all components
    mark_system_shutdown(True)

    # Reset Redis connections
    reset_all_redis_connections()
    logger.info("Reset all Redis pools for detection flow")

    # Now use the flow manager to stop the flow
    logger.info("Stopping detection flow via flow manager")

    # Add a failsafe timer to force exit if normal shutdown gets stuck
    setup_force_exit_timer(5.0)

    # Don't catch exceptions - let them propagate up
    result = flow_manager.stop_flow()
    return result


async def stop_detection_flow_async():
    """Signal the detection flow to stop asynchronously"""
    # First mark for force kill
    mark_detection_force_kill()
    mark_force_kill("detection")

    # Then stop via flow manager
    return await flow_manager.stop_flow_async()


def process_output_opportunity(opportunity):
    """Process output opportunity and ensure it's published."""
    import sys

    from src.arbirich.core.trading.flows.bytewax_flows.detection.detection_sink import publish_trade_opportunity
    from src.arbirich.models.models import TradeOpportunity

    print(f"DETECTION FLOW - PROCESSING OUTPUT OPPORTUNITY: {opportunity}")
    sys.stdout.flush()  # Force print to appear

    if opportunity:
        # Ensure it's a proper TradeOpportunity model
        if not isinstance(opportunity, TradeOpportunity):
            print(f"WARNING: Opportunity is not a TradeOpportunity model: {type(opportunity)}")
            # Try to convert if it's a dict
            if isinstance(opportunity, dict):
                try:
                    opportunity = TradeOpportunity(**opportunity)
                    print("Successfully converted opportunity dict to TradeOpportunity model")
                except Exception as e:
                    print(f"ERROR: Failed to convert to TradeOpportunity: {e}")
                    return None
            else:
                print(f"ERROR: Cannot process opportunity of type {type(opportunity)}")
                return None

        # Now publish the validated opportunity
        result = publish_trade_opportunity(opportunity)
        print(f"OPPORTUNITY PUBLISH RESULT: {result}")
        sys.stdout.flush()  # Force print to appear
        return result
    return None


if __name__ == "__main__":
    # Change the default logging level to DEBUG
    logging.basicConfig(
        level=logging.DEBUG,  # Change from INFO to DEBUG
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

    # Update logger level directly
    logger.setLevel(logging.DEBUG)

    logger.debug("Debug logging is ENABLED for detection flow")

    logger.info(f"Running detection flow for strategy: {strategy_name}")

    def handle_exit_signal(sig, frame):
        logger.info(f"Received signal {sig}, initiating shutdown...")

        try:
            # First check if we are the runner thread to avoid self-joining
            import threading

            current_thread_id = threading.get_ident()

            # Set system shutdown flag to notify all components
            mark_system_shutdown(True)

            # Apply more aggressive shutdown
            mark_detection_force_kill()
            mark_force_kill("detection")

            # Just set the stop event first
            flow_manager.stop_event.set()

            # Give a moment for resources to clean up
            time.sleep(0.5)

            # Only try to join the thread if we're not the same thread
            if hasattr(flow_manager, "runner_thread") and flow_manager.runner_thread:
                if flow_manager.runner_thread.ident != current_thread_id:
                    logger.info("Waiting for flow runner thread to complete...")
                    flow_manager.runner_thread.join(timeout=2.0)
                else:
                    logger.info("Running in flow thread, skipping thread join")

            # In case the thread didn't exit, set up failsafe
            setup_force_exit_timer(3.0)
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
            # Force exit as a last resort
            import os

            os._exit(1)

    # Register signal handlers with better error handling
    try:
        signal.signal(signal.SIGINT, handle_exit_signal)
        signal.signal(signal.SIGTERM, handle_exit_signal)
    except Exception as e:
        logger.error(f"Failed to register signal handlers: {e}")

    # Update the build_flow method to include the strategy_name parameter with forced debug mode
    flow_manager.build_flow = lambda: build_detection_flow(
        strategy_name=strategy_name,
        debug_mode=True,  # Force debug mode to True
    )

    # Run the flow using the manager
    flow_manager.run_flow_with_direct_api()
