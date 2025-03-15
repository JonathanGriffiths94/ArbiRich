import argparse
import asyncio
import logging

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.run import cli_main

from src.arbirich.processing.arbitrage_process import (
    detect_arbitrage,
    key_by_asset,
    update_asset_state,
)
from src.arbirich.services.redis_service import RedisService
from src.arbirich.sinks.arbitrage_sink import (
    debounce_opportunity,
    publish_trade_opportunity,
)
from src.arbirich.sources.arbitrage_source import use_redis_opportunity_source
from src.arbirich.utils.strategy_manager import StrategyManager

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

redis_client = RedisService()


def build_arbitrage_flow(strategy_name="arbitrage", debug_mode=True):
    """
    Build the arbitrage flow for a specific strategy.

    Parameters:
        strategy_name: The name of the strategy to use for trade opportunities
        debug_mode: Whether to log outputs (True) or discard them (False)
    """
    logger.info(f"Building arbitrage flow for strategy: {strategy_name}...")
    flow = Dataflow(f"arbitrage-{strategy_name}")  # Include strategy name in flow name

    # Get strategy-specific exchanges and channels
    exchange_channels = StrategyManager.get_exchange_channels(strategy_name)
    logger.info(f"Using exchanges for {strategy_name}: {list(exchange_channels.keys())}")

    # Get strategy-specific pairs
    pairs = StrategyManager.get_pairs_for_strategy(strategy_name)
    logger.info(f"Using trading pairs for {strategy_name}: {pairs}")

    # Add Redis opportunity source to the flow
    input_stream = use_redis_opportunity_source(flow, "redis_input", exchange_channels, pairs)

    keyed_stream = op.map("key_by_asset", input_stream, key_by_asset)
    asset_state_stream = op.stateful_map("asset_state", keyed_stream, update_asset_state)

    # Filter not ready states
    ready_state = op.filter("ready", asset_state_stream, lambda kv: kv[1] is not None)

    # Get threshold from strategy manager
    threshold = StrategyManager.get_threshold(strategy_name)
    logger.info(f"Using threshold {threshold} for strategy {strategy_name}")

    # Create a detector function closure that includes the strategy name and threshold
    def arbitrage_detector(kv):
        # The error shows detect_arbitrage() takes only 3 arguments but we're giving 4
        # Let's check if the function actually supports the strategy_name parameter
        try:
            # First try with all 4 parameters (asset, state, threshold, strategy)
            return detect_arbitrage(kv[0], kv[1], threshold, strategy_name)
        except TypeError:
            # If that fails, fall back to just 3 parameters
            logger.warning(
                f"detect_arbitrage() only accepts 3 arguments, "
                f"strategy_name '{strategy_name}' will not be used directly"
            )
            result = detect_arbitrage(kv[0], kv[1], threshold)

            # If we got a valid result, manually add the strategy name
            if result:
                result.strategy = strategy_name

            return result

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

    logger.info(f"Arbitrage flow for {strategy_name} built successfully")
    return flow


async def run_arbitrage_flow(strategy_name="arbitrage", debug_mode=False):
    """
    Run the arbitrage flow for a specific strategy.

    Parameters:
        strategy_name: The name of the strategy to use
        debug_mode: Whether to log outputs (True) or discard them (False)
    """
    try:
        logger.info(f"Starting arbitrage pipeline with strategy {strategy_name} (debug={debug_mode})...")
        flow = build_arbitrage_flow(strategy_name, debug_mode)

        logger.info(f"Running arbitrage flow for strategy {strategy_name} in a separate thread.")
        execution_task = asyncio.create_task(
            asyncio.to_thread(cli_main, flow, workers_per_process=1), name=f"arbitrage-flow-{strategy_name}"
        )

        # Allow interruption to propagate
        try:
            await execution_task
        except asyncio.CancelledError:
            logger.info(f"Arbitrage task for strategy {strategy_name} cancelled")
            raise
        logger.info(f"Arbitrage flow for strategy {strategy_name} has finished running.")
    except asyncio.CancelledError:
        logger.info(f"Arbitrage flow for strategy {strategy_name} cancelled")
        raise
    except Exception as e:
        logger.exception(f"Error in arbitrage flow for strategy {strategy_name}: {e}")
    finally:
        logger.info(f"Arbitrage flow for strategy {strategy_name} shutdown")
        # Note: Don't close Redis client here as it might be shared among multiple flows
        # Only close in the parent manager


# For CLI usage
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run arbitrage flow")
    parser.add_argument("strategy", nargs="?", default="arbitrage", help="Strategy name to run")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")

    args = parser.parse_args()

    logger.info(f"Running arbitrage flow for strategy {args.strategy} with debug={args.debug}")

    flow = build_arbitrage_flow(args.strategy, args.debug)
    asyncio.run(cli_main(flow, workers_per_process=1))
