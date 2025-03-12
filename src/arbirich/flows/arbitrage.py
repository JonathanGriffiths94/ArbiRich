import asyncio
import logging

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.run import cli_main

from arbirich.sinks.trade_opportunity import (
    debounce_opportunity,
    publish_trade_opportunity,
)
from src.arbirich.config import REDIS_CONFIG, STRATEGIES
from src.arbirich.processing.process_arbitrage import (
    detect_arbitrage,
    key_by_asset,
    update_asset_state,
)
from src.arbirich.redis_manager import MarketDataService
from src.arbirich.sources.redis_price_partition import RedisPriceSource
from src.arbirich.utils.helpers import build_exchanges_dict

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

redis_client = MarketDataService(
    host=REDIS_CONFIG["host"], port=REDIS_CONFIG["port"], db=REDIS_CONFIG["db"]
)


def build_arbitrage_flow():
    """
    Build the arbitrage flow.
    This flow uses a custom Redis source that yields trade opportunities,
    applies the execute_trade operator, and outputs the result.
    """
    logger.info("Building arbitrage flow...")
    flow = Dataflow("arbitrage")

    exchanges = build_exchanges_dict()
    exchange_channels = {exchange: "order_book" for exchange in exchanges.keys()}

    source = RedisPriceSource(exchange_channels)
    stream = op.input("redis_input", flow, source)
    logger.debug("Input stream created from RedisPriceSource.")

    keyed_stream = op.map("key_by_asset", stream, key_by_asset)
    asset_state_stream = op.stateful_map("asset_state", keyed_stream, update_asset_state)

    # Filter not ready states
    ready_state = op.filter("ready", asset_state_stream, lambda kv: kv[1] is not None)

    # Detect arbitrage on the state (the key is asset).
    arb_stream = op.map(
        "detect_arbitrage",
        ready_state,
        lambda kv: detect_arbitrage(kv[0], kv[1], STRATEGIES["arbitrage"]["threshold"]),
    )

    # Filter out None opportunities from detect_arbitrage
    arb_opportunities = op.filter("arb_filter", arb_stream, lambda x: x is not None)

    debounced_opportunities = op.map(
        "debounce_opportunity",
        arb_opportunities,
        lambda x: debounce_opportunity(redis_client, x),
    )

    # Filter out None values from debouncer
    final_opp = op.filter(
        "final_filter", debounced_opportunities, lambda x: x is not None
    )

    redis_sync = op.map("push_trade_opportunity", final_opp, publish_trade_opportunity)

    op.output("stdout", redis_sync, StdOutSink())
    logger.info("Output operator attached (StdOutSink).")
    return flow


async def run_arbitrage_flow():
    try:
        logger.info("Starting arbitrage pipeline...")
        flow = build_arbitrage_flow()

        logger.info("Running cli_main in a separate thread.")
        execution_task = asyncio.create_task(
            asyncio.to_thread(cli_main, flow, workers_per_process=1)
        )

        # Allow interruption to propagate
        try:
            await execution_task
        except asyncio.CancelledError:
            logger.info("arbitrage task cancelled")
            raise
        logger.info("cli_main has finished running.")
    except asyncio.CancelledError:
        logger.info("Arbitrage task cancelled")
        raise
    finally:
        logger.info("Arbitrage flow shutdown")
        redis_client.close()


# Expose the flow for CLI usage.
flow = build_arbitrage_flow()

if __name__ == "__main__":
    asyncio.run(cli_main(flow, workers_per_process=1))
