import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.run import cli_main

from src.arbirich.market_data_service import MarketDataService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Instantiate the Redis service.
redis_client = MarketDataService(
    host=os.getenv("REDIS_HOST", "localhost"), port=6379, db=0
)

ARBITRAGE_THRESHOLD = 0.00005  # 0.05%


class RedisPriceUpdatePartition(StatefulSourcePartition):
    """
    Listens for price updates in a Redis Pub/Sub channel and streams them.
    """

    def __init__(self, channel: str):
        # Initialize the generator from the Redis subscription.
        logger.debug("Initializing RedisOpportunityPartition.")
        self.gen = redis_client.subscribe_to_prices(channel)
        self._running = True
        self._last_activity = time.time()
        self.channel = channel

    def next_batch(self):
        """
        Reads messages from Redis Pub/Sub and yields them as batches.
        """
        if time.time() - self._last_activity > 60:  # 1 minute timeout
            self._last_activity = time.time()
            # Ensure redis_client.is_healthy() is implemented appropriately
            if not redis_client.is_healthy():
                logger.warning("Redis connection appears unhealthy")
                return []

        # If partition is marked as not running, return empty list.
        if not self._running:
            logger.info("Partition marked as not running, stopping")
            return []

        try:
            # Try to get one message (non-blocking thanks to our generator design).
            result = next(self.gen, None)
            if result:
                self._last_activity = time.time()
                logger.debug(f"next_batch obtained message: {result}")
                return [result]
            return []
        # except StopIteration:
        #     logger.info("Generator exhausted")
        #     self._running = False
        #     return []
        except StopIteration:
            logger.warning("Redis generator stopped, restarting subscription...")
            self.gen = redis_client.subscribe_to_prices(self.channel)
            return []
        except Exception as e:
            logger.error(f"Error in next_batch: {e}")
            return []

    def snapshot(self) -> None:
        # Return None for now (or implement snapshot logic if needed)
        logger.debug("Snapshot requested for RedisOpportunityPartition (returning None).")
        return None


@dataclass
class RedisPriceSource(FixedPartitionedSource):
    """
    Creates partitions for each exchange's price update channel.
    """

    exchange_channels: dict[str, str]  # Mapping: { "exchange": "redis_channel" }

    def list_parts(self):
        """Returns partition keys based on exchange names."""
        parts = list(self.exchange_channels.keys())
        logger.info(f"List of partitions (Redis channels): {parts}")
        return parts

    def build_part(self, step_id, for_key, _resume_state):
        """
        Builds a Redis listener partition for each exchange.
        """
        try:
            redis_channel = self.exchange_channels[for_key]
        except KeyError:
            raise ValueError(f"Invalid exchange key: {for_key}")
        logger.info(f"Building Redis partition for exchange: {for_key}")
        return RedisPriceUpdatePartition(redis_channel)


# -----------------------------
# Normalization function: extract asset from product
# -----------------------------
def normalize_asset(exchange: str, product: str) -> str:
    # For Coinbase, product is typically "BTC-USD", so take the first part.
    if exchange == "coinbase":
        return product.split("-")[0]
    # For Binance and Bybit, assume the asset code is the first 3 (or 4) letters.
    # A more robust implementation might use regex or a mapping.
    if product.startswith("BTC"):
        return "BTC"
    elif product.startswith("ETH"):
        return "ETH"
    else:
        return product


# -----------------------------
# Data structures for asset state and arbitrage detection
# -----------------------------
@dataclass
class AssetPriceState:
    prices: Dict[str, float] = field(default_factory=dict)  # exchange -> price
    timestamps: Dict[str, str] = field(default_factory=dict)  # exchange -> timestamp


# -----------------------------
# Map input data to a keyed tuple by asset
# -----------------------------
def key_by_asset(record: dict) -> Tuple[str, Tuple[str, str, dict]]:
    """
    Extracts the asset symbol and returns a tuple (asset, (exchange, product, data)).
    """
    try:
        exchange = record["exchange"]
        product = record["symbol"]  # "symbol" is the correct key, not "product"
        asset = normalize_asset(exchange, product)
        return asset, (exchange, product, record)
    except KeyError as e:
        logger.error(f"Missing key in key_by_asset: {e}, record: {record}")
        return None  # Ensure no invalid data is passed further


# -----------------------------
# Update state for each asset
# -----------------------------
def update_asset_state(
    state: Optional[AssetPriceState], new_data: Tuple[str, str, dict]
) -> Tuple[AssetPriceState, Optional[AssetPriceState]]:
    """
    new_data: (exchange, product, data)
    """
    if state is None:
        state = AssetPriceState()

    exchange, _, data = new_data

    try:
        state.prices[exchange] = data["price"]
        state.timestamps[exchange] = data["timestamp"]
    except KeyError as e:
        logger.error(f"Missing data key in update_asset_state: {e}, data: {data}")
        return state, None  # Return state, but don't emit invalid data

    # Only emit state if we have prices from at least two exchanges.
    if len(state.prices) >= 2:
        return state, state
    return state, None


# -----------------------------
# Detect arbitrage opportunities
# -----------------------------
def detect_arbitrage(asset: str, state: AssetPriceState, threshold: float):
    if not state or not state.prices:
        logger.warning(f"No prices available for {asset}, skipping arbitrage detection.")
        return None

    try:
        buy_exchange = min(state.prices, key=lambda ex: state.prices[ex])
        sell_exchange = max(state.prices, key=lambda ex: state.prices[ex])
        buy_price = state.prices[buy_exchange]
        sell_price = state.prices[sell_exchange]
        spread = (sell_price - buy_price) / buy_price

        if spread > threshold:
            logger.info(
                f"Arbitrage Opportunity: {asset} - Buy at {buy_exchange}, Sell at {sell_exchange}"
            )
            return (asset, buy_exchange, sell_exchange, buy_price, sell_price, spread)
    except Exception as e:
        logger.error(f"Error in detect_arbitrage for {asset}: {e}", exc_info=True)
        return None


def debounce_opportunity(redis_client, opportunity, expiry_seconds=30):
    """
    Avoid duplicate arbitrage opportunities by checking Redis.

    Only emit an opportunity if:
    - It has a significant price change (0.1% difference)
    - OR enough time (expiry_seconds) has passed since the last similar opportunity.

    Uses Redis for storing the last opportunity per asset.
    """
    try:
        asset, buy_ex, sell_ex, buy_price, sell_price, spread = opportunity
        key = f"last_opp:{asset}:{buy_ex}:{sell_ex}"
        now = time.time()

        # Retrieve last stored opportunity from Redis
        last_opp_data = redis_client.redis_client.get(key)

        if last_opp_data:
            last_opp_entry = json.loads(last_opp_data)  # Removed .decode("utf-8")
            last_time = last_opp_entry.get("timestamp", 0)
            last_buy_price = last_opp_entry.get("buy_price", 0)
            last_sell_price = last_opp_entry.get("sell_price", 0)

            # Skip if opportunity is too similar AND it's within the debounce period
            if (
                abs(buy_price - last_buy_price) / (last_buy_price + 1e-8) < 0.001
                and abs(sell_price - last_sell_price) / (last_sell_price + 1e-8) < 0.001
                and (now - last_time) < expiry_seconds
            ):
                return None  # Skip duplicate opportunity

        # Store the new opportunity in Redis
        redis_client.redis_client.setex(
            key,
            expiry_seconds,
            json.dumps(
                {"timestamp": now, "buy_price": buy_price, "sell_price": sell_price}
            ),
        )
        return opportunity  # Return if it's a valid opportunity

    except Exception as e:
        logger.error(f"Error in debounce_opportunity: {e}", exc_info=True)
        return None


def publish_trade_opportunity(opportunity: dict) -> dict:
    try:
        redis_client.publish_trade_opportunity(opportunity)
        logger.info(f"Pushed arbitrage opportunity: {json.dumps(opportunity)}")
        return opportunity
    except Exception as e:
        logger.error(f"Error pushing opportunity: {e}")
        return None


def build_arbitrage_flow():
    """
    Build the arbitrage flow.
    This flow uses a custom Redis source that yields trade opportunities,
    applies the execute_trade operator, and outputs the result.
    """
    logger.info("Building arbitrage flow...")
    flow = Dataflow("arbitrage")

    # Use the custom RedisPriceSource.
    exchange_channels = {
        "coinbase": "prices",
        "binance": "prices",
    }
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
        lambda kv: detect_arbitrage(kv[0], kv[1], ARBITRAGE_THRESHOLD),
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
