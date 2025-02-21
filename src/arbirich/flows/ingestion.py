import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import websockets
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition, batch_async
from bytewax.run import cli_main

from src.arbirich.market_data_service import MarketDataService

logger = logging.getLogger(__name__)

# -----------------------------
# Arbitrage threshold (e.g., 0.2% spread)
# -----------------------------
ARBITRAGE_THRESHOLD = 0.0005  # 0.2%

# -----------------------------
# Exchange configuration dictionary
# -----------------------------
EXCHANGE_CONFIG = {
    "coinbase": {
        "url": "wss://ws-feed.exchange.coinbase.com",
        "subscribe": lambda product_id: json.dumps(
            {
                "type": "subscribe",
                "product_ids": [product_id],
                "channels": ["ticker"],
            }
        ),
        "extract": lambda data: {"price": float(data["price"]), "timestamp": data["time"]}
        if "price" in data and "time" in data
        else None,
    },
    "binance": {
        "url": lambda product_id: f"wss://stream.binance.com:9443/ws/{product_id.lower()}@trade",
        "subscribe": None,
        "extract": lambda data: {"price": float(data["p"]), "timestamp": data["E"]}
        if "p" in data and "E" in data
        else None,
    },
    "bybit": {
        "url": lambda product_id: "wss://stream.bybit.com/v5/public/spot/tickers",
        "subscribe": lambda product_id: json.dumps(
            {"op": "subscribe", "args": [f"ticker.{product_id}"]}
        ),
        "extract": lambda data: {
            "price": float(data["lastPrice"]),
            "timestamp": data["ts"],
        }
        if "lastPrice" in data and "ts" in data
        else None,
    },
}


# -----------------------------
# Generic WebSocket generator using the configuration
# -----------------------------
async def _ws_agen(exchange: str, product_id: str):
    config = EXCHANGE_CONFIG[exchange]
    url = config["url"](product_id) if callable(config["url"]) else config["url"]
    while True:
        try:
            logger.info(
                f"Connecting to {exchange} WebSocket for product {product_id} at URL: {url}"
            )
            async with websockets.connect(url, max_size=10**7) as websocket:
                if config.get("subscribe"):
                    subscribe_msg = config["subscribe"](product_id)
                    logger.info(
                        f"Sending subscription message to {exchange} for product {product_id}: {subscribe_msg}"
                    )
                    await websocket.send(subscribe_msg)
                    sub_response = (
                        await websocket.recv()
                    )  # Ignore subscription confirmation
                    logger.debug(
                        f"Subscription response from {exchange} for product {product_id}: {sub_response}"
                    )
                while True:
                    msg = await websocket.recv()
                    data = json.loads(msg)
                    extracted = config["extract"](data)
                    if extracted is not None:
                        logger.debug(
                            f"Extracted data from {exchange} product {product_id}: {extracted}"
                        )
                        yield (exchange, product_id, extracted)
        except websockets.exceptions.ConnectionClosedError as e:
            logger.warning(
                f"{exchange} WebSocket Disconnected: {e}, Reconnecting in 5 seconds..."
            )
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(
                f"Unexpected {exchange} WebSocket Error: {e}, Reconnecting in 5 seconds..."
            )
            await asyncio.sleep(5)


# -----------------------------
# Partition that uses the generic generator
# -----------------------------
class ExchangePartition(StatefulSourcePartition):
    def __init__(self, exchange: str, product_id: str):
        self.exchange = exchange
        self.product_id = product_id
        logger.info(f"Creating partition for {exchange} - {product_id}")
        self._batcher = batch_async(
            _ws_agen(exchange, product_id), timedelta(seconds=0.5), 100
        )

    def next_batch(self):
        batch = next(self._batcher)
        logger.debug(f"Batch received for {self.exchange} - {self.product_id}: {batch}")
        return batch

    def snapshot(self):
        return None


# -----------------------------
# Multi-exchange source
# -----------------------------
@dataclass
class MultiExchangeSource(FixedPartitionedSource):
    """
    Creates partitions for each exchange/product pair.
    Partition keys are strings of the form "<exchange>_<product_id>".
    """

    exchanges: dict[str, List[str]]

    def list_parts(self):
        parts = []
        for exchange, products in self.exchanges.items():
            for product in products:
                parts.append(f"{exchange}_{product}")
        logger.info(f"List of partition keys: {parts}")
        return parts

    def build_part(self, step_id, for_key, _resume_state):
        try:
            exchange, product_id = for_key.split("_", 1)
        except ValueError:
            raise ValueError(
                f"Invalid partition key format: {for_key}. Expected '<exchange>_<product_id>'"
            )
        logger.info(f"Building partition for key: {for_key}")
        return ExchangePartition(exchange, product_id)


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
def key_by_asset(record: Tuple[str, str, dict]) -> Tuple[str, Tuple[str, str, dict]]:
    """
    Given (exchange, product, data) return (asset, (exchange, product, data)).
    """
    exchange, product, data = record
    asset = normalize_asset(exchange, product)
    return asset, (exchange, product, data)


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
    state.prices[exchange] = data["price"]
    state.timestamps[exchange] = data["timestamp"]
    # Only emit state if we have prices from at least two exchanges.
    if len(state.prices) >= 2:
        return state, state
    return state, None


# -----------------------------
# Detect arbitrage opportunities
# -----------------------------
def detect_arbitrage(
    asset: str, state: AssetPriceState
) -> Optional[Tuple[str, str, str, float, float, float]]:
    """
    Compare prices across exchanges for the given asset.
    Return a tuple with asset, buy_exchange, sell_exchange, buy_price, sell_price, spread
    if the spread exceeds ARBITRAGE_THRESHOLD; otherwise, return None.
    """
    if not state.prices:
        return None
    # Determine the lowest and highest price exchanges.
    buy_exchange = min(state.prices, key=lambda ex: state.prices[ex])
    sell_exchange = max(state.prices, key=lambda ex: state.prices[ex])
    buy_price = state.prices[buy_exchange]
    sell_price = state.prices[sell_exchange]
    spread = (sell_price - buy_price) / buy_price

    if spread > ARBITRAGE_THRESHOLD:
        logger.warning(
            f"Arbitrage Opportunity Detected for {asset}: Buy from {buy_exchange} at {buy_price}, Sell on {sell_exchange} at {sell_price}"
        )
        return (asset, buy_exchange, sell_exchange, buy_price, sell_price, spread)
    return None


# -----------------------------
# # Simulate trade execution (or integrate real trading logic)
# # -----------------------------
# def execute_trade(arbitrage_signal: Tuple[str, str, str, float, float, float]) -> str:
#     asset, buy_ex, sell_ex, buy_price, sell_price, spread = arbitrage_signal
#     trade_msg = f"Executing trade for {asset}: Buy from {buy_ex} at {buy_price}, Sell on {sell_ex} at {sell_price}, Spread: {spread:.4f}"
#     logger.critical(trade_msg)
#     # TODO: Insert real trade execution code via REST API.
#     return trade_msg


# Create a global or shared instance of your service (if possible)
market_data_service = MarketDataService(host="localhost", port=6379, db=0)


def push_trade_opportunity(opportunity):
    # If the opportunity is a tuple, convert it to a dictionary.
    if isinstance(opportunity, tuple):
        opportunity = {
            "asset": opportunity[0],
            "buy_exchange": opportunity[1],
            "sell_exchange": opportunity[2],
            "buy_price": opportunity[3],
            "sell_price": opportunity[4],
            "spread": opportunity[5],
        }
    # Add a timestamp if not present.
    if "timestamp" not in opportunity:
        opportunity["timestamp"] = datetime.utcnow().isoformat()
    # Add an ID if not present.
    if "id" not in opportunity:
        opportunity["id"] = f"opp:{opportunity['timestamp']}"

    try:
        market_data_service.store_trade_opportunity(opportunity)
        logger.info(f"Pushed opportunity to Redis: {json.dumps(opportunity)}")
    except Exception as e:
        logger.error(f"Error pushing opportunity to Redis: {e}")
    return opportunity


# -----------------------------
# Set up the Bytewax dataflow pipeline
# -----------------------------
def build_flow():
    flow = Dataflow("ingestion")
    source = MultiExchangeSource(
        exchanges={
            "coinbase": ["BTC-USD", "ETH-USD"],
            "binance": ["BTCUSDT", "ETHUSDT"],
            # "bybit": ["BTCUSD", "ETHUSD"],
        }
    )
    from bytewax import operators as op
    from bytewax.connectors.stdio import StdOutSink

    # Ingest raw data from exchanges.
    raw_stream = op.input("input", flow, source)
    # Map each record to a tuple keyed by asset.
    keyed_stream = op.map("key_by_asset", raw_stream, key_by_asset)
    # Use stateful_map keyed by asset to update and emit the latest state.
    asset_state_stream = op.stateful_map("asset_state", keyed_stream, update_asset_state)
    # Filter out states that are not ready (None).
    ready_state = op.filter("ready", asset_state_stream, lambda kv: kv[1] is not None)
    # Detect arbitrage on the state (the key is asset).
    arb_stream = op.map(
        "detect_arbitrage", ready_state, lambda kv: detect_arbitrage(kv[0], kv[1])
    )
    # Filter out None (i.e. no arbitrage detected).
    arb_opportunities = op.filter("arb_filter", arb_stream, lambda x: x is not None)
    # Output trade execution messages.
    redis_sync = op.map(
        "push_trade_opportunity", arb_opportunities, push_trade_opportunity
    )
    op.output("stdout", redis_sync, StdOutSink())
    return flow


async def run_ingestion():
    logger.info("Starting ingestion pipeline...")
    flow = build_flow()
    # This call will run the flow continuously.
    # If bytewax.run() is blocking, consider running it in a thread or as an executor.
    await asyncio.to_thread(cli_main, flow, workers_per_process=1)
