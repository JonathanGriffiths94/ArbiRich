import asyncio
import json
import logging
import os
import time
from datetime import timedelta
from typing import List, Tuple

import websockets
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition, batch_async
from bytewax.run import cli_main

from src.arbirich.market_data_service import MarketDataService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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

# Initialize Redis client
redis_host = os.getenv("REDIS_HOST", "localhost")
redis_client = MarketDataService(host=redis_host, port=6379, db=0)


# -----------------------------
# WebSocket Generator (_ws_agen)
# -----------------------------
async def _ws_agen(exchange: str, product_id: str):
    """
    Connects to the exchange's WebSocket, subscribes (if needed), and yields a tuple:
    (exchange, product_id, extracted_data)
    """
    config = EXCHANGE_CONFIG[exchange]
    url = config["url"](product_id) if callable(config["url"]) else config["url"]
    while True:
        try:
            logger.info(f"Connecting to {exchange} WebSocket for {product_id} at {url}")
            async with websockets.connect(url, max_size=10**7) as websocket:
                if config.get("subscribe"):
                    subscribe_msg = config["subscribe"](product_id)
                    logger.info(
                        f"Subscribing to {exchange} for {product_id}: {subscribe_msg}"
                    )
                    await websocket.send(subscribe_msg)
                while True:
                    msg = await websocket.recv()
                    data = json.loads(msg)
                    extracted = config["extract"](data)
                    if extracted is not None:
                        logger.debug(f"Extracted {exchange} {product_id}: {extracted}")
                        yield (exchange, product_id, extracted)
        except websockets.exceptions.ConnectionClosedError as e:
            logger.warning(
                f"{exchange} WebSocket Disconnected: {e}. Reconnecting in 5s..."
            )
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected error on {exchange}: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)


# -----------------------------
# Custom Partition using _ws_agen
# -----------------------------
class ExchangePartition(StatefulSourcePartition):
    def __init__(self, exchange: str, product_id: str):
        self.exchange = exchange
        self.product_id = product_id
        self._batcher = batch_async(
            _ws_agen(exchange, product_id), timedelta(seconds=0.5), 100
        )

    def next_batch(self) -> list:
        batch = next(self._batcher)
        logger.debug(f"Batch for {self.exchange}-{self.product_id}: {batch}")
        return batch

    def snapshot(self):
        return None


# -----------------------------
# Custom Source: MultiExchangeSource
# -----------------------------
class MultiExchangeSource(FixedPartitionedSource):
    def __init__(self, exchanges: dict[str, List[str]]):
        self.exchanges = exchanges

    def list_parts(self) -> list:
        parts = []
        for exchange, products in self.exchanges.items():
            for product in products:
                parts.append(f"{exchange}_{product}")
        logger.info(f"Partition keys: {parts}")
        return parts

    def build_part(self, step_id, for_key, _resume_state):
        try:
            exchange, product = for_key.split("_", 1)
        except ValueError:
            raise ValueError(f"Invalid partition key format: {for_key}")
        logger.info(f"Building partition for key: {for_key}")
        return ExchangePartition(exchange, product)


# -----------------------------
# Ingestion Operator: Call publish_price for each record
# -----------------------------
def publish_price_operator(record):
    """
    Publishes price to Redis and ensures the latest price is always available.
    """
    exchange, product, data = record
    price = data.get("price")

    if price is not None:
        key = f"price:{exchange}:{product}"
        value = json.dumps({"price": price, "timestamp": time.time()})

        # Store the latest price with a TTL (optional)
        # redis_client.setex(key, 60, value)  # Store with 60s TTL

        # # Publish price updates for real-time listeners
        # redis_client.publish("prices", value)
        redis_client.publish_price(exchange, product, price)

        # (Optional) Maintain a history of last N prices
        # await redis_client.lpush(f"history:{exchange}:{product}", value)
        # await redis_client.ltrim(f"history:{exchange}:{product}", 0, 99)  # Keep last 100

        return f"📌 Synced {exchange} {product}: {price}"

    return None


# -----------------------------
# Build the Ingestion Flow
# -----------------------------
def build_ingestion_flow():
    """
    Bytewax ingestion flow.
    Reads live prices from WebSockets via a custom source and publishes them to Redis.
    """
    logger.info("Building ingestion flow...")
    flow = Dataflow("ingestion")
    # Define which exchange/product pairs to subscribe to.
    source = MultiExchangeSource(
        exchanges={
            "coinbase": ["BTC-USD", "ETH-USD"],
            "binance": ["BTCUSDT", "ETHUSDT"],
            # "bybit": ["BTCUSD", "ETHUSD"],
        }
    )
    from bytewax import operators as op

    stream = op.input("ws_source", flow, source)
    published = op.map("publish_price", stream, publish_price_operator)
    # For debugging, output results to stdout.
    from bytewax.connectors.stdio import StdOutSink

    op.output("stdout", published, StdOutSink())
    logger.info("Ingestion flow built successfully.")
    return flow


# -----------------------------
# Run the Ingestion Flow
# -----------------------------
async def run_ingestion_flow():
    try:
        logger.info("Starting ingestion pipeline...")
        flow = build_ingestion_flow()
        logger.info("Running cli_main in a separate thread.")
        execution_task = asyncio.create_task(
            asyncio.to_thread(cli_main, flow, workers_per_process=1)
        )
        try:
            await execution_task
        except asyncio.CancelledError:
            logger.info("Execution task cancelled")
            raise
        logger.info("cli_main has finished running.")
    except asyncio.CancelledError:
        logger.info("Execution task cancelled")
        raise
    finally:
        logger.info("Ingestion flow shutdown")
        await redis_client.close()


if __name__ == "__main__":
    asyncio.run(run_ingestion_flow())
