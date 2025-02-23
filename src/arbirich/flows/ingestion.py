import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import websockets
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition, batch_async
from bytewax.run import cli_main

from arbirich.market_data_service import MarketDataService

logger = logging.getLogger(__name__)


redis_host = os.getenv("REDIS_HOST", "localhost")
redis_client = MarketDataService(host=redis_host, port=6379, db=0)

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


def push_price(price_update: Tuple[str, str, Dict[str, float]]):
    exchange, product_id, extracted = price_update

    # Ensure data is correctly structured
    price = {
        "exchange": exchange,
        "symbol": product_id,
        "price": extracted.get("price"),
        "timestamp": extracted.get("timestamp"),
    }

    try:
        # Store and publish price using Redis
        redis_client.publish_price(
            exchange, product_id, price
        )  # Pass `price`, not `price_update`
        logger.info(
            f"Pushed price to Redis: {json.dumps(price)}"
        )  # Log correct structure
    except Exception as e:
        logger.error(f"Error pushing price to Redis: {e}")

    return price


def publish_price(price_update):
    exchange, product_id, extracted = price_update

    price = {
        "exchange": exchange,
        "symbol": product_id,
        "price": extracted.get("price"),
        "timestamp": extracted.get("timestamp"),
    }

    try:
        # Debounce before publishing
        if redis_client.debounce_price(exchange, product_id, price):
            redis_client.publish_price(
                exchange, product_id, price
            )  # Publish only if it's updated
            logger.info(f"Pushed price to Redis: {json.dumps(price)}")
    except Exception as e:
        logger.error(f"Error pushing price to Redis: {e}")
    return price_update


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

    redis_sync = op.map("push_price", raw_stream, push_price)
    op.output("stdout", redis_sync, StdOutSink())
    return flow


async def run_ingestion_flow():
    try:
        logger.info("Starting ingestion pipeline...")
        flow = build_flow()

        logger.info("Running cli_main in a separate thread.")
        execution_task = asyncio.create_task(
            asyncio.to_thread(cli_main, flow, workers_per_process=1)
        )

        # Allow interruption to propagate
        try:
            await execution_task
        except asyncio.CancelledError:
            logger.info("Ingestion task cancelled")
            raise
        logger.info("cli_main has finished running.")
    except asyncio.CancelledError:
        logger.info("Execution task cancelled")
        raise
    finally:
        logger.info("Ingestion flow shutdown")
        redis_client.close()
