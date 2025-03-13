import asyncio
import hashlib
import json
import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List

import requests
import websockets
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition, batch_async
from bytewax.run import cli_main

from arbirich.redis_manager import ArbiDataService
from src.arbirich.config import EXCHANGE_CONFIG

logger = logging.getLogger(__name__)
logger.setLevel("INFO")

redis_host = os.getenv("REDIS_HOST", "localhost")
redis_client = ArbiDataService(host=redis_host, port=6379, db=0)


class ExchangePartition(StatefulSourcePartition):
    def __init__(self, product_id: str, exchange: str):
        self.exchange = exchange
        self.product_id = product_id
        self.url = (
            EXCHANGE_CONFIG[exchange]["url"](product_id)
            if callable(EXCHANGE_CONFIG[exchange]["url"])
            else EXCHANGE_CONFIG[exchange]["url"]
        )
        self.subscribe_message = (
            EXCHANGE_CONFIG[exchange]["subscribe"](product_id) if EXCHANGE_CONFIG[exchange]["subscribe"] else None
        )
        self.extract = EXCHANGE_CONFIG[exchange]["extract"]

        # Initialize local order book
        self.order_book = {"bids": defaultdict(float), "asks": defaultdict(float)}

        # Set up async generator for WebSocket data
        self._batcher = batch_async(
            self._ws_agen(),
            timedelta(milliseconds=500),  # Ingest data every 500ms
            100,  # Maximum batch size
        )
        self.processed_timestamps = set()

    async def _ws_agen(self):
        while True:
            try:
                logger.info(f"Connecting to WebSocket: {self.url}")
                async with websockets.connect(self.url) as websocket:
                    if self.subscribe_message:
                        await websocket.send(self.subscribe_message)

                    async for message in websocket:
                        data = json.loads(message)
                        logger.debug(f"Raw WebSocket message from {self.exchange}: {data}")
                        message_type = EXCHANGE_CONFIG[self.exchange]["message_type"](data)
                        self.process_message(message_type, data)

                        if self.order_book:
                            yield (self.exchange, self.product_id, self.order_book)

            except Exception as e:
                logger.error(f"WebSocket connection error: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)

    async def _binance_ws_agen(self):
        """
        Special WebSocket generator for Binance that:
          1. Buffers initial events.
          2. Fetches a REST snapshot.
          3. Applies buffered events.
          4. Then yields live updates.
        """
        buffered_events = []
        first_event = None
        while True:
            try:
                logger.info(f"Connecting to Binance WebSocket: {self.url}")
                async with websockets.connect(self.url) as websocket:
                    if self.subscribe_message:
                        await websocket.send(self.subscribe_message)
                    # Buffer events for an initial period (e.g., 1 second or a fixed number)
                    buffering = True
                    start_time = asyncio.get_event_loop().time()
                    while buffering:
                        message = await websocket.recv()
                        data = json.loads(message)
                        if data.get("e") != "depthUpdate":
                            continue  # Skip non-depth updates
                        if first_event is None:
                            first_event = data
                            logger.info(f"First depthUpdate U: {data['U']}")
                        buffered_events.append(data)
                        # Buffer for 1 second or until we have a few events
                        if asyncio.get_event_loop().time() - start_time > 1 or len(buffered_events) >= 5:
                            buffering = False

                    # Fetch the snapshot from REST.
                    snapshot_url = f"https://api.binance.com/api/v3/depth?symbol={self.product_id.upper()}&limit=5000"
                    snapshot = requests.get(snapshot_url).json()
                    snapshot_lastUpdateId = snapshot["lastUpdateId"]
                    logger.info(f"Snapshot lastUpdateId: {snapshot_lastUpdateId}")

                    # If snapshot is outdated compared to our first buffered event, restart.
                    if snapshot_lastUpdateId < first_event["U"]:
                        logger.warning(
                            "Snapshot lastUpdateId is older than first buffered event U. Restarting buffering..."
                        )
                        buffered_events.clear()
                        first_event = None
                        continue

                    # Discard buffered events with u <= snapshot_lastUpdateId.
                    valid_events = [e for e in buffered_events if e["u"] > snapshot_lastUpdateId]
                    if not valid_events:
                        logger.warning("No valid buffered events after snapshot; restarting...")
                        buffered_events.clear()
                        first_event = None
                        continue

                    # The first valid event should cover snapshot_lastUpdateId.
                    if not (valid_events[0]["U"] <= snapshot_lastUpdateId <= valid_events[0]["u"]):
                        logger.warning("Buffered events do not cover snapshot_lastUpdateId. Restarting process...")
                        buffered_events.clear()
                        first_event = None
                        continue

                    # Initialize local order book using snapshot.
                    self.order_book["bids"] = {float(price): float(qty) for price, qty, *_ in snapshot["bids"]}
                    self.order_book["asks"] = {float(price): float(qty) for price, qty, *_ in snapshot["asks"]}
                    self.order_book["timestamp"] = snapshot_lastUpdateId
                    local_update_id = snapshot_lastUpdateId
                    logger.info(f"Initialized local order book with update ID: {local_update_id}")

                    # Apply buffered events.
                    for event in valid_events:
                        # If event's u is less than local update id, skip it.
                        if event["u"] < local_update_id:
                            continue
                        # If event's U is greater than local_update_id+1, then there is a gap.
                        if event["U"] > local_update_id + 1:
                            logger.error("Gap in events detected; restarting initialization...")
                            # Restart the entire process by clearing state.
                            buffered_events.clear()
                            first_event = None
                            break
                        # Otherwise, apply the event.
                        self._handle_delta(event)
                        local_update_id = event["u"]
                        logger.info(f"Applied buffered event; new update ID: {local_update_id}")

                    # Mark snapshot initialization as complete.
                    self.snapshot_received = True
                    # Yield the initialized order book.
                    yield (self.exchange, self.product_id, self.order_book)

                    # Now process subsequent live updates.
                    async for message in websocket:
                        data = json.loads(message)
                        if data.get("e") != "depthUpdate":
                            continue
                        # Skip outdated events.
                        if data["u"] < local_update_id:
                            continue
                        if data["U"] > local_update_id + 1:
                            logger.error("Missing events; order book out of sync. Restarting initialization...")
                            self.snapshot_received = False
                            break  # Break out to restart the process.
                        self._handle_delta(data)
                        local_update_id = data["u"]
                        yield (self.exchange, self.product_id, self.order_book)
            except Exception as e:
                logger.error(f"Binance WebSocket error: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)

    def process_message(self, message_type, raw_message):
        """
        Use the exchange-specific extract function to standardize data,
        then update the order book based on whether it’s a snapshot or delta.
        """

        # Skip heartbeat messages.
        if raw_message.get("method") == "public/heartbeat":
            return

        extracted = EXCHANGE_CONFIG[self.exchange]["extract"](raw_message)
        if not extracted:
            logger.warning(f"Unable to extract data from message: {raw_message}")
            return

        update_ts = extracted.get("timestamp")
        if update_ts is None:
            logger.warning("No timestamp found in extracted data.")
            return

        # Check deduplication cache
        if update_ts in self.processed_timestamps:
            logger.debug(f"Duplicate Cryptocom event with timestamp {update_ts} skipped.")
            return

        # Add the timestamp to the cache
        self.processed_timestamps.add(update_ts)

        # Optionally, prune the set if it grows too large.
        if len(self.processed_timestamps) > 1000:
            self.processed_timestamps.remove(min(self.processed_timestamps))

        # Dispatch to the appropriate handler
        if message_type == "snapshot":
            self._handle_snapshot(extracted)
        else:
            self._handle_delta(extracted)

    def _handle_snapshot(self, data):
        """
        Initialize the local order book from snapshot data.
        This version checks for both the new keys ("bids"/"asks")
        and legacy keys ("b"/"a") to allow for different exchange formats.
        """
        if "bids" in data and "asks" in data:
            bids = data["bids"]
            asks = data["asks"]
        else:
            bids = data.get("b", [])
            asks = data.get("a", [])

        # For crypto.com, bids/asks are lists of lists: [price, quantity, ...]
        self.order_book["bids"] = {float(price): float(quantity) for price, quantity, *rest in bids}
        self.order_book["asks"] = {float(price): float(quantity) for price, quantity, *rest in asks}
        # Save the timestamp (key name coming from the extract function)
        self.order_book["timestamp"] = data.get("timestamp")
        logger.info(f"Order book snapshot initialized for {self.product_id}")

    def _handle_delta(self, data):
        """
        Update the local order book from delta update data.
        Adapt this to check for the correct keys.
        """
        # If the data contains changes in the same structure as snapshot:
        if "bids" in data and "asks" in data:
            bids = data["bids"]
            asks = data["asks"]
            # If the delta update contains full arrays rather than a 'changes' key,
            # you may want to update the order book similarly to a snapshot,
            # or apply differences. This example assumes full updates.
            self.order_book["bids"] = {float(price): float(quantity) for price, quantity, *rest in bids}
            self.order_book["asks"] = {float(price): float(quantity) for price, quantity, *rest in asks}
        else:
            # Fallback to legacy key names if needed
            for side in ["b", "a"]:
                book_side = "bids" if side == "b" else "asks"
                for price, quantity in data.get(side, []):
                    price = float(price)
                    quantity = float(quantity)
                    if quantity == 0:
                        self.order_book[book_side].pop(price, None)
                    else:
                        self.order_book[book_side][price] = quantity
        # Optionally update the timestamp if provided
        if "timestamp" in data:
            self.order_book["timestamp"] = data["timestamp"]
        logger.debug(f"Order book updated for {self.product_id}")

    def next_batch(self):
        """
        Fetches the next batch of WebSocket messages.
        """
        try:
            batch = next(self._batcher)
            logger.debug(f"Fetched batch for {self.exchange}-{self.product_id}: {batch}")
            return batch
        except Exception as e:
            logger.error(f"Error fetching next batch for {self.exchange}-{self.product_id}: {e}")
            return []

    def snapshot(self):
        """
        Bytewax requires this method for stateful sources, but for WebSockets,
        we return None as no state restoration is needed.
        """
        return None


@dataclass
class MultiExchangeSource(FixedPartitionedSource):
    exchanges: dict[str, List[str]]

    def list_parts(self):
        """
        Returns partition keys based on exchange names and trading pairs.
        Logs a warning if no valid partitions are found.
        """
        parts = [f"{product}_{exchange}" for exchange, products in self.exchanges.items() for product in products]

        if not parts:
            logger.error("No partitions were created! Check EXCHANGE_CONFIG or the provided exchange-product mapping.")

        logger.info(f"List of partitions: {parts}")
        return parts

    def build_part(self, step_id, for_key, _resume_state):
        """
        Builds a partition for the exchange-product pair.
        Catches and logs invalid partitions to prevent Bytewax panics.
        """
        try:
            exchange, product_id = for_key.split("_", 1)
            logger.info(f"Building partition for key: {for_key}")
            return ExchangePartition(product_id, exchange)
        except Exception as e:
            logger.error(f"Invalid partition key: {for_key}, Error: {e}")
            return None  # Prevent panic by returning None if an error occurs


def normalize_timestamp(timestamp):
    """Convert timestamps from different exchanges to a uniform UNIX timestamp in seconds."""
    if isinstance(timestamp, str):  # Handle ISO 8601 timestamps (e.g., Coinbase)
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        return dt.timestamp()
    elif isinstance(timestamp, int) or isinstance(timestamp, float):  # Milliseconds or seconds
        return timestamp / 1000 if timestamp > 10**10 else timestamp
    else:
        raise ValueError(f"Invalid timestamp format: {timestamp}")


def process_order_book(order_book_update):
    """
    Process order book updates by extracting bids and asks,
    normalizing the timestamp, and formatting data for storage.
    """
    exchange, product_id, order_book = order_book_update
    logger.debug(f"processing: {order_book}")
    if not order_book or "bids" not in order_book or "asks" not in order_book or "timestamp" not in order_book:
        logger.warning(f"Invalid order book update received from {exchange} for {product_id}: {order_book}")
        return None

    # Normalize timestamp and use it in the formatted data.
    normalized_timestamp = normalize_timestamp(order_book["timestamp"])

    formatted_data = {
        "exchange": exchange,
        "symbol": product_id,
        "bids": [{"price": float(k), "quantity": float(v)} for k, v in order_book["bids"].items()],
        "asks": [{"price": float(k), "quantity": float(v)} for k, v in order_book["asks"].items()],
        "timestamp": normalized_timestamp,
    }

    return formatted_data


# Global cache for the last order book update per (exchange, symbol)
LAST_ORDER_BOOK = {}


def get_order_book_hash(order_book):
    """
    Compute a hash for the order book's bids and asks.
    We ignore the timestamp if you only want to deduplicate based on market data.
    """
    # Create a canonical string representation using sorted keys.
    data = json.dumps(
        {
            "bids": order_book["bids"],
            "asks": order_book["asks"],
        },
        sort_keys=True,
    )
    return hashlib.md5(data.encode("utf-8")).hexdigest()


def store_order_book(order_book):
    """
    Processed order book data is stored in Redis and published.
    Before doing so, we check if this update is a duplicate.
    """
    logger.debug(f"Storage: {order_book}")
    if not order_book:
        return None

    key = (order_book["exchange"], order_book["symbol"])
    new_hash = get_order_book_hash(order_book)
    last_hash = LAST_ORDER_BOOK.get(key)

    if new_hash == last_hash:
        logger.debug(f"Duplicate update for {key}, skipping publishing.")
        return order_book

    # Update the cache with the new hash
    LAST_ORDER_BOOK[key] = new_hash

    # Proceed to store and publish the order book update.
    redis_client.store_order_book(
        order_book["exchange"],
        order_book["symbol"],
        order_book["bids"],
        order_book["asks"],
        order_book["timestamp"],
    )
    redis_client.publish_order_book(
        order_book["exchange"],
        order_book["symbol"],
        order_book["bids"],
        order_book["asks"],
        order_book["timestamp"],
    )

    logger.info(f"Published order book update: {order_book['exchange']} {order_book['symbol']}")
    return order_book


def build_flow():
    """
    Build the Bytewax dataflow:
      1. Input from MultiExchangeSource.
      2. Transformation to process the raw order book.
      3. Storing (and publishing) the processed order book.
    """
    flow = Dataflow("ingestion")

    source = MultiExchangeSource(
        exchanges={
            "cryptocom": ["ADA_USDT"],
            # "binance": ["BTCUSDT"],
            # "bybit": ["BTCUSDT"],
            # "kucoin": ["BTCUSDT"]
        }
    )

    from bytewax import operators as op
    from bytewax.connectors.stdio import StdOutSink

    raw_stream = op.input("extract", flow, source)
    processed = op.map("transform", raw_stream, process_order_book)
    redis_sync = op.map("load", processed, store_order_book)
    op.output("stdout", redis_sync, StdOutSink())
    return flow


async def run_ingestion_flow():
    try:
        logger.info("Starting ingestion pipeline...")
        flow = build_flow()

        logger.info("Running cli_main in a separate thread.")
        execution_task = asyncio.create_task(asyncio.to_thread(cli_main, flow, workers_per_process=1))

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
