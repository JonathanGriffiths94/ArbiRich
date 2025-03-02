import asyncio
import json
import logging

import requests
import websockets

from src.arbirich.config import EXCHANGE_CONFIGS
from src.arbirich.io.websockets.base import BaseOrderBookProcessor

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class BybitOrderBookProcessor(BaseOrderBookProcessor):
    def __init__(
        self, exchange: str, product: str, subscription_type: str, use_rest_snapshot: bool
    ):
        self.product = product
        self.cfg = EXCHANGE_CONFIGS.get(exchange)
        self.ws_url = self.cfg["ws"]["ws_url"]
        formatted_product = self.process_asset()
        self.subscribe_message = json.dumps(
            {"op": "subscribe", "args": [f"orderbook.200.{formatted_product}"]}
        )
        self.last_snapshot = None  # to store snapshot from WS
        super().__init__(exchange, product, subscription_type, use_rest_snapshot)

    async def connect(self):
        logger.info(f"Connecting to {self.ws_url}...")
        return await websockets.connect(self.ws_url)

    async def subscribe(self, websocket):
        logger.info(f"Sending subscription: {self.subscribe_message}")
        await websocket.send(self.subscribe_message)
        # Wait for subscription confirmation.
        while True:
            message = await websocket.recv()
            data = json.loads(message)
            # Bybit may send a confirmation message with an "op" field or "type" field.
            if data.get("type") in ["snapshot", "delta"]:
                # A snapshot or update implies subscription is active.
                logger.info("Subscription confirmed")
                break

    async def buffer_events(self, websocket):
        buffered_events = []
        first_event = None
        start = asyncio.get_event_loop().time()
        while True:
            message = await websocket.recv()
            data = json.loads(message)
            if data.get("type") == "snapshot":
                first_event = data
                logger.info(
                    f"Received snapshot message with timestamp: {data.get('timestamp_e6')}"
                )
                buffered_events.append(data)
                self.last_snapshot = data
                break
            elif data.get("type") == "delta":
                if first_event is None:
                    first_event = data
                    logger.info(
                        f"First delta buffered event with timestamp: {data.get('timestamp_e6')}"
                    )
                buffered_events.append(data)
                if (
                    asyncio.get_event_loop().time() - start > 1
                    or len(buffered_events) >= 5
                ):
                    break
        return buffered_events, first_event

    def fetch_snapshot(self):
        # If using snapshot mode, choose between the stored WS snapshot or fetching from REST.
        if self.last_snapshot:
            logger.info(
                f"Using WebSocket snapshot with timestamp {self.last_snapshot.get('timestamp_e6')}"
            )
            return self.last_snapshot
        if self.snapshot_url:
            logger.info("Fetching snapshot from REST API")
            response = requests.get(self.snapshot_url)
            response.raise_for_status()
            snapshot = response.json()["result"]["data"][0]
            return snapshot
        logger.error("No snapshot available")
        return None

    def get_snapshot_update_id(self, snapshot):
        # For Bybit, get the update ID from the nested "data" field.
        return snapshot.get("data", {}).get("u")

    def get_first_update_id(self, snapshot):
        # For diff updates, use the event timestamp.
        return snapshot.get("data", {}).get("u")

    def get_final_update_id(self, snapshot):
        # If Bybit’s delta updates do not provide a range, you may simply return the same timestamp.
        return snapshot.get("data", {}).get("u")

    def init_order_book(self, snapshot):
        bids = {}
        asks = {}
        data = snapshot.get("data", {})
        bid_entries = data.get("b", [])
        ask_entries = data.get("a", [])

        # Process bids from bid_entries list
        for entry in bid_entries:
            try:
                price = float(entry[0])
                qty = float(entry[1])
            except Exception as e:
                logger.error(f"Error processing bid entry {entry}: {e}")
                continue
            if qty > 0:
                bids[price] = qty

        # Process asks from ask_entries list
        for entry in ask_entries:
            try:
                price = float(entry[0])
                qty = float(entry[1])
            except Exception as e:
                logger.error(f"Error processing ask entry {entry}: {e}")
                continue
            if qty > 0:
                asks[price] = qty

        logger.info(f"Initialized order book with {len(bids)} bids and {len(asks)} asks")
        return {"bids": bids, "asks": asks}

    def process_buffered_events(self, events, snapshot_update_id):
        # In snapshot mode, we assume no extra delta events are applied.
        logger.info("No buffered events to process in snapshot mode")
        return

    async def live_updates(self, websocket):
        logger.info("Starting to process live updates...")
        async for message in websocket:
            data = json.loads(message)
            if data.get("type") != "delta":
                continue
            # For delta messages, data is under "data" (a list of order changes).
            # Normalize the event if needed.
            event = data
            # Optionally, add or compute fields if required.
            yield json.dumps(event)

    def apply_event(self, event):
        # Set the timestamp from the top level of the event
        self.order_book["timestamp"] = event.get("ts")
        # If the event has a "data" key, use it as the source for order book entries.
        if "data" in event:
            event = event["data"]

        # Now, for Bybit, order book entries are under "b" for bids and "a" for asks.
        for side_key, book_side in [("b", "bids"), ("a", "asks")]:
            if side_key in event:
                for entry in event.get(side_key, []):
                    logger.debug(f"Processing {side_key} entry: {entry}")
                    price = None
                    qty = None
                    if isinstance(entry, list):
                        if len(entry) >= 2:
                            try:
                                price = float(entry[0])
                                qty = float(entry[1])
                            except Exception as e:
                                logger.error(f"Error converting entry {entry}: {e}")
                                continue
                        else:
                            logger.warning(
                                f"Unexpected entry format (list too short): {entry}"
                            )
                            continue
                    elif isinstance(entry, dict):
                        if "p" in entry and "q" in entry:
                            price = float(entry["p"])
                            qty = float(entry["q"])
                        elif "price" in entry and "quantity" in entry:
                            price = float(entry["price"])
                            qty = float(entry["quantity"])
                        else:
                            logger.warning(f"Unknown dictionary entry format: {entry}")
                            continue
                    else:
                        logger.warning(f"Entry is neither list nor dict: {entry}")
                        continue

                    if qty == 0:
                        self.order_book[book_side].pop(price, None)
                    else:
                        self.order_book[book_side][price] = qty

    async def resubscribe(self, websocket):
        logger.info("Re-subscribing to Bybit stream for a fresh snapshot...")
        await websocket.send(self.subscribe_message)
        self.last_snapshot = None


async def run_bybit_orderbook():
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    logger.info("Starting Bybit order book processor...")
    processor = BybitOrderBookProcessor(
        exchange="bybit",
        product="BTCUSDT",
        subscription_type="snapshot",
        use_rest_snapshot=False,
    )

    try:
        count = 0
        async for order_book in processor.run():
            logger.info(f"Order book: {order_book}")

            top_bids = sorted(order_book["bids"].items(), reverse=True)[:3]
            top_asks = sorted(order_book["asks"].items())[:3]

            bid_ask_spread = (
                min(order_book["asks"].keys()) - max(order_book["bids"].keys())
                if order_book["bids"] and order_book["asks"]
                else None
            )

            logger.info(f"Top 3 bids: {top_bids}")
            logger.info(f"Top 3 asks: {top_asks}")
            logger.info(f"Bid-Ask Spread: {bid_ask_spread}")

            count += 1
            if count % 10 == 0:
                logger.info(f"Processed {count} order book updates")
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(run_bybit_orderbook())
