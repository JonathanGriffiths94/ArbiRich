import asyncio
import json
import logging

import requests
import websockets

from src.arbirich.config import EXCHANGE_CONFIGS
from src.arbirich.services.exchange_processors.base_processor import (
    BaseOrderBookProcessor,
    OrderBookGapException,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class BinanceOrderBookProcessor(BaseOrderBookProcessor):
    def __init__(
        self,
        exchange: str,
        product: str,
        subscription_type: str,
        use_rest_snapshot: bool,
    ):
        # Using snapshot mode in this example.
        cfg = EXCHANGE_CONFIGS.get(exchange)
        self.ws_url = cfg["ws"]["ws_url"]
        self.snapshot_url = cfg["ws"]["snapshot_url"]
        self.subscribe_message = cfg["ws"]["subscription_message"](product)
        super().__init__(exchange, product, subscription_type, use_rest_snapshot)

    async def connect(self):
        url = self.ws_url(self.product)
        logger.info(f"Connecting to {url}...")
        return await websockets.connect(url)

    async def subscribe(self, websocket):
        logger.info(f"Sending subscription: {self.subscribe_message}")
        await websocket.send(self.subscribe_message)
        # Binance does not send a dedicated subscription confirmation,
        # so we'll wait a moment to allow the subscription to settle.
        await asyncio.sleep(1)

    async def buffer_events(self, websocket):
        buffered_events = []
        first_event = None
        start = asyncio.get_event_loop().time()
        while True:
            message = await websocket.recv()
            data = json.loads(message)
            # Binance delta updates include the fields "U" and "u".
            # We buffer a few delta events for initial synchronization.
            if "U" not in data or "u" not in data:
                continue  # Skip non-delta messages.
            if first_event is None:
                first_event = data
                logger.info(f"First buffered event U: {first_event.get('U')}")
            buffered_events.append(data)
            if asyncio.get_event_loop().time() - start > 2 or len(buffered_events) >= 10:
                break
        return buffered_events, first_event

    def fetch_snapshot(self):
        logger.info("Fetching snapshot from REST API...")
        response = requests.get(self.snapshot_url(self.product))
        response.raise_for_status()
        snapshot = response.json()
        return snapshot

    def get_snapshot_update_id(self, snapshot):
        # Binance snapshot has a field "lastUpdateId"
        return snapshot.get("lastUpdateId")

    def get_first_update_id(self, event):
        return event.get("U")

    def get_final_update_id(self, event):
        return event.get("u")

    def init_order_book(self, snapshot):
        logger.info(f"Snapshot: {snapshot}")
        # Use 'b' and 'a' as provided in the snapshot.
        bids_data = snapshot.get("bids")
        asks_data = snapshot.get("asks")
        if bids_data is None or asks_data is None:
            raise KeyError("Snapshot does not contain expected bids or asks data.")

        bids = {float(price): float(qty) for price, qty in bids_data}
        asks = {float(price): float(qty) for price, qty in asks_data}
        logger.info(f"Initialized order book with {len(bids)} bids and {len(asks)} asks")
        return {"bids": bids, "asks": asks}

    def process_buffered_events(self, events, snapshot_update_id):
        for event in events:
            if event["u"] < snapshot_update_id:
                continue
            # If there's a gap, log and raise an exception.
            if event["U"] > snapshot_update_id + 1:
                logger.error("Gap detected in buffered events; re-subscribing...")
                raise OrderBookGapException("Gap detected in buffered events")
            self.apply_event(event)
            self.local_update_id = event["u"]

    async def live_updates(self, websocket):
        logger.info("Starting to process live updates...")
        async for message in websocket:
            yield message

    def apply_event(self, event):
        # Apply updates for both bids and asks.
        for side_key, book_side in [("bids", "bids"), ("asks", "asks")]:
            for entry in event.get(side_key, []):
                try:
                    price = float(entry[0])
                    qty = float(entry[1])
                except Exception as e:
                    logger.error(f"Error processing entry {entry}: {e}")
                    continue
                if qty == 0:
                    self.order_book[book_side].pop(price, None)
                else:
                    self.order_book[book_side][price] = qty
        # Set the timestamp from the event.
        self.order_book["timestamp"] = event.get("E", self.order_book.get("timestamp"))

    async def resubscribe(self, websocket):
        logger.info("Re-subscribing to Binance stream for a fresh snapshot...")
        await websocket.send(self.subscribe_message)


async def run_binance_orderbook():
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger.info("Starting Binance order book processor...")
    processor = BinanceOrderBookProcessor(
        product="BTCUSDT",
        exchange="binance",
        subscription_type="delta",
        use_rest_snapshot=True,
    )
    try:
        count = 0
        async for order_book in processor.run():
            logger.info(f"Order book: {order_book}")

            top_bids = sorted(order_book["bids"].items(), reverse=True)[:3]
            top_asks = sorted(order_book["asks"].items())[:3]
            spread = (
                min(order_book["asks"].keys()) - max(order_book["bids"].keys())
                if order_book["asks"] and order_book["bids"]
                else None
            )
            logger.info(f"Top 3 bids: {top_bids}")
            logger.info(f"Top 3 asks: {top_asks}")
            logger.info(f"Bid-Ask Spread: {spread}")
            count += 1
            if count % 10 == 0:
                logger.info(f"Processed {count} order book updates")
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(run_binance_orderbook())
