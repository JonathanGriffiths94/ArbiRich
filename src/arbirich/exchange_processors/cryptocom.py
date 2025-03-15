import asyncio
import json
import logging

import requests
import websockets

from src.arbirich.config import EXCHANGE_CONFIGS
from src.arbirich.exchange_processors.processor_factory import register
from src.arbirich.io.websockets.base import BaseOrderBookProcessor

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@register("cryptocom")
class CryptocomOrderBookProcessor(BaseOrderBookProcessor):
    def __init__(
        self,
        exchange: str,
        product: str,
        subscription_type: str,
        use_rest_snapshot: bool,
    ):
        # Using snapshot mode in this example.
        self.product = product
        self.cfg = EXCHANGE_CONFIGS.get(exchange)
        self.ws_url = self.cfg["ws"]["ws_url"]
        formatted_product = self.process_asset()

        self.subscribe_message = json.dumps(
            {
                "id": 1,
                "method": "subscribe",
                "params": {"channels": [f"book.{formatted_product}.10"]},
                "book_subscription_type": "SNAPSHOT_AND_UPDATE",
                "book_update_frequency": 10,
            }
        )
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
            if "id" in data and data.get("method") == "subscribe":
                logger.info("Subscription confirmed")
                break

    async def buffer_events(self, websocket):
        buffered_events = []
        first_event = None
        start = asyncio.get_event_loop().time()
        while True:
            message = await websocket.recv()
            data = json.loads(message)
            logger.debug(data.get("channel"))
            # If the message type is snapshot, use it as the full snapshot.
            if data["result"].get("channel") == "book":
                first_event = data["result"]["data"][0]
                logger.debug(f"First event: {first_event}")
                logger.debug(f"Received snapshot message with update ID: {first_event.get('u')}")
                buffered_events.append(first_event)
                # Optionally save this snapshot.
                self.last_snapshot = first_event
                break
            elif data["result"].get("channel") == "book.update":
                if first_event is None:
                    first_event = data["result"]["data"][0]
                    logger.debug(f"First delta buffered event U: {first_event.get('u')}")
                buffered_events.append(data["data"])
                if asyncio.get_event_loop().time() - start > 1 or len(buffered_events) >= 5:
                    break
        return buffered_events, first_event

    def fetch_snapshot(self):
        # In snapshot mode, we may choose to use the WebSocket snapshot already received.
        if self.last_snapshot:
            logger.debug(f"Using WebSocket snapshot with timestamp {self.last_snapshot.get('t')}")
            return self.last_snapshot
        logger.debug("Fetching snapshot from REST API")
        response = requests.get(self.snapshot_url)
        response.raise_for_status()
        snapshot = response.json()["result"]["data"][0]
        return snapshot

    # def fetch_snapshot(self):
    #     if self.last_snapshot:
    #         logger.info(f"Using WebSocket snapshot with timestamp {self.last_snapshot.get('t')}")
    #         try:
    #             snapshot = CryptocomOrderBookSnapshot(**self.last_snapshot)
    #         except Exception as e:
    #             logger.error(f"Snapshot validation error: {e}")
    #             raise e
    #         return snapshot
    #     logger.info("Fetching snapshot from REST API")
    #     response = requests.get(self.snapshot_url)
    #     response.raise_for_status()
    #     snapshot_data = response.json()["result"]["data"][0]
    #     try:
    #         snapshot = CryptocomOrderBookSnapshot(**snapshot_data)
    #     except Exception as e:
    #         logger.error(f"Snapshot validation error from REST: {e}")
    #         raise e
    #     return snapshot

    # def get_snapshot_update_id(self, snapshot):
    #     return snapshot.get("u")

    # def get_first_update_id(self, event):
    #     return event.get("pu", self.get_snapshot_update_id(event) - 1)

    def get_snapshot_update_id(self, snapshot):
        return snapshot.get("u")

    def get_first_update_id(self, snapshot):
        return snapshot.get("pu", snapshot.get("u", 0) - 1)

    def get_final_update_id(self, snapshot):
        return snapshot.get("u")

    def init_order_book(self, snapshot):
        bids = {}
        asks = {}
        # Process bids
        for price, qty, *_ in snapshot.get("bids", []):
            try:
                price = float(price)
                qty = float(qty)
            except Exception as e:
                logger.error(f"Error processing bid entry {price, qty}: {e}")
                continue
            if qty > 0:
                bids[price] = qty
        # Process asks
        for price, qty, *_ in snapshot.get("asks", []):
            try:
                price = float(price)
                qty = float(qty)
            except Exception as e:
                logger.error(f"Error processing ask entry {price, qty}: {e}")
                continue
            if qty > 0:
                asks[price] = qty
        logger.debug(f"Initialized order book with {len(bids)} bids and {len(asks)} asks")
        return {"bids": bids, "asks": asks}

    def process_buffered_events(self, events, snapshot_update_id):
        # For snapshot mode, we assume no additional buffered delta events are needed.
        logger.debug("No buffered events to process in snapshot mode")
        return

    async def live_updates(self, websocket):
        async for message in websocket:
            data = json.loads(message)
            if "result" not in data or "data" not in data["result"]:
                continue
            event = data["result"]["data"][0]
            ## Check this!!!
            event["pu"] = self.get_first_update_id(event)

            logger.debug(f"Received update with t: {event.get('t')}, pu: {event.get('pu')}, u: {event.get('u')}")
            yield json.dumps(event)

    # async def live_updates(self, websocket):
    #     async for message in websocket:
    #         data = json.loads(message)
    #         if "result" not in data or "data" not in data["result"]:
    #             continue
    #         event_data = data["result"]["data"][0]
    #         logger.info(f"Event_data: {event_data}")
    #         try:
    #             event = CryptocomOrderBookSnapshot(
    #                 bids=event_data["bids"],  # [[price, quantity, lvl], [...] ]
    #                 asks=event_data["asks"],
    #                 u=event_data["u"],
    #                 t=event_data["t"],
    #                 pu=0,
    #             )
    #         except Exception as e:
    #             logger.error(f"Event validation error: {e}")
    #             continue
    #         logger.debug(
    #             f"Received update with t: {event.t}, pu: {event.pu}, u: {event.u}"
    #         )
    #         # Yield either the model or its JSON/dict representation as needed:
    #         yield event.model_dump_json()  # or yield event.dict()

    def apply_event(self, event):
        # Process both bids ("bids") and asks ("asks").
        for side_key, book_side in [("bids", "bids"), ("asks", "asks")]:
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
                            logger.warning(f"Unexpected entry format (list too short): {entry}")
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
        # Set the timestamp from the event.
        self.order_book["timestamp"] = event.get("t", self.order_book.get("timestamp"))

    async def resubscribe(self, websocket):
        logger.info("Re-subscribing to Crypto.com stream for a fresh snapshot...")
        await websocket.send(self.subscribe_message)
        self.last_snapshot = None


async def run_crypto_orderbook():
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger.info("Starting Crypto.com order book processor...")
    processor = CryptocomOrderBookProcessor(
        exchange="cryptocom",
        product="BTC_USDT",
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
                max(order_book["bids"].keys()) - min(order_book["asks"].keys())
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
    asyncio.run(run_crypto_orderbook())
