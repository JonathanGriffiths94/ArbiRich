import asyncio
import json
import logging
import time

import websockets

from src.arbirich.config.config import EXCHANGE_CONFIGS
from src.arbirich.services.exchange_processors.base_processor import BaseOrderBookProcessor
from src.arbirich.services.exchange_processors.registry import register

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
        self.product = product
        self.cfg = EXCHANGE_CONFIGS.get(exchange)
        self.ws_url = self.cfg["ws"]["ws_url"]
        self.formatted_product = self._format_product(product)
        self.subscribe_message = json.dumps(
            {
                "id": 1,
                "method": "subscribe",
                "params": {"channels": [f"book.{self.formatted_product}.10"]},
                "book_subscription_type": "SNAPSHOT_AND_UPDATE",
                "book_update_frequency": 10,
            }
        )
        self.last_snapshot = None  # Store the last received snapshot
        super().__init__(exchange, product, subscription_type, use_rest_snapshot)

    def _format_product(self, product):
        """Format the product symbol for Crypto.com (e.g., BTC-USDT -> BTC_USDT)"""
        return product.replace("-", "_")

    def process_asset(self):
        """Format the asset for use in API calls."""
        return self.formatted_product

    async def connect(self):
        """Connect to the Crypto.com WebSocket endpoint."""
        logger.info(f"Connecting to {self.ws_url}...")
        return await websockets.connect(self.ws_url)

    async def subscribe(self, websocket):
        """Subscribe to the order book channel."""
        logger.info(f"Sending subscription: {self.subscribe_message}")
        await websocket.send(self.subscribe_message)

        # Wait for subscription confirmation
        for _ in range(5):  # Try a few times
            message = await websocket.recv()
            data = json.loads(message)
            if "id" in data and data.get("method") == "subscribe":
                logger.info("Subscription confirmed")
                return

            # If we get data immediately, that's also good
            if "result" in data and "data" in data["result"]:
                logger.info("Received data during subscription, confirming subscription")
                return

        logger.warning("No subscription confirmation received, continuing anyway")

    async def buffer_events(self, websocket):
        """Buffer initial events to get a snapshot."""
        buffered_events = []
        first_event = None
        start = time.time()

        while time.time() - start < 10:  # Wait up to 10 seconds
            message = await websocket.recv()
            data = json.loads(message)

            # If we get a message with result.data, it's probably a snapshot
            if "result" in data and "data" in data["result"] and data["result"]["data"]:
                event_data = data["result"]["data"][0]
                logger.info(f"Received snapshot with update ID: {event_data.get('u')}")

                # Keep track of this snapshot
                self.last_snapshot = event_data

                if first_event is None:
                    first_event = event_data

                buffered_events.append(event_data)
                break  # We have a snapshot, no need to wait further

            # Skip subscription confirmations
            if data.get("method") == "subscribe":
                continue

            # If we've collected enough events, stop
            if len(buffered_events) >= 3:
                break

        logger.info(f"Buffered {len(buffered_events)} events")
        return buffered_events, first_event

    def fetch_snapshot(self):
        """Use the last snapshot we received from WebSocket."""
        if self.last_snapshot:
            logger.info(f"Using WebSocket snapshot with update ID: {self.last_snapshot.get('u')}")
            return self.last_snapshot
        logger.warning("No snapshot available")
        return None

    def get_snapshot_update_id(self, snapshot):
        """Get the sequence number from the snapshot."""
        return snapshot.get("u", 0)

    def get_first_update_id(self, event):
        """Get the previous sequence number from the event."""
        # Previous update ID is pu, but if not present, use u-1
        return event.get("pu", event.get("u", 0) - 1)

    def get_final_update_id(self, event):
        """Get the final sequence number from the event."""
        return event.get("u", 0)

    def init_order_book(self, snapshot):
        """Initialize the order book from a snapshot."""
        bids = {}
        asks = {}

        # If we don't have a snapshot, return an empty order book
        if not snapshot:
            logger.warning("No snapshot to initialize order book")
            return {"bids": bids, "asks": asks, "timestamp": time.time()}

        # Process bids from the snapshot
        for entry in snapshot.get("bids", []):
            if len(entry) >= 2:
                try:
                    price = float(entry[0])
                    qty = float(entry[1])
                    if qty > 0:
                        bids[price] = qty
                except (ValueError, TypeError) as e:
                    logger.error(f"Error processing bid: {entry} - {e}")

        # Process asks from the snapshot
        for entry in snapshot.get("asks", []):
            if len(entry) >= 2:
                try:
                    price = float(entry[0])
                    qty = float(entry[1])
                    if qty > 0:
                        asks[price] = qty
                except (ValueError, TypeError) as e:
                    logger.error(f"Error processing ask: {entry} - {e}")

        logger.info(f"Initialized order book with {len(bids)} bids and {len(asks)} asks")
        return {"bids": bids, "asks": asks, "timestamp": snapshot.get("t", time.time())}

    def process_buffered_events(self, events, snapshot_update_id):
        """Process buffered events - nothing to do for snapshot mode."""
        logger.debug("No buffered events to process in snapshot mode")
        pass  # No additional processing needed

    async def live_updates(self, websocket):
        """Process live updates from the WebSocket."""
        logger.info("Starting to process live updates...")
        while True:
            message = await websocket.recv()
            data = json.loads(message)

            # Check if this is a data message
            if "result" in data and "data" in data["result"] and data["result"]["data"]:
                event = data["result"]["data"][0]

                # Make sure it has update IDs
                if "u" in event:
                    event["pu"] = self.get_first_update_id(event)
                    yield json.dumps(event)  # Return the event as JSON string

            # Skip subscription confirmations and other non-data messages
            elif data.get("method") == "subscribe":
                continue

            # Debug log any other messages
            else:
                logger.debug(f"Skipping message: {message[:100]}...")

    def apply_event(self, event):
        """Apply an update event to the order book."""
        # Process bids
        for side_key, book_side in [("bids", "bids"), ("asks", "asks")]:
            if side_key in event:
                for entry in event.get(side_key, []):
                    if len(entry) >= 2:
                        try:
                            price = float(entry[0])
                            qty = float(entry[1])

                            if qty > 0:
                                self.order_book[book_side][price] = qty
                            else:
                                # Remove price level if quantity is 0
                                self.order_book[book_side].pop(price, None)
                        except (ValueError, TypeError) as e:
                            logger.error(f"Error processing {side_key} entry: {entry} - {e}")

        # Update timestamp
        if "t" in event:
            # Convert from milliseconds to seconds if needed
            timestamp = event["t"]
            if isinstance(timestamp, (int, float)) and timestamp > 1600000000000:
                timestamp = timestamp / 1000.0
            self.order_book["timestamp"] = timestamp

    async def resubscribe(self, websocket):
        """Resubscribe to get a fresh snapshot."""
        logger.info("Resubscribing to Crypto.com stream...")
        await self.subscribe(websocket)
        self.last_snapshot = None  # Clear the last snapshot


async def run_cryptocom_orderbook():
    """Test the processor directly."""
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger.info("Starting Crypto.com order book processor...")
    processor = CryptocomOrderBookProcessor(
        exchange="cryptocom",
        product="BTC-USDT",
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
    asyncio.run(run_cryptocom_orderbook())
