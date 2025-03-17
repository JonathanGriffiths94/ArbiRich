import asyncio
import json
import logging
import random
import time
from typing import Any, Dict, Generator

from src.arbirich.factories.processor_factory import register
from src.arbirich.io.exchange_processors.base_processor import BaseOrderBookProcessor

logger = logging.getLogger(__name__)


@register("default")
class DefaultOrderBookProcessor(BaseOrderBookProcessor):
    """
    Default processor that generates synthetic order book data.
    This is useful for testing when no exchange-specific processor is available.
    """

    async def connect(self):
        """Simulate connecting to an exchange."""
        logger.info(f"Simulating connection to {self.exchange} for {self.product}")

        # This needs to return an async context manager for the websocket
        class DummyWebsocket:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

            async def send(self, data):
                logger.debug(f"Sending data: {data}")
                pass

            async def recv(self):
                await asyncio.sleep(0.5)  # Simulate network delay
                return json.dumps(self._generate_dummy_data())

            def _generate_dummy_data(self):
                # Generate random order book data
                return {
                    "type": "orderbook",
                    "timestamp": time.time(),
                    "bids": {str(40000 - i * 10): str(random.uniform(0.1, 1.0)) for i in range(10)},
                    "asks": {str(40000 + i * 10): str(random.uniform(0.1, 1.0)) for i in range(10)},
                }

        return DummyWebsocket()

    async def subscribe(self, websocket):
        """Send subscription request."""
        logger.info(f"Simulating subscription for {self.product}")
        await websocket.send(json.dumps({"type": "subscribe", "product_ids": [self.product], "channels": ["level2"]}))

    async def buffer_events(self, websocket):
        """Buffer initial events."""
        logger.info(f"Buffering initial events for {self.product}")
        events = []
        # Get a few initial events
        for _ in range(3):
            message = await websocket.recv()
            event = json.loads(message)
            events.append(event)

        # Return buffered events and the first event
        return events, events[0] if events else None

    def fetch_snapshot(self):
        """Fetch order book snapshot."""
        logger.info(f"Fetching snapshot for {self.product}")
        # Generate a dummy snapshot
        return {
            "type": "snapshot",
            "product_id": self.product,
            "timestamp": time.time(),
            "update_id": 1,  # Start with update_id 1
            "bids": {str(40000 - i * 10): str(random.uniform(0.1, 1.0)) for i in range(20)},
            "asks": {str(40000 + i * 10): str(random.uniform(0.1, 1.0)) for i in range(20)},
        }

    def get_snapshot_update_id(self, snapshot):
        """Get the snapshot update ID."""
        return snapshot.get("update_id", 0)

    def get_first_update_id(self, event):
        """Get the first update ID from an event."""
        return event.get("U", None)

    def get_final_update_id(self, event):
        """Get the final update ID from an event."""
        return event.get("u", None)

    def init_order_book(self, snapshot):
        """Initialize order book from snapshot."""
        return {
            "bids": snapshot.get("bids", {}),
            "asks": snapshot.get("asks", {}),
            "timestamp": snapshot.get("timestamp", time.time()),
        }

    def process_buffered_events(self, events, snapshot_update_id):
        """Process buffered events."""
        logger.info(f"Processing {len(events)} buffered events")
        # For simplicity in the dummy implementation, we don't actually process the events
        pass

    async def live_updates(self, websocket):
        """Yield live update messages."""
        logger.info(f"Starting live updates for {self.product}")
        while True:
            try:
                message = await websocket.recv()
                yield message
            except Exception as e:
                logger.error(f"Error receiving message: {e}")
                break

    def apply_event(self, event):
        """Apply event to order book."""
        # Update bids and asks
        if "bids" in event:
            for price, qty in event["bids"].items():
                if float(qty) > 0:
                    self.order_book["bids"][price] = qty
                else:
                    # Remove price level if quantity is 0
                    self.order_book["bids"].pop(price, None)

        if "asks" in event:
            for price, qty in event["asks"].items():
                if float(qty) > 0:
                    self.order_book["asks"][price] = qty
                else:
                    # Remove price level if quantity is 0
                    self.order_book["asks"].pop(price, None)

        # Update timestamp
        self.order_book["timestamp"] = event.get("timestamp", time.time())

    async def resubscribe(self, websocket):
        """Resubscribe to the feed."""
        logger.info(f"Resubscribing for {self.product}")
        await self.subscribe(websocket)

    async def run(self) -> Generator[Dict[str, Any], None, None]:
        """
        Run the processor to get order book updates.

        Yields:
            Dict with dummy order book data
        """
        logger.warning(f"Using dummy data for {self.exchange}:{self.product}")

        while True:
            # Generate dummy order book
            order_book = {
                "exchange": self.exchange,
                "symbol": self.product,
                "timestamp": time.time(),
                "bids": {},  # Empty bids dictionary
                "asks": {},  # Empty asks dictionary
            }

            yield order_book

            # Wait before next update
            await asyncio.sleep(5)
