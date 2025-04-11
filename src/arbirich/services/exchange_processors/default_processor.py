import asyncio
import json
import logging
import time

from src.arbirich.services.exchange_processors.base_processor import BaseOrderBookProcessor
from src.arbirich.services.exchange_processors.registry import register

logger = logging.getLogger(__name__)


@register("default")
class DefaultOrderBookProcessor(BaseOrderBookProcessor):
    """Default implementation for exchanges without a specific processor."""

    def __init__(
        self,
        exchange: str,
        product: str,
        subscription_type: str = "snapshot",
        use_rest_snapshot: bool = False,
    ):
        super().__init__(exchange, product, subscription_type, use_rest_snapshot)
        self.exchange = exchange
        self.product = product
        logger.info(f"Using DefaultOrderBookProcessor for {exchange} with product {product}")

    async def connect(self):
        """
        Create a simple dummy context manager since we're not actually connecting
        to a real WebSocket in this default implementation.
        """
        # Store exchange and product in variables to capture in the closure
        exchange_name = self.exchange
        product_name = self.product

        class DummyWebsocket:
            def __init__(self):
                # Store the exchange and product from the outer scope
                self.exchange = exchange_name
                self.product = product_name

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass

            async def recv(self):
                """Simulate receiving a message with an empty order book"""
                await asyncio.sleep(1)  # Wait to avoid high CPU usage
                dummy_data = {
                    "exchange": self.exchange,
                    "symbol": self.product,
                    "bids": {"100.0": 1.0, "99.0": 2.0},  # Add some dummy bids for testing
                    "asks": {"101.0": 1.0, "102.0": 2.0},  # Add some dummy asks for testing
                    "timestamp": time.time(),
                }
                return json.dumps(dummy_data)

            async def send(self, message):
                """Dummy send method"""
                pass

        return DummyWebsocket()

    async def subscribe(self, websocket):
        """Send dummy subscription message"""
        logger.info(f"[DEFAULT] Subscribing to {self.product} on {self.exchange}")
        # No actual subscription needed
        pass

    async def buffer_events(self, websocket):
        """Return empty buffer in default implementation"""
        logger.info(f"[DEFAULT] Buffering events for {self.product} on {self.exchange}")
        return [], {}

    def fetch_snapshot(self):
        """Return empty snapshot"""
        logger.info(f"[DEFAULT] Fetching snapshot for {self.product} on {self.exchange}")
        return {"id": 1, "bids": {}, "asks": {}, "timestamp": time.time()}

    def get_snapshot_update_id(self, snapshot):
        """Get update ID from snapshot"""
        return snapshot.get("id", 1)

    def get_first_update_id(self, snapshot):
        """Get first update ID from snapshot"""
        return snapshot.get("id", 1)

    def get_final_update_id(self, event):
        """Get final update ID from event"""
        return event.get("id", 1)

    def init_order_book(self, snapshot):
        """Initialize order book from snapshot"""
        return {
            "bids": snapshot.get("bids", {}),
            "asks": snapshot.get("asks", {}),
            "timestamp": snapshot.get("timestamp", time.time()),
        }

    def process_buffered_events(self, events, snapshot_update_id):
        """Process buffered events - no-op in default implementation"""
        pass

    async def live_updates(self, websocket):
        """Yield dummy updates"""
        while True:
            message = await websocket.recv()
            yield message

    def apply_event(self, event):
        """Apply event to order book - no-op in default implementation"""
        pass

    async def resubscribe(self, websocket):
        """Resubscribe - no-op in default implementation"""
        pass


# Register common exchanges with the default processor
for exchange_name in ["bybit", "cryptocom"]:
    try:

        @register(exchange_name)
        class ExchangeProcessor(DefaultOrderBookProcessor):
            """Processor for specific exchange using default implementation."""

            pass

        logger.info(f"Registered default processor for {exchange_name}")
    except Exception as e:
        logger.error(f"Failed to register default processor for {exchange_name}: {e}")
