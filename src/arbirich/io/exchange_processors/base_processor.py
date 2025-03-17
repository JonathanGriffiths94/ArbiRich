import asyncio
import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Generator, Optional

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class OrderBookGapException(Exception):
    """Raised when there is a gap in the order book update sequence."""

    pass


class BaseOrderBookProcessor(ABC):
    """Base class for all exchange order book processors."""

    def __init__(
        self, exchange: str, product: str, subscription_type: str = "snapshot", use_rest_snapshot: bool = False
    ):
        """
        Initialize the order book processor.

        Parameters:
            exchange: The exchange name
            product: The product/symbol to get order books for
            subscription_type: Type of subscription (snapshot or delta)
            use_rest_snapshot: Whether to use REST API for initial snapshot
        """
        self.exchange = exchange
        self.product = product
        self.subscription_type = subscription_type
        self.use_rest_snapshot = use_rest_snapshot
        self.order_book = {"bids": {}, "asks": {}}
        self.local_update_id = None
        logger.info(f"Initialized {self.__class__.__name__} for {exchange}:{product}")

    async def get_initial_snapshot(self) -> Optional[Dict[str, Any]]:
        """
        Get initial order book snapshot from REST API.

        Returns:
            Dict with order book data or None if not available
        """
        logger.warning(f"get_initial_snapshot not implemented for {self.exchange}")
        return None

    async def run(self) -> Generator[Dict[str, Any], None, None]:
        """
        Standard implementation of run() that most processors can use.

        It follows this algorithm:
         1. Connect to WebSocket and subscribe.
         2. Buffer initial events.
         3. Fetch a full snapshot (via REST or WebSocket).
         4. Initialize the local order book.
         5. Process buffered events.
         6. Process live delta updates continuously.
         7. If sequence gaps occur, re-subscribe.

        Yields:
            Dict with order book data
        """
        while True:
            try:
                async with await self.connect() as websocket:
                    await self.subscribe(websocket)

                    # Buffer initial events and record the first event.
                    buffered_events, first_event = await self.buffer_events(websocket)
                    snapshot = self.fetch_snapshot() if self.use_rest_snapshot else first_event
                    logger.debug(f"Snapshot: {snapshot}")

                    # Get snapshot id
                    snapshot_update_id = self.get_snapshot_update_id(snapshot)
                    logger.debug(f"Snapshot update ID: {snapshot_update_id}")

                    if snapshot_update_id == 1:
                        logger.debug(
                            "Received snapshot with update ID 1 (service restart). Overwriting local order book."
                        )
                        self.order_book = self.init_order_book(snapshot)
                        self.local_update_id = snapshot_update_id
                        yield self.order_book

                    # For delta subscriptions, verify that the snapshot is recent.
                    if self.subscription_type == "delta" and snapshot_update_id < first_event.get(
                        "U", snapshot_update_id
                    ):
                        logger.warning("Snapshot is older than first buffered event. Restarting...")
                        continue

                    # Get valid events from buffered events where the snapshot id is greater than the current id
                    valid_events = [
                        e for e in buffered_events if self.get_snapshot_update_id(snapshot) > snapshot_update_id
                    ]

                    # In delta mode, filter events whose update id is greater than the snapshot.
                    if self.subscription_type == "delta":
                        deadline = asyncio.get_event_loop().time() + 5  # wait an extra 2 seconds
                        # Look for the next update id
                        while (
                            not any(
                                self.get_first_update_id(snapshot)
                                <= snapshot_update_id + 1
                                <= self.get_snapshot_update_id(snapshot)
                                for event in valid_events
                            )
                            and asyncio.get_event_loop().time() < deadline
                        ):
                            try:
                                message = await asyncio.wait_for(websocket.recv(), timeout=0.5)
                                event = json.loads(message)
                                if "U" in event and "u" in event:
                                    buffered_events.append(event)
                                    valid_events = [
                                        e
                                        for e in buffered_events
                                        if self.get_snapshot_update_id(snapshot) > snapshot_update_id
                                    ]
                            except asyncio.TimeoutError:
                                pass

                        if not any(
                            self.get_first_update_id(snapshot)
                            <= snapshot_update_id + 1
                            <= self.get_snapshot_update_id(snapshot)
                            for event in valid_events
                        ):
                            logger.warning(
                                "Buffered events still do not properly cover the snapshot update. Restarting..."
                            )
                            continue

                    try:
                        self.process_buffered_events(valid_events, snapshot_update_id)
                    except OrderBookGapException:
                        # Optionally log additional details, then break/continue to trigger a resubscription.
                        continue

                    # Initialize the local order book.
                    self.order_book = self.init_order_book(snapshot)
                    self.local_update_id = snapshot_update_id
                    logger.debug(f"Initialized local order book with update ID: {self.local_update_id}")

                    # Process live updates continuously.
                    async for message in self.live_updates(websocket):
                        event = json.loads(message)

                        # For delta subscriptions, if a previous update ID is provided, check it.
                        if self.subscription_type == "delta" and self.get_first_update_id(event) is not None:
                            if self.get_first_update_id(event) != self.local_update_id + 1:
                                logger.error("Delta update sequence mismatch; re-subscribing...")
                                await self.resubscribe(websocket)
                                break

                        # Ignore outdated events.
                        if self.get_snapshot_update_id(event) <= self.local_update_id:
                            continue

                        # Apply the delta update.
                        self.apply_event(event)
                        self.local_update_id = self.get_snapshot_update_id(event)
                        if self.order_book:
                            yield self.order_book
            except Exception as e:
                logger.error(
                    f"Error in order book processor: {e}. Retrying in 5 seconds...",
                    exc_info=True,
                )
                await asyncio.sleep(5)

    def process_asset(self) -> str:
        try:
            quote, base = self.product.split("-")
        except ValueError:
            # If splitting fails, fall back to using the product as-is.
            return self.product.upper()
        # Check if quote currency has a mapping for the product
        mapping = self.cfg.get("mapping", {})
        if quote in mapping.keys():
            quote = mapping[quote]
        # Return symbol with exchange specific format
        delimiter = self.cfg.get("delimiter", "")
        return delimiter.join((quote, base))

    def asset_map(self):
        pass

    @abstractmethod
    async def connect(self):
        """Return an async context manager for the WebSocket connection."""
        pass

    @abstractmethod
    async def subscribe(self, websocket):
        """Send the subscription message over the WebSocket."""
        pass

    @abstractmethod
    async def buffer_events(self, websocket):
        """
        Buffer initial events from the WebSocket.
        Returns a tuple: (buffered_events, first_event).
        """
        pass

    @abstractmethod
    def fetch_snapshot(self):
        """Fetch a full order book snapshot via REST."""
        pass

    @abstractmethod
    def get_snapshot_update_id(self, snapshot):
        """
        Extract and return the snapshot update ID.
        """
        pass

    @abstractmethod
    def get_first_update_id(self, snapshot):
        """
        Extract and return the first snapshot ID.
        """
        pass

    @abstractmethod
    def get_final_update_id(self, event):
        """
        Extract and return the final snapshot ID.
        """
        pass

    @abstractmethod
    def init_order_book(self, snapshot):
        """Initialize the local order book from the snapshot."""
        pass

    @abstractmethod
    def process_buffered_events(self, events, snapshot_update_id):
        """Apply buffered delta events to the local order book."""
        pass

    @abstractmethod
    async def live_updates(self, websocket):
        """Yield live update messages from the WebSocket."""
        pass

    @abstractmethod
    def apply_event(self, event):
        """Apply a single delta update event to the local order book."""
        pass

    @abstractmethod
    async def resubscribe(self, websocket):
        """Re-issue a subscription request to obtain a new snapshot."""
        pass
