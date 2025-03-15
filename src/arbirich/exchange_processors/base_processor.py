import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Dict, Generator, Optional

logger = logging.getLogger(__name__)


class BaseExchangeProcessor(ABC):
    """
    Base class for all exchange processors.
    Exchange processors are responsible for connecting to exchange APIs
    and parsing order book data into a standard format.
    """

    def __init__(
        self, exchange: str, product_id: str, subscription_type: str = "snapshot", use_rest_snapshot: bool = False
    ):
        """
        Initialize the processor.

        Parameters:
            exchange: The exchange name
            product_id: The product ID (e.g., 'BTC-USD')
            subscription_type: Type of subscription ('snapshot' or 'delta')
            use_rest_snapshot: Whether to use REST API for initial snapshot
        """
        self.exchange = exchange
        self.product_id = product_id
        self.subscription_type = subscription_type
        self.use_rest_snapshot = use_rest_snapshot
        self.logger = logging.getLogger(f"{__name__}.{exchange}")
        self.logger.info(f"Initialized processor for {exchange}:{product_id}")

    @abstractmethod
    async def connect(self) -> None:
        """
        Connect to the exchange API.
        """
        pass

    @abstractmethod
    async def subscribe(self) -> None:
        """
        Subscribe to order book updates.
        """
        pass

    @abstractmethod
    async def fetch_order_book(self) -> Dict:
        """
        Fetch a snapshot of the order book.

        Returns:
            A dictionary with the order book data
        """
        pass

    @abstractmethod
    async def parse_message(self, message: Dict) -> Optional[Dict]:
        """
        Parse a WebSocket message into order book format.

        Parameters:
            message: The raw message from the WebSocket

        Returns:
            A dictionary with the parsed order book data or None if it's not an order book update
        """
        pass

    async def process_messages(self) -> Generator[Dict, None, None]:
        """
        Process messages from the WebSocket and yield parsed order book updates.

        Yields:
            Parsed order book updates
        """
        try:
            await self.connect()
            await self.subscribe()

            # Optionally get an initial snapshot
            if self.use_rest_snapshot:
                try:
                    snapshot = await self.fetch_order_book()
                    if snapshot:
                        yield snapshot
                except Exception as e:
                    self.logger.error(f"Error fetching initial snapshot: {e}")

            # Process WebSocket messages
            async for message in self._listen():
                try:
                    parsed = await self.parse_message(message)
                    if parsed:
                        yield parsed
                except Exception as e:
                    self.logger.error(f"Error parsing message: {e}")
                    continue

        except Exception as e:
            self.logger.error(f"Error in message processing: {e}")
            # Re-raise to signal that the connection is broken
            raise

    @abstractmethod
    async def _listen(self) -> Generator[Dict, None, None]:
        """
        Listen for WebSocket messages.

        Yields:
            Raw WebSocket messages
        """
        pass

    def run(self) -> Generator[Dict, None, None]:
        """
        Run the processor and yield order book updates.

        This is the main entry point for the processor and is called by the ingestion flow.
        It converts the async process_messages generator to a sync generator.

        Yields:
            Order book updates in a standardized format
        """
        self.logger.info(f"Starting processor for {self.exchange}:{self.product_id}")

        # Create a new event loop in this thread if necessary
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        async def get_messages():
            try:
                async for message in self.process_messages():
                    yield message
            except Exception as e:
                self.logger.error(f"Error processing messages: {e}")
                # Yield an empty message to allow the generator to continue
                yield {"bids": {}, "asks": {}, "timestamp": 0}

        # Convert async generator to sync generator
        message_gen = get_messages()

        while True:
            try:
                # Get the next message from the async generator
                message = loop.run_until_complete(anext_wrapper(message_gen))

                # If we're starting a new event loop, we should clean it up
                # to avoid "Task was destroyed but it is pending" warnings
                if loop != asyncio.get_event_loop():
                    pending = asyncio.all_tasks(loop)
                    if pending:
                        loop.run_until_complete(asyncio.gather(*pending))

                # Add exchange-specific fields if needed
                if "timestamp" not in message:
                    message["timestamp"] = loop.time()

                yield message

            except StopAsyncIteration:
                self.logger.info(f"No more messages for {self.exchange}:{self.product_id}")
                break
            except Exception as e:
                self.logger.error(f"Error in run loop: {e}")
                # Sleep briefly to avoid tight loops on error
                loop.run_until_complete(asyncio.sleep(1))
                # Yield an empty message to allow the generator to continue
                yield {"bids": {}, "asks": {}, "timestamp": 0}


async def anext_wrapper(async_gen):
    """Helper function to handle Python versions that don't have anext() builtin"""
    try:
        # For Python 3.10+
        return await anext(async_gen)
    except NameError:
        # For earlier Python versions
        return await async_gen.__anext__()
