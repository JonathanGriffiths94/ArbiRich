import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple, Type

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class WebsocketPartition(StatefulSourcePartition):
    """
    A Bytewax partition for handling websocket connections to exchanges.
    Each partition handles one exchange+product pair.
    """

    def __init__(self, exchange: str, product: str, processor_class: Type):
        self.exchange = exchange
        self.product = product
        self.processor_class = processor_class
        self.processor = None
        self.last_activity = time.time()
        self.error_backoff = 1
        self.max_backoff = 30
        self.running = False
        self.queue = asyncio.Queue()
        self.loop = None
        logger.info(f"Initialized WebsocketPartition for {exchange}:{product}")

    def _get_or_create_event_loop(self):
        """Get the current event loop or create a new one if none exists."""
        try:
            return asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop

    def next_batch(self):
        """
        Get the next batch of data from the websocket.
        This method runs in the Bytewax worker thread.
        """
        try:
            # Ensure websocket coroutine is running
            if not self.running:
                self.loop = self._get_or_create_event_loop()
                # Start the websocket processor
                self.loop.run_until_complete(self._initialize_processor())
                self.running = True
                # Start the consumer in the background
                asyncio.run_coroutine_threadsafe(self._consume_websocket(), self.loop)

            # Check for data in the queue (non-blocking)
            try:
                # Use loop.run_until_complete to handle asyncio calls from a non-async context
                order_book = self.loop.run_until_complete(asyncio.wait_for(self.queue.get(), 0.1))
                self.last_activity = time.time()
                self.error_backoff = 1  # Reset backoff on successful message
                return [(self.exchange, self.product, order_book)]
            except asyncio.TimeoutError:
                # No data available yet, return empty batch
                return []

        except Exception as e:
            # Use exponential backoff for error retries
            logger.error(f"Error in next_batch for {self.exchange}:{self.product}: {e}")
            time.sleep(min(self.error_backoff, self.max_backoff))
            self.error_backoff = min(self.error_backoff * 2, self.max_backoff)
            # Try to reconnect if necessary
            self.running = False
            return []

    async def _initialize_processor(self):
        """Initialize the websocket processor."""
        try:
            logger.info(f"Initializing processor for {self.exchange}:{self.product}")
            self.processor = self.processor_class(
                exchange=self.exchange,
                product=self.product,
                subscription_type="snapshot",
                use_rest_snapshot=False,
            )
        except Exception as e:
            logger.error(f"Error initializing processor for {self.exchange}:{self.product}: {e}")
            raise

    async def _consume_websocket(self):
        """
        Consume data from the websocket processor and put it in the queue.
        This runs as a background task in the event loop.
        """
        try:
            logger.info(f"Starting websocket consumer for {self.exchange}:{self.product}")
            async for order_book in self.processor.run():
                await self.queue.put(order_book)
        except Exception as e:
            logger.error(f"Error in websocket consumer for {self.exchange}:{self.product}: {e}")
            self.running = False
        finally:
            logger.info(f"Websocket consumer for {self.exchange}:{self.product} stopped")

    def snapshot(self):
        """Get the partition state for recovery."""
        return None


@dataclass
class MultiExchangeSource(FixedPartitionedSource):
    """
    A Bytewax source that creates partitions for multiple exchanges and products.
    """

    exchanges_and_pairs: Dict[str, List[str]]
    processor_factory: Callable[[str], Type]

    def list_parts(self) -> List[Tuple[str, str]]:
        """
        List all exchange+product partitions.
        """
        parts = []
        for exchange, products in self.exchanges_and_pairs.items():
            for product in products:
                parts.append(f"{product}_{exchange}")

        logger.info(f"List of partitions: {parts}")
        return parts

    def build_part(self, step_id: str, for_key: str, resume_state: Any) -> Optional[StatefulSourcePartition]:
        """
        Build a partition for a specific exchange+product pair.
        """
        try:
            # Parse exchange and product from the key
            parts = for_key.split("_")
            if len(parts) >= 2:
                product = parts[0]
                exchange = parts[1]
            else:
                logger.error(f"Invalid partition key format: {for_key}")
                return None

            logger.info(f"Building partition for key: {for_key}")

            # Get the processor class for this exchange
            processor_class = self.processor_factory(exchange)

            # Create and return the partition
            return WebsocketPartition(exchange, product, processor_class)
        except Exception as e:
            logger.error(f"Error building partition for {for_key}: {e}")
            return None
