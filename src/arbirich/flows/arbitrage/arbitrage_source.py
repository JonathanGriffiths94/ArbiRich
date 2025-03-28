import json
import logging
import time
from dataclasses import dataclass
from typing import List, Optional

from bytewax import operators as op
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

from src.arbirich.core.system_state import is_system_shutting_down, mark_component_notified
from src.arbirich.models.models import OrderBookUpdate
from src.arbirich.services.redis.redis_service import get_shared_redis_client

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class RedisExchangePartition(StatefulSourcePartition):
    def __init__(self, exchange: str, channel: str = "order_book", pairs: Optional[List[str]] = None, stop_event=None):
        self.exchange = exchange
        self.channel = channel
        self.pairs = pairs
        self.stop_event = stop_event  # Store the stop event
        # Use shared Redis client instead of creating a new one
        self.redis_client = get_shared_redis_client()
        self.last_activity = time.time()
        self.error_backoff = 1  # Initial backoff in seconds
        self.max_backoff = 30  # Maximum backoff in seconds
        # Subscribe to the Redis pubsub channel explicitly
        self.pubsub = self.redis_client.client.pubsub()
        self.pubsub.subscribe(self.channel)
        self._running = True
        logger.info(f"Initialized RedisExchangePartition for {exchange} and subscribed to channel {channel}")

    def subscribe_with_retry(self, channels, max_retries=3, retry_delay=1.0):
        """
        Subscribe to Redis channels with retry logic.

        Args:
            channels: List of channels to subscribe to
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries in seconds

        Returns:
            True if subscription succeeded, False otherwise
        """
        logger.info(f"Subscribing to channels with retry: {channels}")

        for attempt in range(max_retries):
            try:
                # Ensure we have a valid Redis client
                if not self.redis_client or not self.redis_client.client:
                    logger.warning(f"Redis client unavailable on attempt {attempt + 1}, reconnecting...")
                    self.redis_client = get_shared_redis_client()
                    if not self.redis_client:
                        logger.error("Failed to get Redis client")
                        time.sleep(retry_delay)
                        continue

                # Create or reset the pubsub
                if hasattr(self, "pubsub") and self.pubsub:
                    try:
                        self.pubsub.close()
                    except Exception as e:
                        logger.warning(f"Error closing existing pubsub: {e}")

                self.pubsub = self.redis_client.client.pubsub()

                # Perform the subscription
                self.pubsub.subscribe(*channels)
                logger.info(f"Successfully subscribed to {len(channels)} channels: {channels}")

                # Verify the subscription worked by checking for confirmation messages
                for _ in range(len(channels)):
                    message = self.pubsub.get_message(timeout=1.0)
                    if message and message.get("type") == "subscribe":
                        logger.debug(f"Received subscription confirmation: {message}")
                    else:
                        logger.warning(f"Did not receive subscription confirmation: {message}")
                        # We'll retry the whole process
                        raise Exception("Failed to get subscription confirmation")

                # All channels subscribed successfully
                return True

            except Exception as e:
                logger.error(f"Error subscribing to channels (attempt {attempt + 1}/{max_retries}): {e}")
                time.sleep(retry_delay)

        # If we got here, all retries failed
        logger.error(f"Failed to subscribe to channels after {max_retries} attempts: {channels}")
        return False

    def next_batch(self) -> list:
        try:
            # Check if the system is shutting down
            if is_system_shutting_down():
                component_id = f"arbitrage:{self.exchange}"
                if mark_component_notified("arbitrage", component_id):
                    logger.info(f"Stop event detected for {component_id}")
                    # Clean up resources
                    try:
                        self.pubsub.unsubscribe()
                        self.pubsub.close()
                        logger.info(f"Successfully unsubscribed {component_id} from Redis channels")
                    except Exception as e:
                        logger.error(f"Error unsubscribing {component_id} from Redis channels: {e}")
                self._running = False
                return []

            # Check Redis connection health periodically
            if time.time() - self.last_activity > 30:  # Check every 30 seconds
                if not self.redis_client.is_healthy():
                    logger.warning(f"Redis connection unhealthy for {self.exchange}, reconnecting...")
                    self.redis_client = get_shared_redis_client()
                    # Resubscribe after reconnecting
                    self.pubsub = self.redis_client.client.pubsub()
                    self.pubsub.subscribe(self.channel)
                self.last_activity = time.time()

            # Get message from Redis pubsub
            message = self.pubsub.get_message(timeout=0.01)  # Use a short timeout

            if message and message["type"] == "message":
                data = message.get("data")
                if isinstance(data, bytes):
                    data = data.decode("utf-8")

                try:
                    # Try to parse JSON data
                    data_dict = json.loads(data)

                    # Only process messages for this exchange
                    if data_dict.get("exchange") == self.exchange:
                        symbol = data_dict.get("symbol")
                        logger.debug(f"Received message for {self.exchange}: {symbol}")

                        # Filter by pairs if specified
                        if self.pairs and symbol not in self.pairs:
                            logger.debug(f"Skipping message for {symbol} - not in specified pairs {self.pairs}")
                            return []

                        # Convert to OrderBookUpdate model
                        try:
                            order_book = OrderBookUpdate(
                                exchange=self.exchange,
                                symbol=symbol,
                                bids=data_dict.get("bids", {}),
                                asks=data_dict.get("asks", {}),
                                timestamp=data_dict.get("timestamp", time.time()),
                                sequence=data_dict.get("sequence"),
                            )
                            logger.debug(f"Created OrderBookUpdate for {self.exchange}:{symbol}")
                            return [(self.exchange, order_book)]
                        except Exception as e:
                            logger.error(f"Error creating OrderBookUpdate: {e}")
                            # Fall back to returning the raw dictionary if model creation fails
                            return [(self.exchange, data_dict)]
                except json.JSONDecodeError:
                    logger.warning(f"Failed to decode JSON message: {data}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}, data: {data}")

            # If we got here, either no message or message was not for this exchange
            return []

        except Exception as e:
            # Use exponential backoff for error retries
            logger.error(f"Error fetching next message for {self.exchange}: {e}")
            time.sleep(min(self.error_backoff, self.max_backoff))
            self.error_backoff = min(self.error_backoff * 2, self.max_backoff)
            return []

    def snapshot(self):
        # No state to snapshot
        return None


@dataclass
class RedisOpportunitySource(FixedPartitionedSource):
    def __init__(self, exchange_channels, pairs, stop_event=None):
        """
        Initialize the Redis opportunity source.

        Args:
            exchange_channels: Dict mapping exchange names to channels
            pairs: List of trading pairs
            stop_event: Optional threading.Event for stopping the source
        """
        self.exchange_channels = exchange_channels
        self.pairs = pairs
        self.stop_event = stop_event

    def list_parts(self):
        parts = list(self.exchange_channels.keys())
        if not parts:
            logger.error("No partitions were created! Check your exchange-channel mapping.")
        logger.info(f"List of partitions: {parts}")
        return parts

    def build_part(self, step_id, for_key, _resume_state):
        try:
            exchange = for_key
            channel = self.exchange_channels.get(exchange, "order_book")  # Default to "order_book" if not specified

            pairs = [f"{base}-{quote}" for base, quote in self.pairs] if self.pairs else None
            logger.info(f"Building partition for exchange: {exchange}")
            return RedisExchangePartition(exchange, channel, pairs, stop_event=self.stop_event)
        except Exception as e:
            logger.error(f"Invalid partition key: {for_key}, Error: {e}")
            return None


def use_redis_opportunity_source(flow, op_name, exchange_channels, pairs, stop_event=None):
    """
    Add a Redis opportunity source to the flow.

    Parameters:
        flow: The Dataflow instance
        op_name: The operator name
        exchange_channels: Dict of exchange to channels
        pairs: List of trading pairs
        stop_event: Optional threading.Event for stopping the source

    Returns:
        The input stream
    """
    source = RedisOpportunitySource(exchange_channels, pairs, stop_event=stop_event)

    return op.input(op_name, flow, source)


import logging
import threading

from src.arbirich.services.redis.redis_service import RedisService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Shared Redis client and lock
_shared_redis_client = None
_redis_lock = threading.Lock()


def get_shared_redis_client():
    """Get or create a shared Redis client for this module."""
    global _shared_redis_client
    with _redis_lock:
        if _shared_redis_client is None:
            try:
                _shared_redis_client = RedisService()
                logger.debug("Created new Redis client for arbitrage source")
            except Exception as e:
                logger.error(f"Error creating Redis client: {e}")
                return None
        return _shared_redis_client


def reset_shared_redis_client():
    """Reset the shared Redis client for clean restarts."""
    global _shared_redis_client
    with _redis_lock:
        if _shared_redis_client is not None:
            try:
                _shared_redis_client.close()
                logger.info("Closed shared Redis client in arbitrage source")
            except Exception as e:
                logger.warning(f"Error closing Redis client: {e}")
            _shared_redis_client = None
