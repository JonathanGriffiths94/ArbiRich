import json
import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union

from bytewax import operators as op
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

from src.arbirich.models.models import OrderBookUpdate
from src.arbirich.services.redis_service import RedisService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a single shared Redis client for all partitions
_shared_redis_client = None


def get_shared_redis_client():
    global _shared_redis_client
    if _shared_redis_client is None:
        _shared_redis_client = RedisService()
    return _shared_redis_client


class RedisExchangePartition(StatefulSourcePartition):
    def __init__(self, exchange: str, channel: str = "order_book", pairs: Optional[List[str]] = None):
        self.exchange = exchange
        self.channel = channel
        self.pairs = pairs
        # Use shared Redis client instead of creating a new one
        self.redis_client = get_shared_redis_client()
        self.last_activity = time.time()
        self.error_backoff = 1  # Initial backoff in seconds
        self.max_backoff = 30  # Maximum backoff in seconds
        # Subscribe to the Redis pubsub channel explicitly
        self.pubsub = self.redis_client.client.pubsub()
        self.pubsub.subscribe(self.channel)
        logger.info(f"Initialized RedisExchangePartition for {exchange} and subscribed to channel {channel}")

    def next_batch(self) -> List[Tuple[str, Union[Dict, OrderBookUpdate]]]:
        """
        Get the next batch of messages from Redis.
        Returns a list of tuples (exchange, OrderBookUpdate).
        """
        try:
            # Check Redis connection health periodically
            if time.time() - self.last_activity > 30:  # Check every 30 seconds
                if not self.redis_client.is_healthy():
                    logger.warning(f"Redis connection unhealthy for {self.exchange}, reconnecting...")
                    self.redis_client = RedisService()  # Recreate Redis connection
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
    exchange_channels: Dict[str, str]
    pairs: Optional[List[Tuple[str, str]]] = None

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
            return RedisExchangePartition(exchange, channel, pairs)
        except Exception as e:
            logger.error(f"Invalid partition key: {for_key}, Error: {e}")
            return None


def use_redis_opportunity_source(flow, step_name, exchange_channels, pairs=None):
    """
    Add a Redis opportunity source to a Bytewax dataflow.
    """
    source = RedisOpportunitySource(exchange_channels, pairs)
    return op.input(step_name, flow, source)
