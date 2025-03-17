# Move both Redis source implementations here from:
# - /src/arbirich/sources/arbitrage_source.py
# - /src/arbirich/flows/arbitrage/arbitrage_source.py
# - /src/arbirich/flows/execution/execution_source.py

import json
import logging
import time
from dataclasses import dataclass
from threading import Lock
from typing import Dict, List, Optional, Tuple, Union

from bytewax import operators as op
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

from src.arbirich.models.models import OrderBookUpdate
from src.arbirich.services.redis_service import RedisService

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create a single shared Redis client for all partitions
_shared_redis_client = None


def get_shared_redis_client():
    global _shared_redis_client
    if _shared_redis_client is None:
        _shared_redis_client = RedisService()
    return _shared_redis_client


# Base Redis partition with common functionality
class BaseRedisPartition(StatefulSourcePartition):
    def __init__(self, channel: str):
        self.channel = channel
        self.redis_client = get_shared_redis_client()
        self.last_activity = time.time()
        self.error_backoff = 1
        self.max_backoff = 30

        # Subscribe to the Redis pubsub channel
        self.pubsub = self.redis_client.client.pubsub()
        self.pubsub.subscribe(self.channel)
        logger.info(f"Initialized Redis partition for channel: {channel}")

    def snapshot(self):
        # No state to snapshot
        return None

    def _health_check(self):
        """Check Redis connection health and reconnect if needed"""
        if time.time() - self.last_activity > 30:
            if not self.redis_client.is_healthy():
                logger.warning("Redis connection unhealthy, reconnecting...")
                self.redis_client = RedisService()
                self.pubsub = self.redis_client.client.pubsub()
                self.pubsub.subscribe(self.channel)
            self.last_activity = time.time()


# Specific implementations
class RedisExchangePartition(BaseRedisPartition):
    """Partition for receiving order book updates from Redis"""

    def __init__(self, exchange: str, channel: str = "order_book", pairs: Optional[List[str]] = None):
        super().__init__(channel)
        self.exchange = exchange
        self.pairs = pairs

    def next_batch(self) -> List[Tuple[str, Union[Dict, OrderBookUpdate]]]:
        """Get the next batch of messages and convert to OrderBookUpdate"""
        try:
            self._health_check()

            # Get message from Redis pubsub
            message = self.pubsub.get_message(timeout=0.01)

            if message and message["type"] == "message":
                data = message.get("data")
                if isinstance(data, bytes):
                    data = data.decode("utf-8")

                try:
                    # Parse JSON data
                    data_dict = json.loads(data)

                    # Only process messages for this exchange
                    if data_dict.get("exchange") == self.exchange:
                        symbol = data_dict.get("symbol")

                        # Filter by pairs if specified
                        if self.pairs and symbol not in self.pairs:
                            return []

                        # Convert to OrderBookUpdate
                        try:
                            order_book = OrderBookUpdate(
                                exchange=self.exchange,
                                symbol=symbol,
                                bids=data_dict.get("bids", {}),
                                asks=data_dict.get("asks", {}),
                                timestamp=data_dict.get("timestamp", time.time()),
                                sequence=data_dict.get("sequence"),
                            )
                            return [(self.exchange, order_book)]
                        except Exception as e:
                            logger.error(f"Error creating OrderBookUpdate: {e}")
                            # Fall back to raw dict if model creation fails
                            return [(self.exchange, data_dict)]
                except Exception as e:
                    logger.error(f"Error processing message: {e}, data: {data}")

            return []
        except Exception as e:
            logger.error(f"Error fetching next message: {e}")
            time.sleep(min(self.error_backoff, self.max_backoff))
            self.error_backoff = min(self.error_backoff * 2, self.max_backoff)
            return []


class RedisExecutionPartition(BaseRedisPartition):
    """Partition for receiving trade opportunities from Redis"""

    def __init__(self, strategy_name=None):
        # Use the channel manager to get the correct channel name
        from src.arbirich.services.redis_channel_manager import RedisChannelManager

        self.strategy_name = strategy_name
        self.channel_manager = RedisChannelManager(get_shared_redis_client())
        channel = self.channel_manager.get_opportunity_channel(strategy_name)

        super().__init__(channel)
        self._lock = Lock()

    def next_batch(self) -> list:
        try:
            with self._lock:
                self._health_check()

                # Get opportunity from Redis
                message = self.redis_client.get_message(self.channel)

                if message:
                    try:
                        # Parse message (assuming it's JSON)
                        if isinstance(message, bytes):
                            message = message.decode("utf-8")
                        opportunity = json.loads(message)

                        logger.debug(f"Received opportunity: {opportunity}")
                        self.last_activity = time.time()
                        self.error_backoff = 1  # Reset backoff on success
                        return [opportunity]
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")

                return []
        except Exception as e:
            logger.error(f"Error in next_batch: {e}")
            time.sleep(min(self.error_backoff, self.max_backoff))
            self.error_backoff = min(self.error_backoff * 2, self.max_backoff)
            return []


# Source classes
@dataclass
class RedisOpportunitySource(FixedPartitionedSource):
    exchange_channels: Dict[str, str]
    pairs: Optional[List[Tuple[str, str]]] = None

    def list_parts(self):
        parts = list(self.exchange_channels.keys())
        if not parts:
            logger.error("No partitions were created! Check your exchange-channel mapping.")
        return parts

    def build_part(self, step_id, for_key, _resume_state):
        try:
            exchange = for_key
            channel = self.exchange_channels.get(exchange, "order_book")

            pairs = [f"{base}-{quote}" for base, quote in self.pairs] if self.pairs else None
            return RedisExchangePartition(exchange, channel, pairs)
        except Exception as e:
            logger.error(f"Invalid partition key: {for_key}, Error: {e}")
            return None


class RedisExecutionSource(FixedPartitionedSource):
    def __init__(self, strategy_name=None):
        self.strategy_name = strategy_name

    def list_parts(self):
        return ["execution_part"]

    def build_part(self, step_id, for_key, _resume_state):
        return RedisExecutionPartition(self.strategy_name)


# Convenience functions
def use_redis_opportunity_source(flow, step_name, exchange_channels, pairs=None):
    """Add a Redis opportunity source to a Bytewax dataflow."""
    source = RedisOpportunitySource(exchange_channels, pairs)
    return op.input(step_name, flow, source)


def use_redis_execution_source(flow, step_name, strategy_name=None):
    """Add a Redis execution source to a Bytewax dataflow."""
    source = RedisExecutionSource(strategy_name)
    return op.input(step_name, flow, source)
