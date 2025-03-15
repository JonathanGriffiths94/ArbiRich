import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from bytewax import operators as op
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

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


class RedisExchangePartition(StatefulSourcePartition):
    def __init__(self, exchange: str, channel: str, pairs: Optional[List[str]] = None):
        self.exchange = exchange
        self.channel = channel
        self.pairs = pairs
        # Use shared Redis client instead of creating a new one
        self.redis_client = get_shared_redis_client()
        self.last_activity = time.time()
        self.error_backoff = 1  # Initial backoff in seconds
        self.max_backoff = 30  # Maximum backoff in seconds
        logger.info(f"Initialized RedisExchangePartition for {exchange} on channel {channel}")

    def next_batch(self):
        try:
            # Check Redis connection health periodically
            if time.time() - self.last_activity > 30:  # Check every 30 seconds
                if not self.redis_client.is_healthy():
                    logger.warning(f"Redis connection unhealthy for {self.exchange}, reconnecting...")
                    self.redis_client = RedisService()  # Recreate Redis connection
                self.last_activity = time.time()

            # Get message from Redis
            message = self.redis_client.get_message(self.channel)

            if message:
                logger.debug(f"Fetched message for {self.exchange}: {message}")
                self.error_backoff = 1  # Reset backoff on successful message
                return [(self.exchange, message)]

            # No message but successful check - no need to introduce delay
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
            channel = self.exchange_channels.get(exchange)

            if not channel:
                logger.error(f"No channel configured for exchange {exchange}")
                return None

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
