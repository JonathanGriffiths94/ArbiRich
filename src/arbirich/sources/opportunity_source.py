import logging
import time
from dataclasses import dataclass

import redis
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

from src.arbirich.services.redis_service import RedisService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

redis_client = RedisService()


class RedisOpportunityPartition(StatefulSourcePartition):
    """
    Listens for price updates in a Redis Pub/Sub channel and streams them.
    """

    def __init__(self, channel: str):
        logger.debug("Initializing RedisOpportunityPartition.")
        self.gen = redis_client.subscribe_to_order_book_updates(channel)
        self._running = True
        self._last_activity = time.time()
        self.channel = channel

    def next_batch(self):
        """
        Reads messages from Redis Pub/Sub and yields them as batches.
        Automatically reconnects if Redis disconnects.
        """
        if time.time() - self._last_activity > 60:
            self._last_activity = time.time()
            if not redis_client.is_healthy():
                logger.warning("Redis connection appears unhealthy")
                return []

        if not self._running:
            logger.info("Partition marked as not running, stopping")
            return []

        try:
            result = next(self.gen, None)
            if result:
                self._last_activity = time.time()
                return [result]
            return []

        except StopIteration:
            logger.warning("Redis generator stopped, restarting subscription...")
            self.gen = redis_client.subscribe_to_order_book_updates(self.channel)
            return []

        except redis.exceptions.ConnectionError:
            logger.error("Redis connection lost! Attempting to reconnect...")
            self.gen = redis_client.subscribe_to_order_book_updates(self.channel)
            return []

        except Exception as e:
            logger.error(f"Error in next_batch: {e}")
            return []

    def snapshot(self) -> None:
        # Return None for now (or implement snapshot logic if needed)
        logger.debug("Snapshot requested for RedisOpportunityPartition (returning None).")
        return None


@dataclass
class RedisOpportunitySource(FixedPartitionedSource):
    """
    Creates partitions for each exchange's price update channel.
    """

    exchange_channels: dict[str, str]  # Mapping: { "exchange": "redis_channel" }

    def list_parts(self):
        """Returns partition keys based on exchange names."""
        parts = list(self.exchange_channels.keys())
        logger.info(f"List of partitions (Redis channels): {parts}")
        return parts

    def build_part(self, step_id, for_key, _resume_state):
        """
        Builds a Redis listener partition for each exchange.
        """
        try:
            redis_channel = self.exchange_channels[for_key]
            logger.info(f"Redis channel: {redis_channel}")
        except KeyError:
            raise ValueError(f"Invalid exchange key: {for_key}")
        logger.info(f"Building Redis partition for exchange: {for_key}")
        return RedisOpportunityPartition(redis_channel)
