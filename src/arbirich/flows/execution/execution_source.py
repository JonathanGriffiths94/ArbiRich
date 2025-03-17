import logging
import time
from threading import Lock

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

from src.arbirich.flows.arbitrage.arbitrage_source import get_shared_redis_client
from src.arbirich.services.redis_service import RedisService

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class RedisExecutionPartition(StatefulSourcePartition):
    def __init__(self, strategy_name=None):
        """
        Initialize a Redis partition for trade opportunities.

        Parameters:
            strategy_name: Optional strategy name to filter opportunities
        """
        logger.info(f"Initializing RedisExecutionPartition for strategy: {strategy_name or 'all'}")
        # Store strategy name
        self.strategy_name = strategy_name

        # Use shared Redis client instead of creating a new one
        self.redis_client = get_shared_redis_client()

        # Use the channel manager to get the correct channel name
        from src.arbirich.services.redis_channel_manager import RedisChannelManager

        self.channel_manager = RedisChannelManager(self.redis_client)
        self.channel = self.channel_manager.get_opportunity_channel(strategy_name)

        self._running = True
        self._last_activity = time.time()
        self._lock = Lock()
        self._error_backoff = 1  # Initial backoff in seconds
        self._max_backoff = 30  # Maximum backoff in seconds

        # Subscribe to the appropriate channel
        self.redis_client._ensure_subscribed(self.channel)

        logger.info(f"RedisExecutionPartition subscribed to channel: {self.channel}")

    def next_batch(self) -> list:
        try:
            with self._lock:
                # Check if we should perform periodic health check
                current_time = time.time()
                if current_time - self._last_activity > 30:  # 30 seconds timeout
                    logger.debug("Performing periodic health check on Redis connection")
                    self._last_activity = current_time

                    # Check connection health and recreate if necessary
                    if not self.redis_client.is_healthy():
                        logger.warning("Redis connection appears unhealthy, reconnecting")
                        # Close existing connection and create a new one
                        try:
                            self.redis_client.close()
                        except Exception as e:
                            logger.warning(f"Error closing Redis connection: {e}")

                        self.redis_client = RedisService()
                        self.redis_client._ensure_subscribed(self.channel)

                if not self._running:
                    logger.info("Partition marked as not running, stopping")
                    return []

                # Get opportunity from Redis
                message = self.redis_client.get_message(self.channel)
                if message:
                    logger.info(
                        f"Received message on channel {self.channel}: {message[:100]}..."
                    )  # Log with truncation for large messages
                if message:
                    try:
                        # Parse message (assuming it's JSON)
                        import json

                        if isinstance(message, bytes):
                            message = message.decode("utf-8")
                        opportunity = json.loads(message)

                        logger.debug(f"Received opportunity: {opportunity}")
                        self._last_activity = time.time()
                        self._error_backoff = 1  # Reset backoff on success
                        return [opportunity]
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        return []

                # No opportunity but no error either
                return []

        except Exception as e:
            logger.error(f"Error in next_batch: {e}")
            # Use exponential backoff for errors
            time.sleep(min(self._error_backoff, self._max_backoff))
            self._error_backoff = min(self._error_backoff * 2, self._max_backoff)
            return []

    def snapshot(self) -> None:
        logger.debug("Snapshot requested for RedisExecutionPartition (returning None)")
        return None


class RedisExecutionSource(FixedPartitionedSource):
    def __init__(self, strategy_name=None):
        """
        Create a Redis source for trade opportunities.

        Parameters:
            strategy_name: Optional strategy name to filter opportunities
        """
        self.strategy_name = strategy_name
        logger.info(f"Created RedisExecutionSource for strategy: {strategy_name or 'all'}")

    def list_parts(self):
        # Simple single partition for execution source
        parts = ["execution_part"]
        logger.info(f"List of partition keys: {parts}")
        return parts

    def build_part(self, step_id, for_key, _resume_state):
        logger.info(f"Building partition for key: {for_key}")
        return RedisExecutionPartition(self.strategy_name)
