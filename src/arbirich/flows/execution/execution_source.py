import logging
import threading
import time
from threading import Lock

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

from src.arbirich.flows.arbitrage.arbitrage_source import get_shared_redis_client
from src.arbirich.services.redis_service import RedisService

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Shared stop event for coordinating shutdown
_shared_stop_event = threading.Event()


def set_stop_event():
    """Signal all execution partitions to stop."""
    global _shared_stop_event
    _shared_stop_event.set()
    logger.info("Stop event set for all execution partitions")


class RedisExecutionPartition(StatefulSourcePartition):
    def __init__(self):
        logger.info("Initializing RedisExecutionPartition")
        # Use shared Redis client instead of creating a new one
        self.redis_client = get_shared_redis_client()
        self._running = True
        self._last_activity = time.time()
        self._lock = Lock()
        self._error_backoff = 1  # Initial backoff in seconds
        self._max_backoff = 30  # Maximum backoff in seconds

        # Subscribe to opportunities channel and verify
        logger.info("Subscribing to trade opportunities channel")
        self.redis_client.subscribe_to_trade_opportunities()

        # Verify subscription is active by checking Redis pubsub channels
        try:
            channels = self.redis_client.client.pubsub_channels()
            channels = [ch.decode("utf-8") if isinstance(ch, bytes) else ch for ch in channels]
            if "trade_opportunities" in channels:
                logger.info("Successfully subscribed to trade_opportunities channel")
            else:
                logger.warning("trade_opportunities channel not found in active channels!")
                logger.info("Creating explicit subscription")
                # Create a persistent subscription
                self.pubsub = self.redis_client.client.pubsub()
                self.pubsub.subscribe("trade_opportunities")
                self.pubsub.subscribe("trade_opportunities:basic_arbitrage")
        except Exception as e:
            logger.error(f"Error verifying subscription: {e}")

        logger.info("RedisExecutionPartition initialization complete")

    def next_batch(self) -> list:
        try:
            # First check if stop has been requested
            if _shared_stop_event.is_set():
                logger.info("Stop event detected in execution partition")
                return []

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
                        self.redis_client.subscribe_to_trade_opportunities()

                if not self._running:
                    logger.info("Partition marked as not running, stopping")
                    return []

                # Get opportunity from Redis
                opportunity = self.redis_client.get_opportunity()
                if opportunity:
                    logger.debug(f"Received opportunity: {opportunity}")
                    self._last_activity = time.time()
                    self._error_backoff = 1  # Reset backoff on success
                    return [opportunity]

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
    def list_parts(self):
        # Simple single partition for execution source
        parts = ["execution_part"]
        logger.info(f"List of execution partitions: {parts}")
        return parts

    def build_part(self, step_id, for_key, _resume_state):
        logger.info(f"Building execution partition for key: {for_key}")
        return RedisExecutionPartition()
