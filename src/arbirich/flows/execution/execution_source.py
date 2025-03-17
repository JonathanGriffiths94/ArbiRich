import json
import logging
import threading
import time
from threading import Lock

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

from src.arbirich.config import STRATEGIES
from src.arbirich.constants import TRADE_OPPORTUNITIES_CHANNEL
from src.arbirich.flows.arbitrage.arbitrage_source import get_shared_redis_client
from src.arbirich.services.redis.redis_service import RedisService

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
    def __init__(self, strategy_name=None):
        logger.info(f"Initializing RedisExecutionPartition for strategy: {strategy_name or 'all'}")
        self.strategy_name = strategy_name
        # Use shared Redis client instead of creating a new one
        self.redis_client = get_shared_redis_client()
        self._running = True
        self._last_activity = time.time()
        self._lock = Lock()
        self._error_backoff = 1  # Initial backoff in seconds
        self._max_backoff = 30  # Maximum backoff in seconds

        # Determine which channels to subscribe to
        self.channels_to_check = []

        if strategy_name:
            # If strategy is specified, only check that strategy's channel
            strategy_channel = f"{TRADE_OPPORTUNITIES_CHANNEL}:{strategy_name}"
            self.channels_to_check.append(strategy_channel)
            logger.info(f"Will check strategy-specific channel: {strategy_channel}")
        else:
            # Otherwise check all strategy channels
            for strategy in STRATEGIES.keys():
                strategy_channel = f"{TRADE_OPPORTUNITIES_CHANNEL}:{strategy}"
                self.channels_to_check.append(strategy_channel)
                logger.info(f"Will check strategy channel: {strategy_channel}")

            # Also check main channel as fallback
            self.channels_to_check.append(TRADE_OPPORTUNITIES_CHANNEL)
            logger.info(f"Will check main channel: {TRADE_OPPORTUNITIES_CHANNEL}")

        # Create explicit subscriptions for all channels
        self.pubsub = self.redis_client.client.pubsub(ignore_subscribe_messages=True)
        for channel in self.channels_to_check:
            self.pubsub.subscribe(channel)
            logger.info(f"Explicitly subscribed to: {channel}")

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
                    logger.info("Performing periodic health check on Redis connection")
                    self._last_activity = current_time

                    # Check connection health and recreate if necessary
                    if not self.redis_client.is_healthy():
                        logger.warning("Redis connection appears unhealthy, reconnecting")
                        try:
                            self.redis_client.close()
                        except Exception as e:
                            logger.warning(f"Error closing Redis connection: {e}")

                        self.redis_client = RedisService()
                        # Recreate subscriptions
                        self.pubsub = self.redis_client.client.pubsub(ignore_subscribe_messages=True)
                        for channel in self.channels_to_check:
                            self.pubsub.subscribe(channel)
                            logger.info(f"Resubscribed to: {channel}")

                if not self._running:
                    logger.info("Partition marked as not running, stopping")
                    return []

                # Check for messages from our pubsub client directly
                message = self.pubsub.get_message(timeout=0.01)
                opportunity = None

                if message and message.get("type") == "message":
                    channel = message.get("channel")
                    if isinstance(channel, bytes):
                        channel = channel.decode("utf-8")

                    data = message.get("data")
                    if isinstance(data, bytes):
                        data = data.decode("utf-8")

                    logger.info(f"Received message from channel: {channel}")
                    try:
                        opportunity = json.loads(data)
                        logger.info(f"Parsed opportunity: {opportunity.get('id', '(no ID)')}")
                    except json.JSONDecodeError:
                        logger.error(f"Error decoding message data: {data}")

                if opportunity:
                    logger.info(
                        f"Processing opportunity: {opportunity.get('id', '(no ID)')} for pair {opportunity.get('pair')}"
                    )
                    self._last_activity = time.time()
                    self._error_backoff = 1  # Reset backoff on success
                    return [opportunity]
                else:
                    # Periodically log that we're waiting for opportunities
                    if current_time - self._last_activity > 120:  # Every 2 minutes
                        logger.info("Waiting for opportunities...")
                        self._last_activity = current_time

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
        self.strategy_name = strategy_name
        logger.info(f"Created RedisExecutionSource for strategy: {strategy_name or 'all'}")

    def list_parts(self):
        # Simple single partition for execution source
        parts = ["execution_part"]
        logger.info(f"List of execution partitions: {parts}")
        return parts

    def build_part(self, step_id, for_key, _resume_state):
        logger.info(f"Building execution partition for key: {for_key}")
        return RedisExecutionPartition(self.strategy_name)
