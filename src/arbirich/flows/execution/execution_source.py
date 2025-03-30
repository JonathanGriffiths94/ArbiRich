import json
import logging
import threading
import time
from threading import Lock

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

from src.arbirich.config.config import STRATEGIES
from src.arbirich.constants import TRADE_OPPORTUNITIES_CHANNEL
from src.arbirich.core.system_state import is_system_shutting_down, mark_component_notified, set_stop_event
from src.arbirich.services.redis.redis_service import get_shared_redis_client

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class RedisExecutionPartition(StatefulSourcePartition):
    def __init__(self, strategy_name=None, stop_event=None):
        """
        Initialize a Redis execution partition.

        Args:
            strategy_name: Optional filter for a specific strategy
            stop_event: Optional threading.Event for stopping the partition
        """
        logger.info(f"Initializing RedisExecutionPartition for strategy: {strategy_name or 'all'}")
        self.strategy_name = strategy_name
        self.stop_event = stop_event  # Store the passed stop event
        # Use shared Redis client instead of creating a new one
        self.redis_client = get_shared_redis_client()
        self._running = True  # Always start in running state
        self._last_activity = time.time()
        self._lock = Lock()
        self._error_backoff = 1  # Initial backoff in seconds
        self._max_backoff = 30  # Maximum backoff in seconds
        self._initialized = False  # Track initialization state

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

        self._initialized = True
        logger.info("RedisExecutionPartition initialization complete")

    def next_batch(self) -> list:
        try:
            # Only check for shutdown if we've been initialized
            # This prevents premature shutdown during restart
            if self._initialized and is_system_shutting_down():
                component_id = f"execution:{self.strategy_name or 'all'}"
                if mark_component_notified("execution", component_id):
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

            with self._lock:
                # After a restart, make sure we're in running state if system isn't shutting down
                if not self._running and not is_system_shutting_down():
                    logger.info("Restoring running state after system restart")
                    self._running = True

                # Check if we should perform periodic health check
                current_time = time.time()
                if current_time - self._last_activity > 30:  # 30 seconds timeout
                    logger.info("Performing periodic health check on Redis connection")
                    self._last_activity = current_time

                    # Check connection health and recreate if necessary
                    if not self.redis_client.is_healthy():
                        logger.warning("Redis connection appears unhealthy, reconnecting")
                        try:
                            self.pubsub.unsubscribe()
                            self.pubsub.close()
                        except Exception as e:
                            logger.warning(f"Error closing Redis pubsub: {e}")

                        self.redis_client = get_shared_redis_client()
                        # Recreate subscriptions
                        self.pubsub = self.redis_client.client.pubsub(ignore_subscribe_messages=True)
                        for channel in self.channels_to_check:
                            self.pubsub.subscribe(channel)
                            logger.info(f"Resubscribed to: {channel}")
                            # Fix missing closing parenthesis on the next line
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
    """A source that reads trade opportunities from Redis."""

    def __init__(self, strategy_name=None, stop_event=None):
        """
        Initialize the Redis execution source.

        Args:
            strategy_name: Optional filter for a specific strategy
            stop_event: Optional threading.Event for signaling source to stop
        """
        self.strategy_name = strategy_name
        self.stop_event = stop_event
        self.logger = logging.getLogger(__name__)
        self.channel = TRADE_OPPORTUNITIES_CHANNEL
        if strategy_name:
            self.channel = f"{self.channel}:{strategy_name}"
        self.logger.info(f"RedisExecutionSource initialized for channel: {self.channel}")

        # Monitor the stop_event and propagate it to the system-wide shutdown flag
        if self.stop_event:

            def monitor_stop():
                while not self.stop_event.is_set():
                    time.sleep(0.1)
                set_stop_event()  # Signal system-wide shutdown

            monitor_thread = threading.Thread(target=monitor_stop, daemon=True)
            monitor_thread.start()

    def list_parts(self):
        """List available partitions."""
        return [self.channel]

    def build_part(self, step_id, for_key, _resume_state):
        """Create a partition for the given key."""
        logger.info(f"Building execution partition for key: {for_key}")
        # Pass the stop_event to the partition
        return RedisExecutionPartition(self.strategy_name, stop_event=self.stop_event)


import logging

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
                logger.debug("Created new Redis client for execution source")
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
                logger.info("Closed shared Redis client in execution source")
            except Exception as e:
                logger.warning(f"Error closing Redis client: {e}")

            # Sleep briefly to allow proper client recreation
            time.sleep(0.5)
            _shared_redis_client = None
