import json
import logging
import threading
import time
from threading import Lock

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

from arbirich.core.state.system_state import is_system_shutting_down, mark_component_notified, set_stop_event
from src.arbirich.constants import TRADE_OPPORTUNITIES_CHANNEL
from src.arbirich.services.redis.redis_service import get_shared_redis_client

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Add this shared client variable at module level
_shared_redis_client = None


def reset_shared_redis_client() -> bool:
    """
    Reset the shared Redis client for execution flows.

    Returns:
        bool: True if reset was successful, False otherwise
    """
    global _shared_redis_client

    try:
        logger.info("Resetting shared Redis client for execution flows")

        # Close existing pubsub if any instances exist
        for partition in RedisExecutionSource.__dict__.get("_partitions", {}).values():
            if hasattr(partition, "pubsub") and partition.pubsub:
                try:
                    partition.pubsub.unsubscribe()
                    partition.pubsub.close()
                    logger.info("Closed execution partition pubsub")
                except Exception as e:
                    logger.warning(f"Error closing execution partition pubsub: {e}")

        # Reset the shared client
        if _shared_redis_client is not None:
            try:
                _shared_redis_client.close()
                logger.info("Closed execution shared Redis client")
            except Exception as e:
                logger.warning(f"Error closing execution shared Redis client: {e}")

            _shared_redis_client = None

        # Reset any Redis clients in existing partitions
        reset_count = 0
        for partition in RedisExecutionSource.__dict__.get("_partitions", {}).values():
            if hasattr(partition, "redis_client"):
                try:
                    if partition.redis_client:
                        partition.redis_client = None
                        reset_count += 1
                except Exception as e:
                    logger.warning(f"Error resetting partition Redis client: {e}")

        if reset_count > 0:
            logger.info(f"Reset {reset_count} Redis clients in execution partitions")

        return True
    except Exception as e:
        logger.error(f"Error in reset_shared_redis_client for execution: {e}")
        return False


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
        self.redis_client = get_shared_redis_client()
        self._running = True  # Always start in running state
        self._last_activity = time.time()
        self._lock = Lock()
        self._error_backoff = 1  # Initial backoff in seconds
        self._max_backoff = 30  # Maximum backoff in seconds
        self._initialized = False  # Track initialization state

        self.channels_to_check = []

        from src.arbirich.constants import TRADE_OPPORTUNITIES_CHANNEL

        if strategy_name:
            strategy_channel = f"{TRADE_OPPORTUNITIES_CHANNEL}:{strategy_name}"
            self.channels_to_check.append(strategy_channel)
            logger.info(f"Will check only strategy-specific channel: {strategy_channel}")
        else:
            # If no strategy specified, get all configured strategies
            from src.arbirich.config.config import STRATEGIES

            for strat_name in STRATEGIES.keys():
                strategy_channel = f"{TRADE_OPPORTUNITIES_CHANNEL}:{strat_name}"
                self.channels_to_check.append(strategy_channel)
                logger.info(f"Will check strategy channel: {strategy_channel}")

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
        # Don't convert None to default - keep it as is
        self.strategy_name = strategy_name
        self.stop_event = stop_event
        self.logger = logging.getLogger(__name__)

        # Set up the channel to subscribe to
        if self.strategy_name is None:
            self.logger.error("Strategy name is None, cannot subscribe to strategy-specific channel")
            # Provide an empty channel to avoid failures
            self.channel = ""
        else:
            # For named strategies, use their specific channel
            self.channel = f"{TRADE_OPPORTUNITIES_CHANNEL}:{self.strategy_name}"

        self.logger.info(f"RedisExecutionSource initialized for channel: {self.channel or 'NONE'}")

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
