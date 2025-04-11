import json
import logging
import threading
import time
import weakref

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

from src.arbirich.core.system_state import is_system_shutting_down
from src.arbirich.services.redis.redis_service import get_shared_redis_client

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Track active partitions using a WeakSet to avoid memory leaks
_active_partitions = weakref.WeakSet()
_partition_lock = threading.RLock()

# Global shared Redis client
_shared_redis_client = None


def reset_shared_redis_client():
    """
    Reset the shared Redis client for the reporting module.
    """
    global _shared_redis_client

    # First terminate all partitions to ensure they release Redis resources
    terminate_all_partitions()

    # Then close the shared client
    if _shared_redis_client:
        try:
            _shared_redis_client.close()
            _shared_redis_client = None
            logger.info("Shared Redis client for reporting module reset successfully")
        except Exception as e:
            logger.error(f"Error resetting shared Redis client for reporting module: {e}")


def terminate_all_partitions():
    """Terminate all active reporting partitions."""
    with _partition_lock:
        partition_count = len(_active_partitions)
        logger.info(f"Terminating {partition_count} active reporting partitions")

        # Create a list to avoid modifying during iteration
        for partition in list(_active_partitions):
            try:
                if hasattr(partition, "pubsub") and partition.pubsub:
                    try:
                        partition.pubsub.unsubscribe()
                        partition.pubsub.close()
                        logger.info(f"Closed pubsub for partition: {getattr(partition, 'channel', 'unknown')}")
                    except Exception as e:
                        logger.error(f"Error closing pubsub for partition: {e}")

                # Mark as not running
                if hasattr(partition, "_running"):
                    partition._running = False
                    logger.info(f"Marked partition as not running: {getattr(partition, 'channel', 'unknown')}")

                # Remove from active set
                _active_partitions.remove(partition)

            except Exception as e:
                logger.error(f"Error terminating reporting partition: {e}")

        # Explicitly clear the active partitions set
        _active_partitions.clear()
        logger.info("All reporting partitions terminated")


class RedisReportingPartition(StatefulSourcePartition):
    def __init__(self, channel, stop_event=None, strategy_name=None):
        self.channel = channel
        self.strategy_name = strategy_name
        self.stop_event = stop_event

        # Initialize timing attributes
        self._last_activity = time.time()
        self._next_log_time = time.time() + 300  # First log after 5 minutes

        # Explicitly check if this channel already has an active partition
        existing_channel = False
        with _partition_lock:
            for partition in _active_partitions:
                if hasattr(partition, "channel") and partition.channel == channel:
                    existing_channel = True
                    break

        if existing_channel:
            # If a partition for this channel already exists, mark as stopped immediately
            logger.warning(f"Duplicate partition creation for {channel}, marking as inactive")
            self._running = False
        else:
            # Normal initialization
            self._running = True
            self.redis_client = get_shared_redis_client()
            self.pubsub = self.redis_client.client.pubsub()
            self.pubsub.subscribe(self.channel)
            logger.info(f"Subscribed to Redis channel: {channel}")

            # Add to global registry
            with _partition_lock:
                _active_partitions.add(self)

    def next_batch(self):
        # Check for system shutdown at the very beginning
        if is_system_shutting_down():
            if self._running:  # Only log once when transitioning from running to stopped
                logger.info(f"System shutting down, stopping partition for {self.channel}")
                try:
                    # Clean up Redis resources
                    self.pubsub.unsubscribe()
                    self.pubsub.close()
                    logger.info(f"Unsubscribed from {self.channel} due to system shutdown")
                except Exception as e:
                    logger.error(f"Error closing Redis connection during shutdown: {e}")
                self._running = False
            return []

        # Standard check if we're already not running
        if not self._running:
            return []

        # Check for stop condition
        if self.stop_event and self.stop_event.is_set():
            # Only log once per partition
            logger.info(f"Partition for {self.channel} marked as not running, stopping")
            self._running = False

            try:
                # Clean up Redis resources
                self.pubsub.unsubscribe()
                self.pubsub.close()
            except Exception as e:
                logger.error(f"Error closing Redis connection: {e}")

            # Remove from registry
            with _partition_lock:
                if self in _active_partitions:
                    _active_partitions.remove(self)
            return []

        current_time = time.time()

        # Periodic health check
        if current_time - self._last_activity > 60:  # 1 minute timeout
            logger.info(f"Performing health check for channel: {self.channel}")
            self._last_activity = current_time

            # Check Redis connection
            if not self.redis_client.is_healthy():
                logger.warning(f"Redis connection appears unhealthy for channel: {self.channel}")
                # Attempt to reconnect
                try:
                    self.pubsub.close()
                    self.pubsub = self.redis_client.client.pubsub(ignore_subscribe_messages=True)
                    self.pubsub.subscribe(self.channel)
                    logger.info(f"Resubscribed to Redis channel: {self.channel}")
                except Exception as e:
                    logger.error(f"Error reconnecting to Redis: {e}")
                return []

        # Periodic logging to show we're alive
        if current_time > self._next_log_time:
            logger.debug(f"Reporting partition for {self.channel} is active and waiting for messages")
            self._next_log_time = current_time + 300  # Log every 5 minutes

        try:
            # Get message from Redis
            message = self.pubsub.get_message(timeout=0.01)

            if message and message.get("type") == "message":
                data = message.get("data")
                if isinstance(data, bytes):
                    data = data.decode("utf-8")

                logger.info(f"Received message from {self.channel}: {data[:50]}...")
                self._last_activity = current_time

                # Parse JSON and return
                try:
                    json_data = json.loads(data)
                    return [json_data]
                except json.JSONDecodeError as e:
                    logger.error(f"Error parsing JSON from {self.channel}: {e}")
                    return []

            return []  # No message or not a proper message

        except Exception as e:
            logger.error(f"Error in next_batch for {self.channel}: {e}")
            return []

    def snapshot(self) -> None:
        logger.debug(f"Snapshot requested for RedisReportingPartition {self.channel} (returning None)")
        return None

    def __del__(self):
        try:
            logger.info(f"Closing pubsub for channel: {self.channel}")
            self.pubsub.unsubscribe()
            self.pubsub.close()
        except Exception as e:
            logger.error(f"Error closing pubsub: {e}")


class RedisReportingSource(FixedPartitionedSource):
    def __init__(self, channels, stop_event=None):
        self.channels = channels
        self.stop_event = stop_event

    def list_parts(self):
        parts = self.channels
        logger.info(f"Reporting source partitions: {parts}")
        return parts

    def build_part(self, step_id, for_key, _resume_state):
        logger.info(f"Building reporting partition for channel: {for_key}")
        if "trade_executions" in for_key:
            logger.info(f"*** This is an EXECUTION channel: {for_key} ***")
        return RedisReportingPartition(for_key, stop_event=self.stop_event)
