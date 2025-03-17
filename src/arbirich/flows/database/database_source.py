import json
import logging
import time

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

from arbirich.services.redis.redis_service import RedisService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

redis_client = RedisService()


class RedisDatabasePartition(StatefulSourcePartition):
    def __init__(self, channel, stop_event=None):
        logger.info(f"Initializing RedisDatabasePartition for channel: {channel}")
        self.channel = channel
        self.pubsub = redis_client.client.pubsub(ignore_subscribe_messages=True)
        self.pubsub.subscribe(channel)
        logger.info(f"Subscribed to Redis channel: {channel}")
        self._running = True
        self._last_activity = time.time()
        self._next_log_time = time.time() + 60  # First log after 60 seconds
        self._stop_event = stop_event

    def next_batch(self) -> list:
        # Check for stop event first
        if self._stop_event and self._stop_event.is_set():
            logger.info(f"Stop event detected for channel {self.channel}, returning empty batch")
            self._running = False
            return []

        current_time = time.time()

        # Periodic health check
        if current_time - self._last_activity > 60:  # 1 minute timeout
            logger.info(f"Performing health check for channel: {self.channel}")
            self._last_activity = current_time

            # Check Redis connection
            if not redis_client.is_healthy():
                logger.warning(f"Redis connection appears unhealthy for channel: {self.channel}")
                # Attempt to reconnect
                try:
                    self.pubsub.close()
                    self.pubsub = redis_client.client.pubsub(ignore_subscribe_messages=True)
                    self.pubsub.subscribe(self.channel)
                    logger.info(f"Resubscribed to Redis channel: {self.channel}")
                except Exception as e:
                    logger.error(f"Error reconnecting to Redis: {e}")
                return []

        # Periodic logging to show we're alive
        if current_time > self._next_log_time:
            logger.debug(f"Database partition for {self.channel} is active and waiting for messages")
            self._next_log_time = current_time + 300  # Log every 5 minutes

        if not self._running:
            logger.info(f"Partition for {self.channel} marked as not running, stopping")
            return []

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
        logger.debug(f"Snapshot requested for RedisDatabasePartition {self.channel} (returning None)")
        return None

    def __del__(self):
        try:
            logger.info(f"Closing pubsub for channel: {self.channel}")
            self.pubsub.unsubscribe()
            self.pubsub.close()
        except Exception as e:
            logger.error(f"Error closing pubsub: {e}")


class RedisDatabaseSource(FixedPartitionedSource):
    def __init__(self, channels, stop_event=None):
        self.channels = channels
        self.stop_event = stop_event

    def list_parts(self):
        parts = self.channels
        logger.info(f"Database source partitions: {parts}")
        return parts

    def build_part(self, step_id, for_key, _resume_state):
        logger.info(f"Building database partition for channel: {for_key}")
        if "trade_executions" in for_key:
            logger.info(f"*** This is an EXECUTION channel: {for_key} ***")
        return RedisDatabasePartition(for_key, stop_event=self.stop_event)
