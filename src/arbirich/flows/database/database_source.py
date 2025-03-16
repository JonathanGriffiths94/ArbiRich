import logging
import time

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

from src.arbirich.services.redis_service import RedisService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

redis_client = RedisService()


class RedisDatabasePartition(StatefulSourcePartition):
    def __init__(self, channel):
        logger.debug(f"Initializing RedisDatabasePartition for channel: {channel}.")
        self.gen = redis_client.subscribe(channel, lambda msg: logger.debug(f"Received: {msg}"))
        self._running = True
        self._last_activity = time.time()

    def next_batch(self) -> list:
        if time.time() - self._last_activity > 60:  # 1 minute timeout
            logger.warning("Safety timeout reached, checking Redis connection")
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
                logger.debug(f"next_batch obtained message: {result}")
                return [result]
            return []
        except StopIteration:
            logger.info("Generator exhausted")
            self._running = False
            return []
        except Exception as e:
            logger.error(f"Error in next_batch: {e}")
            return []

    def snapshot(self) -> None:
        logger.debug("Snapshot requested for RedisDatabasePartition (returning None).")
        return None


class RedisDatabaseSource(FixedPartitionedSource):
    def __init__(self, channels):
        self.channels = channels

    def list_parts(self):
        parts = self.channels
        logger.info(f"List of partition keys: {parts}")
        return parts

    def build_part(self, step_id, for_key, _resume_state):
        logger.info(f"Building partition for key: {for_key}")
        return RedisDatabasePartition(for_key)
