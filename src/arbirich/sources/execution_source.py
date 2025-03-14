import logging
import time

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

from src.arbirich.services.redis_service import RedisService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

redis_client = RedisService()


class RedisExecutionPartition(StatefulSourcePartition):
    def __init__(self):
        logger.debug("Initializing RedisOpportunityPartition.")
        self.gen = redis_client.subscribe_to_trade_opportunities(lambda opp: logger.debug(f"Received: {opp}"))
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
        # Return None for now (or implement snapshot logic if needed)
        logger.debug("Snapshot requested for RedisOpportunityPartition (returning None).")
        return None


class RedisExecutionSource(FixedPartitionedSource):
    def list_parts(self):
        parts = ["1"]
        logger.info(f"List of partition keys: {parts}")
        return parts

    def build_part(self, step_id, for_key, _resume_state):
        logger.info(f"Building partition for key: {for_key}")
        return RedisExecutionPartition()
