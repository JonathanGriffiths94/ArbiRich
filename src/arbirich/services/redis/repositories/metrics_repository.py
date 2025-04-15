import logging
import time
from typing import Any, Dict, List

from redis.exceptions import RedisError

logger = logging.getLogger(__name__)


class MetricsRepository:
    """Repository for managing performance metrics."""

    def __init__(self, redis_client):
        self.redis_client = redis_client

    def record_metric(self, metric_name: str, value: float) -> bool:
        """Record a performance metric with timestamp."""
        try:
            timestamp = int(time.time())
            self.redis_client.zadd(f"metrics:{metric_name}", {timestamp: value})
            return True
        except RedisError as e:
            logger.error(f"Error recording metric {metric_name}: {e}")
            return False

    def get_recent_metrics(self, metric_name: str, count: int = 60) -> List[Dict[str, Any]]:
        """Get recent metrics for a given name."""
        try:
            # Get timestamp-value pairs
            raw_metrics = self.redis_client.zrevrange(f"metrics:{metric_name}", 0, count - 1, withscores=True)

            # Convert to list of dicts
            metrics = [{"timestamp": int(timestamp), "value": value} for value, timestamp in raw_metrics]

            return metrics
        except RedisError as e:
            logger.error(f"Error retrieving metrics for {metric_name}: {e}")
            return []
