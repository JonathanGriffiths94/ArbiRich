import json
import logging
from typing import List, Optional

from redis.exceptions import RedisError

from src.arbirich.constants import TRADE_EXECUTION_KEY_PREFIX, TRADE_EXECUTIONS_CHANNEL
from src.arbirich.models.models import TradeExecution

logger = logging.getLogger(__name__)


class TradeExecutionRepository:
    """Repository for managing trade executions."""

    def __init__(self, redis_client):
        self.redis_client = redis_client

    def save(self, execution: TradeExecution) -> bool:
        """Store a trade execution in Redis."""
        try:
            key = f"{TRADE_EXECUTION_KEY_PREFIX}:{execution.id}"

            data = execution.model_dump()

            self.redis_client.set(key, json.dumps(data))

            self.redis_client.expire(key, 300)

            return True
        except RedisError as e:
            logger.error(f"Error storing trade execution {execution.id}: {e}")
            return False

    def get(self, execution_id: str) -> Optional[TradeExecution]:
        """Retrieve a trade execution by ID."""
        try:
            key = f"{TRADE_EXECUTION_KEY_PREFIX}:{execution_id}"
            data = self.redis_client.get(key)

            if not data:
                return None

            # Create and return TradeExecution object
            return TradeExecution(**json.loads(data))
        except RedisError as e:
            logger.error(f"Error retrieving trade execution {execution_id}: {e}")
            return None

    def publish(self, execution: TradeExecution) -> int:
        """Publish a trade execution to the appropriate channel."""
        try:
            # Create message with type field for better handling
            message = {"event": "new_execution", "data": execution.model_dump()}

            result = self.redis_client.publish(TRADE_EXECUTIONS_CHANNEL, json.dumps(message))
            return result
        except RedisError as e:
            logger.error(f"Error publishing trade execution {execution.id}: {e}")
            return 0

    def get_recent(self, count: int = 50) -> List[TradeExecution]:
        """Get the most recent trade executions."""
        try:
            pattern = f"{TRADE_EXECUTION_KEY_PREFIX}:*"
            keys = self.redis_client.keys(pattern)

            executions = []

            # Limit to the requested count
            for key in keys[:count]:
                data = self.redis_client.get(key)
                if data:
                    try:
                        execution_dict = json.loads(data)
                        execution = TradeExecution(**execution_dict)
                        executions.append(execution)
                    except Exception as e:
                        logger.error(f"Error parsing execution data: {e}")

            return executions
        except RedisError as e:
            logger.error(f"Error retrieving recent executions: {e}")
            return []

    def get_by_opportunity_id(self, opportunity_id: str) -> List[TradeExecution]:
        """Retrieve executions related to a specific opportunity."""
        try:
            # Since we don't have an index, we need to scan all executions
            # In a production system, you might want to implement a secondary index
            pattern = f"{TRADE_EXECUTION_KEY_PREFIX}:*"
            keys = self.redis_client.keys(pattern)

            executions = []

            for key in keys:
                data = self.redis_client.get(key)
                if data:
                    try:
                        execution_dict = json.loads(data)
                        if execution_dict.get("opportunity_id") == opportunity_id:
                            execution = TradeExecution(**execution_dict)
                            executions.append(execution)
                    except Exception as e:
                        logger.error(f"Error parsing execution data: {e}")

            return executions
        except RedisError as e:
            logger.error(f"Error retrieving executions for opportunity {opportunity_id}: {e}")
            return []
