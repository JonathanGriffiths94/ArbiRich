import json
import logging
import threading
from typing import Any, Dict, Union

from src.arbirich.constants import ORDER_BOOK_CHANNEL, TRADE_EXECUTIONS_CHANNEL, TRADE_OPPORTUNITIES_CHANNEL
from src.arbirich.models.models import TradeExecution, TradeOpportunity

logger = logging.getLogger(__name__)

# Global shared instance for the channel manager
_channel_manager = None
_channel_manager_lock = threading.RLock()


def get_channel_manager():
    """Get or create a shared channel manager instance"""
    global _channel_manager
    with _channel_manager_lock:
        if _channel_manager is None:
            from src.arbirich.services.redis.redis_service import get_shared_redis_client

            redis_client = get_shared_redis_client()
            if redis_client:
                _channel_manager = RedisChannelManager(redis_client)
            else:
                logger.error("Failed to get Redis client for channel manager")
                return None
        return _channel_manager


def reset_channel_manager():
    """Reset the shared channel manager instance"""
    global _channel_manager
    with _channel_manager_lock:
        _channel_manager = None
        logger.info("Channel manager reset")


class RedisChannelManager:
    """
    Manages Redis channel publication and subscription for different message types
    with support for strategy-specific channels.
    """

    def __init__(self, redis_service):
        """Initialize with a Redis service instance"""
        self.redis = redis_service
        self.ORDER_BOOK = ORDER_BOOK_CHANNEL
        self.TRADE_OPPORTUNITIES = TRADE_OPPORTUNITIES_CHANNEL
        self.TRADE_EXECUTIONS = TRADE_EXECUTIONS_CHANNEL
        logger.info("RedisChannelManager initialized")

    def get_opportunity_channel(self, strategy_name=None):
        """Get the channel name for trade opportunities"""
        if strategy_name:
            return f"{self.TRADE_OPPORTUNITIES}:{strategy_name}"
        return self.TRADE_OPPORTUNITIES

    def get_execution_channel(self, strategy_name=None):
        """Get the channel name for trade executions"""
        if strategy_name:
            return f"{self.TRADE_EXECUTIONS}:{strategy_name}"
        return self.TRADE_EXECUTIONS

    def publish_opportunity(self, opportunity: Union[TradeOpportunity, Dict[str, Any]], strategy_name=None) -> int:
        """
        Publish a trade opportunity to appropriate channels.

        Args:
            opportunity: The opportunity to publish
            strategy_name: Optional strategy name to use for channel selection
                           If None, will extract from opportunity

        Returns:
            Total number of subscribers reached
        """
        total_subscribers = 0

        # Extract strategy from opportunity if not provided
        if not strategy_name:
            if isinstance(opportunity, dict):
                strategy_name = opportunity.get("strategy")
            else:
                strategy_name = getattr(opportunity, "strategy", None)

        # Convert to JSON for publishing
        if isinstance(opportunity, TradeOpportunity):
            if hasattr(opportunity, "model_dump_json"):
                data_json = opportunity.model_dump_json()
            else:
                data_json = json.dumps(opportunity.dict())
        elif isinstance(opportunity, dict):
            data_json = json.dumps(opportunity)
        else:
            logger.error(f"Unsupported opportunity type: {type(opportunity)}")
            return 0

        # Always publish to main channel
        try:
            main_subscribers = self.redis.client.publish(self.TRADE_OPPORTUNITIES, data_json)
            total_subscribers += main_subscribers
            logger.debug(f"Published to main opportunity channel: {main_subscribers} subscribers")
        except Exception as e:
            logger.error(f"Error publishing to main opportunity channel: {e}")

        # Also publish to strategy-specific channel if strategy is provided
        if strategy_name:
            try:
                strategy_channel = f"{self.TRADE_OPPORTUNITIES}:{strategy_name}"
                strategy_subscribers = self.redis.client.publish(strategy_channel, data_json)
                total_subscribers += strategy_subscribers
                logger.debug(f"Published to strategy channel {strategy_channel}: {strategy_subscribers} subscribers")
            except Exception as e:
                logger.error(f"Error publishing to strategy opportunity channel: {e}")

        return total_subscribers

    def publish_execution(self, execution: Union[TradeExecution, Dict[str, Any]]) -> int:
        """
        Publish a trade execution to appropriate channels.

        Args:
            execution: The execution to publish

        Returns:
            Total number of subscribers reached
        """
        total_subscribers = 0

        # Extract strategy from execution
        if isinstance(execution, dict):
            strategy_name = execution.get("strategy")
        else:
            strategy_name = getattr(execution, "strategy", None)

        # Convert to JSON for publishing
        if isinstance(execution, TradeExecution):
            if hasattr(execution, "model_dump_json"):
                data_json = execution.model_dump_json()
            else:
                data_json = json.dumps(execution.dict())
        elif isinstance(execution, dict):
            data_json = json.dumps(execution)
        else:
            logger.error(f"Unsupported execution type: {type(execution)}")
            return 0

        # Always publish to main channel
        try:
            main_subscribers = self.redis.client.publish(self.TRADE_EXECUTIONS, data_json)
            total_subscribers += main_subscribers
            logger.debug(f"Published to main execution channel: {main_subscribers} subscribers")
        except Exception as e:
            logger.error(f"Error publishing to main execution channel: {e}")

        # Also publish to strategy-specific channel if strategy is provided
        if strategy_name:
            try:
                strategy_channel = f"{self.TRADE_EXECUTIONS}:{strategy_name}"
                strategy_subscribers = self.redis.client.publish(strategy_channel, data_json)
                total_subscribers += strategy_subscribers
                logger.debug(f"Published to strategy execution channel: {strategy_subscribers} subscribers")
            except Exception as e:
                logger.error(f"Error publishing to strategy execution channel: {e}")

        return total_subscribers
