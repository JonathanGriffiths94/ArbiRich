import logging
from typing import Dict, List, Optional

from src.arbirich.models.models import TradeExecution, TradeOpportunity
from src.arbirich.services.redis_service import RedisService
from src.arbirich.utils.strategy_manager import StrategyManager

logger = logging.getLogger(__name__)


class RedisChannelManager:
    """
    Manages Redis channels for different strategies and message types.
    This class helps organize channel names and message routing.
    """

    def __init__(self, redis_service: Optional[RedisService] = None):
        """Initialize with an existing RedisService or create a new one"""
        self.redis = redis_service or RedisService()

    def get_opportunity_channel(self, strategy_name: Optional[str] = None) -> str:
        """Get the appropriate channel name for trade opportunities"""
        return f"trade_opportunities:{strategy_name}" if strategy_name else "trade_opportunities"

    def get_execution_channel(self, strategy_name: Optional[str] = None) -> str:
        """Get the appropriate channel name for trade executions"""
        return f"trade_executions:{strategy_name}" if strategy_name else "trade_executions"

    def get_debug_channel(self, strategy_name: Optional[str] = None) -> str:
        """Get the appropriate channel name for debug messages"""
        return f"debug:{strategy_name}" if strategy_name else "debug"

    def publish_opportunity(self, opportunity: TradeOpportunity) -> bool:
        """
        Publish a trade opportunity to the appropriate channel
        based on its strategy
        """
        channel = self.get_opportunity_channel(opportunity.strategy)
        try:
            self.redis.client.publish(channel, opportunity.model_dump_json())
            logger.debug(f"Published opportunity {opportunity.id} to channel {channel}")
            return True
        except Exception as e:
            logger.error(f"Error publishing opportunity to {channel}: {e}")
            return False

    def publish_execution(self, execution: TradeExecution) -> bool:
        """
        Publish a trade execution to the appropriate channel
        based on its strategy
        """
        channel = self.get_execution_channel(execution.strategy)
        try:
            self.redis.client.publish(channel, execution.model_dump_json())
            logger.debug(f"Published execution {execution.id} to channel {channel}")
            return True
        except Exception as e:
            logger.error(f"Error publishing execution to {channel}: {e}")
            return False

    def publish_to_all_strategies(self, message: Dict, message_type: str = "info") -> List[bool]:
        """
        Publish a message to all strategy channels of a specific type

        Parameters:
            message: The message to publish (will be JSON serialized)
            message_type: The type of channel (opportunities, executions, debug)

        Returns:
            List of success/failure results for each publication
        """
        strategies = StrategyManager.get_all_strategy_names()
        results = []

        for strategy in strategies:
            if message_type == "opportunity":
                channel = self.get_opportunity_channel(strategy)
            elif message_type == "execution":
                channel = self.get_execution_channel(strategy)
            else:
                channel = self.get_debug_channel(strategy)

            try:
                import json

                self.redis.client.publish(channel, json.dumps(message))
                logger.debug(f"Published message to {channel}")
                results.append(True)
            except Exception as e:
                logger.error(f"Error publishing to {channel}: {e}")
                results.append(False)

        return results

    def close(self):
        """Close the Redis connection"""
        self.redis.close()
