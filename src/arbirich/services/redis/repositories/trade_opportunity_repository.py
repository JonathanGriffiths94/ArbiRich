import json
import logging
from typing import List, Optional

from redis.exceptions import RedisError

from src.arbirich.constants import TRADE_OPPORTUNITIES_CHANNEL, TRADE_OPPORTUNITY_KEY_PREFIX
from src.arbirich.models.models import TradeOpportunity

logger = logging.getLogger(__name__)


class TradeOpportunityRepository:
    """Repository for managing trade opportunities."""

    def __init__(self, redis_client):
        self.redis_client = redis_client

    def save(self, opportunity: TradeOpportunity) -> bool:
        """Store a trade opportunity in Redis."""
        try:
            key = f"{TRADE_OPPORTUNITY_KEY_PREFIX}:{opportunity.id}"

            # Convert to serializable format using model_dump
            data = opportunity.model_dump()

            # Store as JSON string
            self.redis_client.set(key, json.dumps(data))

            # Set a reasonable expiration (e.g., 5 minutes)
            self.redis_client.expire(key, 300)

            return True
        except RedisError as e:
            logger.error(f"Error storing trade opportunity {opportunity.id}: {e}")
            return False

    def get(self, opportunity_id: str) -> Optional[TradeOpportunity]:
        """Retrieve a trade opportunity by ID."""
        try:
            key = f"{TRADE_OPPORTUNITY_KEY_PREFIX}:{opportunity_id}"
            data = self.redis_client.get(key)

            if not data:
                return None

            # Create and return TradeOpportunity object
            return TradeOpportunity(**json.loads(data))
        except RedisError as e:
            logger.error(f"Error retrieving trade opportunity {opportunity_id}: {e}")
            return None

    def publish(self, opportunity: TradeOpportunity) -> int:
        """
        Publish a trade opportunity to both the main channel and strategy-specific channel.

        Args:
            opportunity: The trade opportunity to publish

        Returns:
            Total number of subscribers that received the message (sum of both channels)
        """
        try:
            # Convert the opportunity to JSON
            data = opportunity.model_dump()
            json_data = json.dumps(data)

            # Publish to main opportunities channel
            main_channel = TRADE_OPPORTUNITIES_CHANNEL
            main_result = self.redis_client.publish(main_channel, json_data)

            # Publish to strategy-specific channel
            strategy_channel = f"{main_channel}:{opportunity.strategy}"
            strategy_result = self.redis_client.publish(strategy_channel, json_data)

            # Return total subscriber count
            total_subscribers = main_result + strategy_result

            # Log success at debug level
            if main_result > 0 or strategy_result > 0:
                logger.debug(
                    f"Published opportunity to {main_channel} ({main_result} subs) and {strategy_channel} ({strategy_result} subs)"
                )
            else:
                # No subscribers - still log but note this
                logger.debug(
                    f"Published opportunity to {main_channel} and {strategy_channel}, but no subscribers were active"
                )

            return total_subscribers

        except RedisError as e:
            logger.error(f"Error publishing trade opportunity: {e}")
            return 0

    def get_recent(self, count: int = 50) -> List[TradeOpportunity]:
        """Get the most recent trade opportunities."""
        try:
            # Pattern to match all opportunities
            pattern = f"{TRADE_OPPORTUNITY_KEY_PREFIX}:*"
            keys = self.redis_client.keys(pattern)

            # Sort by creation time (if available in future) or just get the most recent added
            opportunities = []

            # Limit to the requested count
            for key in keys[:count]:
                data = self.redis_client.get(key)
                if data:
                    try:
                        opportunity_dict = json.loads(data)
                        opportunity = TradeOpportunity(**opportunity_dict)
                        opportunities.append(opportunity)
                    except Exception as e:
                        logger.error(f"Error parsing opportunity data: {e}")

            return opportunities
        except RedisError as e:
            logger.error(f"Error retrieving recent opportunities: {e}")
            return []
