import json
import logging
import time
from typing import List, Optional, Set

from redis.exceptions import RedisError

from src.arbirich.constants import TRADE_OPPORTUNITIES_CHANNEL, TRADE_OPPORTUNITY_KEY_PREFIX
from src.arbirich.models.enums import ChannelName
from src.arbirich.models.models import TradeOpportunity

logger = logging.getLogger(__name__)


class TradeOpportunityRepository:
    """Repository for managing trade opportunities with enhanced reliability."""

    def __init__(self, redis_client):
        self.redis_client = redis_client
        self._last_error_time = 0
        self._error_backoff = 1  # Initial backoff in seconds
        self._max_backoff = 30  # Maximum backoff in seconds
        self._recently_published_key = "recently_published_opportunities"  # Redis set for tracking published IDs
        self._ttl = 60 * 60  # 1 hour TTL for tracking published IDs

    def save(self, opportunity: TradeOpportunity) -> bool:
        """Store a trade opportunity in Redis with retry logic."""
        attempts = 0
        max_attempts = 3

        # Check if this opportunity was already published
        if self.was_recently_published(opportunity.id):
            logger.info(f"Opportunity {opportunity.id} already recently published, skipping save")
            return True

        # Check with database as well
        db_exists = self.check_db_exists(opportunity.id)
        if db_exists:
            logger.info(f"Opportunity {opportunity.id} exists in database, skipping save")
            # Mark it in Redis for future reference
            self.mark_as_published(opportunity.id)
            return True

        while attempts < max_attempts:
            try:
                attempts += 1
                key = f"{TRADE_OPPORTUNITY_KEY_PREFIX}:{opportunity.id}"
                data = opportunity.model_dump()

                # Store as JSON string
                self.redis_client.set(key, json.dumps(data))

                # Set TTL to prevent Redis from filling up with old opportunities
                self.redis_client.expire(key, 300)  # 5 minutes

                # Track this ID as published
                self.mark_as_published(opportunity.id)

                # Reset backoff on success
                self._error_backoff = 1

                return True

            except RedisError as e:
                # Use exponential backoff between retries
                backoff = min(self._error_backoff, self._max_backoff)

                if attempts < max_attempts:
                    logger.warning(
                        f"Redis error storing opportunity {opportunity.id} (attempt {attempts}/{max_attempts}), "
                        f"retrying in {backoff}s: {e}"
                    )
                    time.sleep(backoff)
                    self._error_backoff *= 2  # Double backoff for next failure
                else:
                    logger.error(
                        f"Failed to store trade opportunity {opportunity.id} after {max_attempts} attempts: {e}"
                    )
                    return False

        return False

    def was_recently_published(self, opportunity_id: str) -> bool:
        """Check if an opportunity ID was recently published."""
        try:
            return bool(self.redis_client.sismember(self._recently_published_key, opportunity_id))
        except RedisError as e:
            logger.warning(f"Redis error checking recently published status: {e}")
            return False

    def mark_as_published(self, opportunity_id: str) -> bool:
        """Mark an opportunity as published to avoid duplicates."""
        try:
            # Add to Redis set with TTL
            self.redis_client.sadd(self._recently_published_key, opportunity_id)
            self.redis_client.expire(self._recently_published_key, self._ttl)
            return True
        except RedisError as e:
            logger.warning(f"Redis error marking opportunity as published: {e}")
            return False

    def get_recently_published_ids(self) -> Set[str]:
        """
        Get all IDs in the recently published set.

        Returns:
            Set of recently published opportunity IDs
        """
        try:
            # Get all members of the set
            members = self.redis_client.smembers(self._recently_published_key)

            # Convert from bytes to strings
            return {member.decode("utf-8") if isinstance(member, bytes) else str(member) for member in members}
        except RedisError as e:
            logger.warning(f"Redis error getting recently published IDs: {e}")
            return set()

    def check_db_exists(self, opportunity_id: str) -> bool:
        """
        Check if an opportunity exists in the database.

        Args:
            opportunity_id: ID to check

        Returns:
            bool: True if found in database, False otherwise
        """
        try:
            # Use database repository to check if it exists
            from src.arbirich.services.database.database_service import DatabaseService

            # We need to be careful with circular imports,
            # only import and instantiate when needed
            db = DatabaseService()
            exists = db.trade_opportunity_repo.check_exists(opportunity_id)
            db.close()
            return exists
        except Exception as e:
            logger.warning(f"Error checking database for opportunity {opportunity_id}: {e}")
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
        Publish a trade opportunity to the appropriate channels with enhanced reliability.

        This publishes to both the general opportunities channel and a strategy-specific channel.

        Args:
            opportunity: The trade opportunity to publish

        Returns:
            int: Total number of subscribers that received the message
        """
        if not opportunity:
            logger.error("Cannot publish None opportunity")
            return 0

        # Check if this opportunity was already published in Redis
        if self.was_recently_published(opportunity.id):
            logger.info(f"Opportunity {opportunity.id} already recently published, skipping publish")
            return 0

        # Check if it exists in database
        db_exists = self.check_db_exists(opportunity.id)
        if db_exists:
            logger.info(f"Opportunity {opportunity.id} exists in database, marking as published and skipping")
            self.mark_as_published(opportunity.id)
            return 0

        attempts = 0
        max_attempts = 3
        total_subscribers = 0

        while attempts < max_attempts and total_subscribers == 0:
            try:
                attempts += 1

                # IMPORTANT: Get direct JSON representation from model
                message_json = opportunity.model_dump_json()

                # Publish to main channel
                subscribers_main = self.redis_client.publish(TRADE_OPPORTUNITIES_CHANNEL, message_json)

                # Also publish to strategy-specific channel if strategy is specified
                subscribers_strategy = 0
                if hasattr(opportunity, "strategy") and opportunity.strategy:
                    strategy_channel = f"{ChannelName.TRADE_OPPORTUNITIES.value}:{opportunity.strategy}"
                    subscribers_strategy = self.redis_client.publish(strategy_channel, message_json)

                # Sum up all subscribers
                total_subscribers = subscribers_main + subscribers_strategy

                # Log success or warning based on subscriber count
                if total_subscribers > 0:
                    logger.info(
                        f"✅ Published opportunity {opportunity.id} to {2 if subscribers_strategy > 0 else 1} "
                        f"channels with {total_subscribers} total subscribers"
                    )

                    # Mark this ID as published
                    self.mark_as_published(opportunity.id)
                else:
                    logger.warning(f"⚠️ Published opportunity {opportunity.id} but no subscribers were listening")

                # Reset backoff on success
                self._error_backoff = 1

                return total_subscribers

            except RedisError as e:
                # Use exponential backoff between retries
                backoff = min(self._error_backoff, self._max_backoff)

                if attempts < max_attempts:
                    logger.warning(
                        f"Redis error publishing opportunity {opportunity.id} (attempt {attempts}/{max_attempts}), "
                        f"retrying in {backoff}s: {e}"
                    )
                    time.sleep(backoff)
                    self._error_backoff *= 2  # Double backoff for next failure
                else:
                    logger.error(
                        f"Failed to publish trade opportunity {opportunity.id} after {max_attempts} attempts: {e}"
                    )
                    return 0

        return total_subscribers

    def get_recent(self, count: int = 50) -> List[TradeOpportunity]:
        """Get the most recent trade opportunities."""
        try:
            pattern = f"{TRADE_OPPORTUNITY_KEY_PREFIX}:*"
            keys = self.redis_client.keys(pattern)

            opportunities = []

            # Sort keys by creation time if possible
            sorted_keys = keys[:count]

            # Limit to the requested count
            for key in sorted_keys:
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

    def get_by_strategy(self, strategy_name: str, count: int = 20) -> List[TradeOpportunity]:
        """
        Retrieve opportunities for a specific strategy.

        Args:
            strategy_name: The strategy name to filter by
            count: Maximum number of opportunities to return

        Returns:
            List of matching TradeOpportunity objects
        """
        try:
            pattern = f"{TRADE_OPPORTUNITY_KEY_PREFIX}:*"
            keys = self.redis_client.keys(pattern)

            opportunities = []

            for key in keys:
                if len(opportunities) >= count:
                    break

                data = self.redis_client.get(key)
                if data:
                    try:
                        opportunity_dict = json.loads(data)
                        # Check if it's for the requested strategy
                        if opportunity_dict.get("strategy") == strategy_name:
                            opportunity = TradeOpportunity(**opportunity_dict)
                            opportunities.append(opportunity)
                    except Exception as e:
                        logger.error(f"Error parsing opportunity data: {e}")

            return opportunities
        except RedisError as e:
            logger.error(f"Error retrieving opportunities for strategy {strategy_name}: {e}")
            return []
