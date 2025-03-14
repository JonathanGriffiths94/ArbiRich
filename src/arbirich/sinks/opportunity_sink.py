import logging
import time
from typing import Optional

from src.arbirich.models.models import TradeOpportunity
from src.arbirich.services.redis_channel_manager import RedisChannelManager

logger = logging.getLogger(__name__)

# Maintain a cache of recently seen opportunities to avoid duplicates
opportunity_cache = {}
OPPORTUNITY_CACHE_TTL = 60  # seconds
redis_channel_manager = RedisChannelManager()


def debounce_opportunity(redis_client, opportunity: TradeOpportunity, strategy_name=None) -> Optional[TradeOpportunity]:
    """
    Debounce trade opportunities to avoid processing duplicates.

    Parameters:
        redis_client: Redis client for checking existing opportunities
        opportunity: The opportunity to debounce
        strategy_name: Optional strategy name for more specific caching
    """
    if not opportunity:
        return None

    # Use the opportunity's strategy field if no strategy_name is provided
    strategy = strategy_name or opportunity.strategy

    # Include strategy in the cache key
    cache_key = f"{strategy}:{opportunity.opportunity_key}"

    current_time = time.time()

    # Check if we've seen this opportunity recently
    if cache_key in opportunity_cache:
        last_seen, last_spread = opportunity_cache[cache_key]
        if current_time - last_seen < OPPORTUNITY_CACHE_TTL:
            # Only update and return if the new spread is better
            if opportunity.spread > last_spread:
                logger.info(f"Better spread for existing opportunity {cache_key}: {opportunity.spread} > {last_spread}")
                opportunity_cache[cache_key] = (current_time, opportunity.spread)
                return opportunity
            else:
                logger.debug(f"Duplicate opportunity with equal/worse spread, skipping: {cache_key}")
                return None

    # New opportunity, add to cache
    opportunity_cache[cache_key] = (current_time, opportunity.spread)

    # Clean up expired entries
    for k in list(opportunity_cache.keys()):
        timestamp, _ = opportunity_cache[k]
        if current_time - timestamp > OPPORTUNITY_CACHE_TTL:
            del opportunity_cache[k]

    return opportunity


def publish_trade_opportunity(opportunity: TradeOpportunity, strategy_name=None) -> str:
    """
    Publish a trade opportunity to appropriate channels using RedisChannelManager.

    Parameters:
        opportunity: The opportunity to publish
        strategy_name: Optional strategy name (will override opportunity.strategy if provided)
    """
    if not opportunity:
        return "No opportunity to publish"

    # If strategy_name is provided, update the opportunity's strategy
    if strategy_name and opportunity.strategy != strategy_name:
        opportunity.strategy = strategy_name

    # Use the channel manager to publish to the appropriate channel
    if redis_channel_manager.publish_opportunity(opportunity):
        return f"Published: {opportunity.id}"
    else:
        return f"Failed to publish: {opportunity.id}"
