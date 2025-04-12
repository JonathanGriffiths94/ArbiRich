import logging
import time
from typing import Optional

from src.arbirich.constants import TRADE_OPPORTUNITIES_CHANNEL
from src.arbirich.models.models import TradeOpportunity

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Maintain a cache of recently seen opportunities to avoid duplicates
opportunity_cache = {}
OPPORTUNITY_CACHE_TTL = 60  # seconds

# Use a single shared Redis service instance
_redis_service = None


def get_redis_service():
    global _redis_service
    if _redis_service is None:
        from src.arbirich.services.redis.redis_service import RedisService

        _redis_service = RedisService()
    return _redis_service


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

    # Create a key for debouncing - use opportunity.opportunity_key if available
    cache_key = (
        f"{strategy}:{opportunity.opportunity_key}"
        if hasattr(opportunity, "opportunity_key")
        else f"{strategy}:{opportunity.pair}:{opportunity.buy_exchange}:{opportunity.sell_exchange}"
    )

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
    Publish a trade opportunity directly to Redis.

    Parameters:
        opportunity: The opportunity to publish
        strategy_name: Optional strategy name
    """
    logger.debug(f"Publishing opportunity: {opportunity}")
    logger.debug(f"Strategy name: {strategy_name}")
    if not opportunity:
        return "No opportunity to publish"

    # If strategy_name is provided, update the opportunity's strategy
    if strategy_name and opportunity.strategy != strategy_name:
        opportunity.strategy = strategy_name

    try:
        # Get Redis service
        redis_service = get_redis_service()

        # Channel is strategy-specific
        channel = f"{TRADE_OPPORTUNITIES_CHANNEL}:{opportunity.strategy}"

        # Convert to JSON
        opportunity_json = opportunity.model_dump_json()

        # Debug what we're publishing
        logger.info(
            f"Publishing opportunity to channel {channel}: {opportunity.pair} with spread {opportunity.spread:.4%}"
        )

        # Store in Redis with key
        opportunity_key = f"trade_opportunity:{opportunity.strategy}:{opportunity.id}"
        redis_service.client.setex(opportunity_key, 300, opportunity_json)

        # Publish to channel
        result = redis_service.client.publish(channel, opportunity_json)

        if result > 0:
            logger.info(f"Published opportunity to {result} subscribers on {channel}")
            return f"Published: {opportunity.id} to {result} subscribers"
        else:
            logger.warning(f"Published opportunity to {channel} but no subscribers")
            return f"Published (no subscribers): {opportunity.id}"
    except Exception as e:
        logger.error(f"Error publishing opportunity: {e}")
        return f"Failed to publish: {opportunity.id}"
