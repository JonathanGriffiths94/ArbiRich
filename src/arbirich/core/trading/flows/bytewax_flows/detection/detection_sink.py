import logging
import time
from typing import Optional

from src.arbirich.core.trading.flows.bytewax_flows.common.redis_utils import get_redis_client
from src.arbirich.models.enums import ChannelName
from src.arbirich.models.models import TradeOpportunity
from src.arbirich.services.redis.redis_channel_manager import get_channel_manager

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Maintain a cache of recently seen opportunities to avoid duplicates
opportunity_cache = {}
OPPORTUNITY_CACHE_TTL = 60  # seconds


def debounce_opportunity(opportunity: TradeOpportunity) -> Optional[TradeOpportunity]:
    """
    Debounce trade opportunities to avoid processing duplicates.

    Parameters:
        opportunity: The TradeOpportunity to debounce
    """
    if not opportunity:
        return None

    # Create a key for debouncing - use opportunity.opportunity_key if available
    cache_key = (
        f"{opportunity.strategy}:{opportunity.opportunity_key}"
        if hasattr(opportunity, "opportunity_key")
        else f"{opportunity.strategy}:{opportunity.pair}:{opportunity.buy_exchange}:{opportunity.sell_exchange}"
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


def publish_trade_opportunity(opportunity: TradeOpportunity) -> int:
    """
    Publish a trade opportunity to Redis.

    Args:
        opportunity: Trade opportunity Pydantic model

    Returns:
        number of subscribers that received the message
    """
    try:
        # Get Redis client for detection
        redis_client = get_redis_client("detection")
        if not redis_client:
            logger.error("Failed to get Redis client for publishing opportunity")
            return 0

        # Debounce the opportunity
        debounced = debounce_opportunity(opportunity)
        if not debounced:
            logger.debug("Opportunity was debounced, not publishing")
            return 0

        # Get the channel manager
        channel_manager = get_channel_manager()
        if not channel_manager:
            logger.error("Failed to get Redis channel manager")
            return 0

        subscribers = channel_manager.publish_opportunity(debounced)

        strategy_channel = f"{ChannelName.TRADE_OPPORTUNITIES.value}:{opportunity.strategy}"
        logger.debug(
            f"Published opportunity to main channel and {strategy_channel} with {subscribers} total subscribers"
        )

        return subscribers

    except Exception as e:
        logger.error(f"Error publishing trade opportunity: {e}", exc_info=True)
        return 0
