import logging
import time
from typing import Optional

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
        from src.arbirich.services.redis.redis_service import get_shared_redis_client

        _redis_service = get_shared_redis_client()
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


def publish_trade_opportunity(opportunity, strategy_name=None):
    """
    Publish a trade opportunity to Redis.

    Args:
        opportunity: The trade opportunity to publish
        strategy_name: Optional strategy name (will publish to strategy-specific channel)

    Returns:
        The opportunity if published successfully, None otherwise
    """
    from src.arbirich.services.redis.redis_channel_manager import RedisChannelManager
    from src.arbirich.services.redis.redis_service import get_shared_redis_client

    publish_start = time.time()
    logger.info(f"Publishing trade opportunity: {opportunity.get('id', 'unknown')}")

    try:
        # If no strategy_name was provided, use the one from the opportunity
        if not strategy_name and hasattr(opportunity, "strategy"):
            strategy_name = opportunity.strategy
        elif not strategy_name and isinstance(opportunity, dict):
            strategy_name = opportunity.get("strategy")

        logger.info(f"Publishing to strategy: {strategy_name}")

        # Make sure we have a strategy name that matches what's in the config
        from src.arbirich.config.config import ALL_STRATEGIES

        if strategy_name and strategy_name not in ALL_STRATEGIES:
            logger.warning(f"Publishing opportunity for unknown strategy: {strategy_name}")

        # Use the channel manager to handle publication to multiple channels
        redis_client = get_shared_redis_client()
        channel_manager = RedisChannelManager(redis_client)

        # Log the opportunity details before publishing
        if isinstance(opportunity, dict):
            pair = opportunity.get("pair", "unknown")
            buy_exchange = opportunity.get("buy_exchange", "unknown")
            sell_exchange = opportunity.get("sell_exchange", "unknown")
            buy_price = opportunity.get("buy_price", 0)
            sell_price = opportunity.get("sell_price", 0)
            spread = opportunity.get("spread", 0)
            volume = opportunity.get("volume", 0)
        else:
            pair = getattr(opportunity, "pair", "unknown")
            buy_exchange = getattr(opportunity, "buy_exchange", "unknown")
            sell_exchange = getattr(opportunity, "sell_exchange", "unknown")
            buy_price = getattr(opportunity, "buy_price", 0)
            sell_price = getattr(opportunity, "sell_price", 0)
            spread = getattr(opportunity, "spread", 0)
            volume = getattr(opportunity, "volume", 0)

        logger.info(
            f"PUBLISHING OPPORTUNITY:\n"
            f"  Strategy: {strategy_name}\n"
            f"  Pair: {pair}\n"
            f"  Buy: {buy_exchange} @ {buy_price:.8f}\n"
            f"  Sell: {sell_exchange} @ {sell_price:.8f}\n"
            f"  Spread: {spread:.6%}\n"
            f"  Volume: {volume:.8f}\n"
            f"  Est. Profit: {(spread * volume * buy_price):.8f}"
        )

        subscribers = channel_manager.publish_opportunity(opportunity)
        publish_time = time.time() - publish_start

        if subscribers > 0:
            logger.info(f"Published opportunity to {subscribers} subscribers in {publish_time:.6f}s")
            return opportunity
        else:
            logger.warning(f"No subscribers found for opportunity (took {publish_time:.6f}s)")
            return opportunity  # Still return the opportunity to continue the flow

    except Exception as e:
        logger.error(f"Error publishing opportunity: {e}", exc_info=True)
        publish_time = time.time() - publish_start
        logger.error(f"Publishing failed after {publish_time:.6f}s")
        return opportunity  # Still return the opportunity to continue the flow
