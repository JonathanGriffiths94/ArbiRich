import json
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
        if not _redis_service:
            raise RuntimeError("Failed to initialize Redis service")
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

    # Early safety check for None opportunity
    if opportunity is None:
        raise ValueError("Attempted to publish None opportunity")

    # Safer ID extraction - raise error if ID cannot be determined
    opportunity_id = None
    if isinstance(opportunity, dict):
        opportunity_id = opportunity.get("id")
    else:
        opportunity_id = getattr(opportunity, "id", None)

    if not opportunity_id:
        raise ValueError(f"Missing opportunity ID in {type(opportunity)}")

    logger.info(f"Publishing trade opportunity: {opportunity_id}")

    # If no strategy_name was provided, use the one from the opportunity
    if not strategy_name:
        if hasattr(opportunity, "strategy"):
            strategy_name = opportunity.strategy
        elif isinstance(opportunity, dict):
            strategy_name = opportunity.get("strategy")

        if not strategy_name:
            raise ValueError("No strategy name provided and none found in opportunity")

    logger.info(f"Publishing to strategy: {strategy_name}")

    # Make sure we have a strategy name that matches what's in the config
    from src.arbirich.config.config import ALL_STRATEGIES

    if strategy_name not in ALL_STRATEGIES:
        raise ValueError(f"Publishing opportunity for unknown strategy: {strategy_name}")

    # Use the channel manager to handle publication to multiple channels
    redis_client = get_shared_redis_client()
    if not redis_client:
        raise RuntimeError("Failed to get Redis client, cannot publish opportunity")

    channel_manager = RedisChannelManager(redis_client)

    # Extract opportunity details - with strict requirements
    if isinstance(opportunity, dict):
        if "pair" not in opportunity:
            raise KeyError("Missing 'pair' in opportunity dict")

        pair = opportunity["pair"]
        buy_exchange = opportunity.get("buy_exchange")
        sell_exchange = opportunity.get("sell_exchange")
        buy_price = opportunity.get("buy_price")
        sell_price = opportunity.get("sell_price")
        spread = opportunity.get("spread")
        volume = opportunity.get("volume")

        # Validate critical fields
        if not all([buy_exchange, sell_exchange, buy_price, sell_price, spread, volume]):
            missing = []
            for field in ["buy_exchange", "sell_exchange", "buy_price", "sell_price", "spread", "volume"]:
                if not opportunity.get(field):
                    missing.append(field)
            raise ValueError(f"Missing required fields in opportunity: {missing}")
    else:
        # Object attribute access
        if not hasattr(opportunity, "pair"):
            raise AttributeError("Missing 'pair' attribute in opportunity object")

        pair = opportunity.pair
        buy_exchange = getattr(opportunity, "buy_exchange", None)
        sell_exchange = getattr(opportunity, "sell_exchange", None)
        buy_price = getattr(opportunity, "buy_price", None)
        sell_price = getattr(opportunity, "sell_price", None)
        spread = getattr(opportunity, "spread", None)
        volume = getattr(opportunity, "volume", None)

        # Validate critical fields
        if not all([buy_exchange, sell_exchange, buy_price, sell_price, spread, volume]):
            missing = []
            for field in ["buy_exchange", "sell_exchange", "buy_price", "sell_price", "spread", "volume"]:
                if not getattr(opportunity, field, None):
                    missing.append(field)
            raise ValueError(f"Missing required fields in opportunity: {missing}")

    # Calculate profit
    est_profit = spread * volume * buy_price

    logger.info(
        f"PUBLISHING OPPORTUNITY:\n"
        f"  Strategy: {strategy_name}\n"
        f"  Pair: {pair}\n"
        f"  Buy: {buy_exchange} @ {buy_price:.8f}\n"
        f"  Sell: {sell_exchange} @ {sell_price:.8f}\n"
        f"  Spread: {spread:.6%}\n"
        f"  Volume: {volume:.8f}\n"
        f"  Est. Profit: {est_profit:.8f}"
    )

    # Log the exact channel we're publishing to
    main_channel = channel_manager.get_opportunity_channel()
    strategy_channel = f"{main_channel}:{strategy_name}"

    logger.info(f"Publishing opportunity to channels: {main_channel} and {strategy_channel}")

    # Actual publishing - raise errors instead of suppressing them
    subscribers = channel_manager.publish_opportunity(opportunity, strategy_name)
    publish_time = time.time() - publish_start

    if subscribers > 0:
        logger.info(f"Published opportunity to {subscribers} subscribers in {publish_time:.6f}s")

        # Verify the strategy-specific publication
        try:
            # Check direct publication to strategy channel as additional verification
            # This is redundant but provides verification that strategy channels are working
            direct_strategy_pubs = redis_client.client.publish(
                strategy_channel,
                opportunity.model_dump_json() if hasattr(opportunity, "model_dump_json") else json.dumps(opportunity),
            )
            logger.info(f"Verification: Direct strategy channel publication reached {direct_strategy_pubs} subscribers")
        except Exception as e:
            logger.warning(f"Verification publication failed (but main publication succeeded): {e}")
    else:
        logger.warning(f"No subscribers found for opportunity (took {publish_time:.6f}s)")
        logger.warning(f"Check if subscribers are listening on channels {main_channel} or {strategy_channel}")

    return opportunity
