import logging
import sys
import time
from typing import Optional

from src.arbirich.core.trading.flows.bytewax_flows.common.redis_utils import get_redis_client
from src.arbirich.models.enums import ChannelName
from src.arbirich.models.models import TradeOpportunity

# Configure root logger to ensure messages are displayed
root_logger = logging.getLogger()
if not root_logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Print direct console output to verify execution
print("DETECTION_SINK MODULE LOADED")

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
                logger.info(
                    f"⬆️ Better spread for existing opportunity {cache_key}: {opportunity.spread} > {last_spread}"
                )
                opportunity_cache[cache_key] = (current_time, opportunity.spread)
                return opportunity
            else:
                logger.debug(f"🔄 Duplicate opportunity with equal/worse spread, skipping: {cache_key}")
                return None

    # New opportunity, add to cache
    opportunity_cache[cache_key] = (current_time, opportunity.spread)
    logger.debug(f"🆕 New opportunity added to cache: {cache_key}")

    # Clean up expired entries
    for k in list(opportunity_cache.keys()):
        timestamp, _ = opportunity_cache[k]
        if current_time - timestamp > OPPORTUNITY_CACHE_TTL:
            del opportunity_cache[k]
            logger.debug(f"🧹 Removed expired opportunity from cache: {k}")

    return opportunity


def publish_trade_opportunity(opportunity: TradeOpportunity) -> int:
    """
    Publish a trade opportunity to Redis.

    Args:
        opportunity: Trade opportunity Pydantic model

    Returns:
        number of subscribers that received the message
    """
    # First validate that we have a proper TradeOpportunity model
    if not isinstance(opportunity, TradeOpportunity):
        error_msg = f"❌ Expected TradeOpportunity model, got {type(opportunity)}"
        print(f"ERROR: {error_msg}")
        logger.critical(error_msg)
        return 0

    # Direct print to verify function execution regardless of logging
    print(f"PUBLISHING OPPORTUNITY: {opportunity.pair} {opportunity.strategy}")

    # Force a log message to appear with maximum visibility
    logger.critical(f"🚨 TRADE OPPORTUNITY PUBLISH ATTEMPT: {opportunity.pair} {opportunity.strategy}")

    try:
        # Get Redis client for detection
        redis_client = get_redis_client("detection")
        if not redis_client:
            error_msg = "❌ Failed to get Redis client for publishing opportunity"
            logger.error(error_msg)
            print(f"ERROR: {error_msg}")
            return 0

        # Debounce the opportunity
        debounced = debounce_opportunity(opportunity)
        if not debounced:
            logger.info("🚫 Opportunity was debounced, not publishing")
            print("INFO: Opportunity was debounced, not publishing")
            return 0

        # Use the repository pattern to publish the opportunity
        subscribers = redis_client.get_trade_opportunity_repository().publish(debounced)

        strategy_channel = f"{ChannelName.TRADE_OPPORTUNITIES.value}:{opportunity.strategy}"

        # Log at multiple levels to ensure visibility
        success_msg = (
            f"📢 Published opportunity for {opportunity.pair} to main channel and {strategy_channel} "
            f"with {subscribers} subscribers (spread: {opportunity.spread:.6%}, "
            f"est. profit: {opportunity.spread * opportunity.volume * opportunity.buy_price:.8f})"
        )
        logger.critical(success_msg)  # CRITICAL to ensure it appears in logs
        logger.error(success_msg)  # Also try ERROR level
        logger.info(success_msg)  # And standard INFO level
        print(f"SUCCESS: {success_msg}")  # Direct print as failsafe

        # Return the subscriber count
        return subscribers

    except Exception as e:
        error_msg = f"❌ Error publishing trade opportunity: {str(e)}"
        logger.critical(error_msg)
        print(f"EXCEPTION: {error_msg}")
        import traceback

        trace = traceback.format_exc()
        print(f"TRACEBACK: {trace}")
        logger.error(error_msg, exc_info=True)
        return 0
