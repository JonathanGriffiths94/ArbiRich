import logging
import time
from typing import Any, Dict, Optional, Tuple

from src.arbirich.core.trading.flows.bytewax_flows.common.redis_utils import get_repository, publish_message
from src.arbirich.models import ChannelName, TradeOpportunity

# Maintain a cache of recently seen opportunities to avoid duplicates
opportunity_cache = {}
OPPORTUNITY_CACHE_TTL = 60  # seconds
# Configurable threshold for considering an opportunity as "better"
SPREAD_IMPROVEMENT_THRESHOLD = 0.0005  # 0.05% improvement needed to republish
# Configurable flag to temporarily disable debouncing for debugging
DEBUG_DISABLE_DEBOUNCE = True

# Add a set to track recently published opportunity IDs
_recently_published_ids = set()
_MAX_RECENT_IDS = 1000  # Maximum number of IDs to track

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def log_opportunity_details(opportunity: TradeOpportunity, prefix: str = "OPPORTUNITY") -> None:
    """
    Log detailed information about an opportunity for debugging purposes.

    Args:
        opportunity: The opportunity to log
        prefix: Prefix for the log message
    """
    if not opportunity:
        logger.debug(f"{prefix}: None")
        return

    # Extract key fields for logging
    details = {
        "id": getattr(opportunity, "id", "unknown"),
        "pair": getattr(opportunity, "pair", "unknown"),
        "strategy": getattr(opportunity, "strategy", "unknown"),
        "buy_exchange": getattr(opportunity, "buy_exchange", "unknown"),
        "sell_exchange": getattr(opportunity, "sell_exchange", "unknown"),
        "spread": f"{getattr(opportunity, 'spread', 0) * 100:.4f}%",
        "volume": getattr(opportunity, "volume", 0),
        "buy_price": getattr(opportunity, "buy_price", 0),
        "sell_price": getattr(opportunity, "sell_price", 0),
        "timestamp": getattr(opportunity, "timestamp", time.time()),
    }

    logger.info(f"{prefix}: {details}")


def get_cache_key(opportunity: TradeOpportunity) -> str:
    """
    Generate a consistent cache key for an opportunity.

    Args:
        opportunity: The opportunity to generate a key for

    Returns:
        str: A unique cache key for this opportunity
    """
    # Use opportunity_key attribute if available (preferred)
    if hasattr(opportunity, "opportunity_key") and opportunity.opportunity_key:
        return f"{opportunity.strategy}:{opportunity.opportunity_key}"

    # Otherwise use combination of strategy, pair, and exchanges
    return f"{opportunity.strategy}:{opportunity.pair}:{opportunity.buy_exchange}:{opportunity.sell_exchange}"


def dump_opportunity_cache() -> Dict[str, Tuple[float, float]]:
    """
    Return a copy of the current opportunity cache for inspection.

    Returns:
        Dict: Current opportunity cache
    """
    return opportunity_cache.copy()


def debounce_opportunity(opportunity: TradeOpportunity) -> Optional[TradeOpportunity]:
    """
    Debounce trade opportunities to avoid processing duplicates.

    Logs detailed information about why opportunities are accepted or rejected.

    Args:
        opportunity: The TradeOpportunity to debounce

    Returns:
        The opportunity if it should be processed, None if it should be debounced
    """
    if not opportunity:
        logger.debug("Debounce rejected: Opportunity is None")
        return None

    # Debug override - always accept if debouncing is disabled
    if DEBUG_DISABLE_DEBOUNCE:
        logger.warning("⚠️ DEBOUNCE DISABLED: Allowing all opportunities through")
        return opportunity

    # Generate cache key
    cache_key = get_cache_key(opportunity)
    current_time = time.time()
    spread = getattr(opportunity, "spread", 0)

    # Log the incoming opportunity for debugging
    log_opportunity_details(opportunity, "INCOMING OPPORTUNITY")
    logger.debug(f"Cache key: {cache_key}, Current time: {current_time}")

    # Check if we've seen this opportunity recently
    if cache_key in opportunity_cache:
        last_seen, last_spread = opportunity_cache[cache_key]
        time_since_last = current_time - last_seen

        if time_since_last < OPPORTUNITY_CACHE_TTL:
            # Calculate improvement in spread
            spread_diff = spread - last_spread
            spread_pct_improvement = (spread_diff / last_spread) if last_spread > 0 else 0

            logger.info(
                f"Found in cache: {cache_key} (time since last: {time_since_last:.2f}s, "
                f"last spread: {last_spread:.6f}, new spread: {spread:.6f}, "
                f"improvement: {spread_diff:.6f} ({spread_pct_improvement * 100:.2f}%))"
            )

            # Only update and return if the new spread is significantly better
            if spread > last_spread and spread_diff >= SPREAD_IMPROVEMENT_THRESHOLD:
                logger.info(
                    f"⬆️ ACCEPTING: Better spread for {cache_key}: {spread:.6f} > {last_spread:.6f} "
                    f"(diff: {spread_diff:.6f}, meets threshold: {SPREAD_IMPROVEMENT_THRESHOLD:.6f})"
                )
                opportunity_cache[cache_key] = (current_time, spread)
                return opportunity
            else:
                logger.info(
                    f"🔄 DEBOUNCING: {cache_key} - Spread improvement insufficient: "
                    f"{spread_diff:.6f} < required {SPREAD_IMPROVEMENT_THRESHOLD:.6f}"
                )
                # Update timestamp to extend TTL, but keep existing spread for comparison
                opportunity_cache[cache_key] = (current_time, last_spread)
                return None
        else:
            # TTL expired, treat as new opportunity
            logger.info(
                f"🕒 Cache entry expired after {time_since_last:.2f}s (> {OPPORTUNITY_CACHE_TTL}s), accepting as new"
            )

    # New opportunity, add to cache
    opportunity_cache[cache_key] = (current_time, spread)
    logger.info(f"🆕 ACCEPTING: New opportunity added to cache: {cache_key} with spread {spread:.6f}")

    # Clean up expired entries
    expired_count = 0
    for k in list(opportunity_cache.keys()):
        timestamp, _ = opportunity_cache[k]
        if current_time - timestamp > OPPORTUNITY_CACHE_TTL:
            del opportunity_cache[k]
            expired_count += 1

    if expired_count > 0:
        logger.debug(f"🧹 Removed {expired_count} expired entries from cache")

    # Log cache size
    logger.debug(f"Cache size: {len(opportunity_cache)} entries")

    return opportunity


# Function to temporarily disable debouncing for diagnostic purposes
def disable_debounce(disable: bool = True) -> None:
    """
    Temporarily disable debouncing for diagnostic purposes.

    Args:
        disable: True to disable debouncing, False to enable it
    """
    global DEBUG_DISABLE_DEBOUNCE
    DEBUG_DISABLE_DEBOUNCE = disable
    logger.warning(f"⚠️ Opportunity debouncing {'DISABLED' if disable else 'ENABLED'}")


def prepare_opportunity_data(opportunity: TradeOpportunity) -> Dict[str, Any]:
    """
    Prepare opportunity data in a consistent format that will pass validation.

    Args:
        opportunity: The trade opportunity to format

    Returns:
        Dict with properly formatted opportunity data
    """
    # Use model_dump directly - Assumes we always have Pydantic v2 models
    data = opportunity.model_dump()
    logger.debug(f"Created opportunity data using model_dump() with keys: {list(data.keys())}")
    return data


# Add function to check against Redis tracking
def is_opportunity_published_in_redis(opportunity_id: str) -> bool:
    """
    Check if an opportunity ID is tracked as published in Redis.

    Args:
        opportunity_id: The opportunity ID to check

    Returns:
        bool: True if already published according to Redis, False otherwise
    """
    try:
        # Get repository to use its tracking mechanism
        repo = get_repository("trade_opportunity", "detection")
        if repo and hasattr(repo, "was_recently_published"):
            return repo.was_recently_published(opportunity_id)
    except Exception as e:
        logger.error(f"Error checking Redis for published opportunity: {e}")

    return False


def publish_trade_opportunity(opportunity: TradeOpportunity) -> int:
    """
    Publish a trade opportunity to Redis with enhanced reliability.

    Args:
        opportunity: Trade opportunity Pydantic model

    Returns:
        number of subscribers that received the message
    """
    global _recently_published_ids

    logger.critical(f"🚨 TRADE OPPORTUNITY PUBLISH ATTEMPT: {opportunity.pair} {opportunity.strategy}")

    # First check our local tracking
    if opportunity.id in _recently_published_ids:
        logger.info(f"🔄 Skipping duplicate opportunity {opportunity.id} - already published in local cache")
        return 0

    # Then check Redis for global tracking
    if is_opportunity_published_in_redis(opportunity.id):
        logger.info(f"🔄 Skipping duplicate opportunity {opportunity.id} - already published in Redis")
        _recently_published_ids.add(opportunity.id)  # Add to local tracking as well
        return 0

    # Log more details about the opportunity for debugging
    log_opportunity_details(opportunity, "ATTEMPTING TO PUBLISH")

    # Number of retry attempts for publishing
    MAX_PUBLISH_RETRIES = 3

    try:
        # Debounce the opportunity
        debounced = debounce_opportunity(opportunity)
        if not debounced:
            debounce_info = f"Cache key: {get_cache_key(opportunity)}, spread: {opportunity.spread:.6f}"
            logger.info(f"🚫 Opportunity was debounced, not publishing. {debounce_info}")
            print(f"INFO: Opportunity was debounced, not publishing. {debounce_info}")
            return 0

        # Convert the Pydantic model to JSON directly
        opportunity_json = opportunity.model_dump_json()

        # Strategy-specific channel
        strategy_channel = f"{ChannelName.TRADE_OPPORTUNITIES.value}:{opportunity.strategy}"

        # First, try using the repository pattern for more robust handling
        repo_success = False
        for retry in range(MAX_PUBLISH_RETRIES):
            try:
                # Get the trade opportunity repository
                repo = get_repository("trade_opportunity", "detection")
                if repo:
                    # Try to save the opportunity first (this stores it in Redis)
                    save_result = repo.save(opportunity)
                    if not save_result:
                        logger.warning(f"⚠️ Failed to save opportunity to Redis (attempt {retry + 1})")

                    # Then try to publish it using the repository - pass the model directly
                    subscribers = repo.publish(opportunity)
                    if subscribers > 0:
                        success_msg = (
                            f"📢 Published opportunity for {opportunity.pair} via repository pattern "
                            f"with {subscribers} subscribers (spread: {opportunity.spread:.6%}, "
                            f"est. profit: {opportunity.spread * opportunity.volume * opportunity.buy_price:.8f})"
                        )
                        logger.critical(success_msg)
                        repo_success = True

                        # Add to recently published set
                        _recently_published_ids.add(opportunity.id)

                        # Trim set if it gets too large
                        if len(_recently_published_ids) > _MAX_RECENT_IDS:
                            # Convert to list, sort by ID, and keep newest
                            id_list = list(_recently_published_ids)
                            id_list.sort()  # Sort lexicographically
                            _recently_published_ids = set(id_list[-_MAX_RECENT_IDS:])

                        return subscribers
                    else:
                        logger.warning(f"⚠️ Repository publish returned 0 subscribers (attempt {retry + 1})")
                else:
                    logger.warning(f"⚠️ Could not get trade_opportunity repository (attempt {retry + 1})")
                # If retry isn't the last attempt, wait before trying again
                if retry < MAX_PUBLISH_RETRIES - 1:
                    time.sleep(0.1 * (retry + 1))  # Increasing backoff
            except Exception as repo_error:
                logger.error(f"❌ Repository publishing error (attempt {retry + 1}): {repo_error}")
                if retry < MAX_PUBLISH_RETRIES - 1:
                    time.sleep(0.1 * (retry + 1))  # Increasing backoff

        # If repository method failed after all retries, try direct channel publishing
        if not repo_success:
            logger.warning("⚠️ Falling back to direct Redis publish method")
            total_subscribers = 0

            for retry in range(MAX_PUBLISH_RETRIES):
                try:
                    # Use the direct JSON string representation of the model
                    logger.debug("Publishing model JSON directly")

                    # First publish to the main opportunities channel
                    main_subscribers = publish_message(
                        ChannelName.TRADE_OPPORTUNITIES.value, opportunity_json, "detection"
                    )

                    # Then publish to the strategy-specific channel
                    strategy_subscribers = publish_message(strategy_channel, opportunity_json, "detection")

                    total_subscribers = main_subscribers + strategy_subscribers

                    if total_subscribers > 0:
                        success_msg = (
                            f"📢 Published opportunity for {opportunity.pair} via direct channels "
                            f"with {total_subscribers} subscribers (spread: {opportunity.spread:.6%}, "
                            f"est. profit: {opportunity.spread * opportunity.volume * opportunity.buy_price:.8f})"
                        )
                        logger.critical(success_msg)
                        logger.info(success_msg)  # Standard INFO level
                        print(f"SUCCESS: {success_msg}")  # Direct print as failsafe

                        # Add to recently published set
                        _recently_published_ids.add(opportunity.id)

                        # Trim set if it gets too large
                        if len(_recently_published_ids) > _MAX_RECENT_IDS:
                            # Convert to list, sort by ID, and keep newest
                            id_list = list(_recently_published_ids)
                            id_list.sort()  # Sort lexicographically
                            _recently_published_ids = set(id_list[-_MAX_RECENT_IDS:])

                        return total_subscribers

                    # If retry isn't the last attempt, wait before trying again
                    if retry < MAX_PUBLISH_RETRIES - 1:
                        logger.warning(f"Retry {retry + 1} failed with 0 subscribers, trying again...")
                        time.sleep(0.1 * (retry + 1))  # Increasing backoff
                except Exception as e:
                    logger.error(f"❌ Error in direct publish attempt {retry + 1}: {e}")
                    if retry < MAX_PUBLISH_RETRIES - 1:
                        time.sleep(0.1 * (retry + 1))  # Increasing backoff

            if total_subscribers == 0:
                logger.error("❌ Failed to publish opportunity to any channels after all retries")

                # Last-ditch effort - try with a direct Redis client
                try:
                    from src.arbirich.services.redis.redis_service import RedisService

                    # Create a fresh Redis client
                    direct_client = RedisService()

                    # Publish using the raw opportunity_data directly
                    result = direct_client.publish_trade_opportunity(opportunity)

                    if result > 0:
                        logger.info(f"✅ Emergency direct publish succeeded with {result} subscribers")
                        return result

                    direct_client.close()
                except Exception as emergency_error:
                    logger.error(f"❌ Emergency direct publish failed: {emergency_error}")

                return 0

            return total_subscribers

    except Exception as e:
        error_msg = f"❌ Error publishing trade opportunity: {str(e)}"
        logger.critical(error_msg)
        print(f"EXCEPTION: {error_msg}")
        import traceback

        trace = traceback.format_exc()
        print(f"TRACEBACK: {trace}")
        logger.error(error_msg, exc_info=True)
        return 0
