import logging
from typing import Dict

from src.arbirich.services.redis.redis_service import get_shared_redis_client

logger = logging.getLogger(__name__)


def get_channel_subscribers(pattern: str = "*") -> Dict[str, int]:
    """
    Get a dictionary of all channels matching pattern and their subscriber count.

    Args:
        pattern: Redis pattern to match channels (default: "*" for all)

    Returns:
        Dictionary mapping channel names to subscriber counts
    """
    result = {}
    try:
        redis_client = get_shared_redis_client()
        if not redis_client or not redis_client.client:
            logger.warning("Redis client unavailable for channel diagnostics")
            return result

        # Use PUBSUB CHANNELS to get all active channels
        channels = redis_client.client.pubsub_channels(pattern)

        # Use PUBSUB NUMSUB to get subscriber counts
        if channels:
            subscriber_data = redis_client.client.pubsub_numsub(*channels)
            for channel, count in subscriber_data:
                if isinstance(channel, bytes):
                    channel = channel.decode("utf-8")
                result[channel] = count

        return result
    except Exception as e:
        logger.error(f"Error getting channel subscribers: {e}")
        return result


def log_channel_diagnostics():
    """Log diagnostic information about Redis channels and subscribers."""
    try:
        # Get all channels and their subscriber counts
        all_channels = get_channel_subscribers()

        # Get pair-specific channels
        pair_channels = get_channel_subscribers("*:*-*")

        # Strategy-specific channels
        strategy_channels = {}
        for strategy in ["basic_arbitrage", "mid_price_arbitrage"]:
            strategy_channels[strategy] = get_channel_subscribers(f"*:{strategy}*")

        # Log the results
        logger.info("=== REDIS CHANNEL DIAGNOSTICS ===")
        logger.info(f"Total active channels: {len(all_channels)}")

        # Log channels with subscribers
        with_subscribers = {k: v for k, v in all_channels.items() if v > 0}
        logger.info(f"Channels with subscribers ({len(with_subscribers)}): {with_subscribers}")

        # Log channels without subscribers
        without_subscribers = {k: v for k, v in all_channels.items() if v == 0}
        if without_subscribers:
            logger.warning(f"Channels without subscribers ({len(without_subscribers)}): {without_subscribers}")

        # Log pair-specific channel information
        logger.info(f"Trading pair channels: {pair_channels}")

        # Log strategy-specific channel information
        for strategy, channels in strategy_channels.items():
            logger.info(f"{strategy} channels: {channels}")

        logger.info("=== END CHANNEL DIAGNOSTICS ===")

        return {"all_channels": all_channels, "pair_channels": pair_channels, "strategy_channels": strategy_channels}
    except Exception as e:
        logger.error(f"Error in channel diagnostics: {e}")
        return {}
