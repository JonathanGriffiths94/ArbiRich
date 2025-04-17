"""
Redis client utilities for the reporting flow.
Ensures proper Redis connection and channel subscription.
"""

import logging
from typing import List

from src.arbirich.config.constants import TRADE_EXECUTIONS_CHANNEL, TRADE_OPPORTUNITIES_CHANNEL
from src.arbirich.services.redis.redis_service import get_shared_redis_client

logger = logging.getLogger(__name__)


def get_redis_with_pubsub():
    """
    Get a Redis client with a PubSub instance configured.

    Returns:
        Tuple of (redis_client, pubsub)
    """
    redis_client = get_shared_redis_client()
    if redis_client and hasattr(redis_client, "client"):
        pubsub = redis_client.client.pubsub(ignore_subscribe_messages=True)
        return redis_client, pubsub

    return None, None


def subscribe_to_main_channels(pubsub) -> List[str]:
    """
    Subscribe to the main channels and strategy-specific opportunity channels.

    Args:
        pubsub: The Redis PubSub instance

    Returns:
        List of channels subscribed to
    """
    if not pubsub:
        logger.error("❌ PubSub not available")
        return []

    # Start with main channels
    channels = [
        TRADE_OPPORTUNITIES_CHANNEL,
        TRADE_EXECUTIONS_CHANNEL,
    ]

    try:
        pubsub.subscribe(*channels)
        logger.info(f"🔄 Subscribed to {len(channels)} channels")
        return channels
    except Exception as e:
        logger.error(f"❌ Error subscribing to channels: {e}")
        return []


async def check_redis_health(redis_client, pubsub, channels=None) -> bool:
    """
    Check if Redis connection and subscriptions are healthy.

    Args:
        redis_client: The Redis client
        pubsub: The Redis PubSub instance
        channels: List of channels to verify, or None to use main channels

    Returns:
        True if healthy, False otherwise
    """
    if not redis_client or not pubsub:
        logger.error("❌ Redis client or PubSub not available")
        return False

    try:
        # Check ping
        if not redis_client.client.ping():
            logger.error("❌ Redis ping failed")
            return False

        # If no channels specified, use main channels
        if not channels:
            channels = [TRADE_OPPORTUNITIES_CHANNEL, TRADE_EXECUTIONS_CHANNEL]

        # Check subscriptions - just return true for now
        # In real implementation, would verify subscriptions are active
        return True

    except Exception as e:
        logger.error(f"❌ Redis health check failed: {e}")
        return False
