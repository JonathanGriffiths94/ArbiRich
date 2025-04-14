import logging
import threading
from typing import Optional

from src.arbirich.services.redis.redis_service import RedisService

logger = logging.getLogger(__name__)

# Global variables
_redis_client = None
_pubsub = None
_client_lock = threading.RLock()
_pubsub_lock = threading.RLock()


def get_shared_redis_client() -> Optional[RedisService]:
    """Get or create a shared Redis client for the reporting flow."""
    global _redis_client
    with _client_lock:
        if _redis_client is None:
            try:
                from src.arbirich.services.redis.redis_service import get_shared_redis_client as global_get_client

                _redis_client = global_get_client()
                logger.info("Created shared Redis client for reporting flow")
            except Exception as e:
                logger.error(f"Error creating Redis client for reporting flow: {e}")
                return None
        return _redis_client


def get_pubsub():
    """Get or create a shared PubSub client."""
    global _pubsub
    with _pubsub_lock:
        if _pubsub is None:
            redis_client = get_shared_redis_client()
            if redis_client and hasattr(redis_client, "client") and redis_client.client:
                try:
                    _pubsub = redis_client.client.pubsub(ignore_subscribe_messages=True)
                    logger.info("Created PubSub client for reporting flow")
                except Exception as e:
                    logger.error(f"Error creating PubSub: {e}")
                    return None
            else:
                logger.warning("Cannot create PubSub - Redis client unavailable")
                return None
        return _pubsub


def reset_shared_redis_client() -> bool:
    """Reset the shared Redis client with proper cleanup."""
    global _redis_client, _pubsub

    # Log the operation
    logger.info("Resetting shared Redis client for reporting flow")

    # Close PubSub first if it exists
    with _pubsub_lock:
        if _pubsub is not None:
            try:
                _pubsub.unsubscribe()
                logger.info("Unsubscribed from all channels")
            except Exception as e:
                logger.warning(f"Error unsubscribing from channels: {e}")

            try:
                _pubsub.close()
                logger.info("Closed PubSub client")
            except Exception as e:
                logger.warning(f"Error closing PubSub client: {e}")

            _pubsub = None
            logger.info("PubSub reference cleared")

    # Then close the Redis client
    with _client_lock:
        client_to_close = _redis_client
        _redis_client = None

        if client_to_close is not None:
            try:
                client_to_close.close()
                logger.info("Closed Redis client")
            except Exception as e:
                logger.warning(f"Error closing Redis client: {e}")

    return True


async def safe_close_pubsub():
    """
    Safely close PubSub without using await on None objects.
    This prevents the 'NoneType can't be used in await expression' error.
    """
    global _pubsub

    with _pubsub_lock:
        if _pubsub is None:
            logger.debug("No PubSub to close")
            return True

        try:
            # Use synchronous methods instead of async to avoid await issues
            _pubsub.unsubscribe()
            _pubsub.close()
            _pubsub = None
            logger.info("PubSub closed successfully")
            return True
        except Exception as e:
            logger.error(f"Error closing PubSub: {e}")
            _pubsub = None
            return False


def sync_close_pubsub():
    """
    Synchronous version of safe_close_pubsub.
    Useful when being called from non-async contexts.
    """
    global _pubsub

    with _pubsub_lock:
        if _pubsub is None:
            logger.debug("No PubSub to close synchronously")
            return True

        try:
            _pubsub.unsubscribe()
            _pubsub.close()
            _pubsub = None
            logger.info("PubSub closed successfully (sync)")
            return True
        except Exception as e:
            logger.error(f"Error closing PubSub synchronously: {e}")
            _pubsub = None
            return False
