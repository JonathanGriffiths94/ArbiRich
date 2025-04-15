import logging
import threading
from typing import Dict, Tuple

from src.arbirich.core.state.system_state import is_system_shutting_down
from src.arbirich.models.models import OrderBookUpdate
from src.arbirich.services.redis.redis_channel_manager import get_channel_manager
from src.arbirich.services.redis.redis_service import RedisService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Set to DEBUG for more detailed logs

# Create a singleton instance of RedisService to be shared by all functions
_shared_redis_client = None
_is_shutting_down = False
_shutdown_lock = threading.Lock()
_redis_lock = threading.RLock()


def get_shared_redis_client():
    """Get or create a shared Redis client"""
    global _shared_redis_client

    if is_shutting_down():
        return None

    with _redis_lock:
        if _shared_redis_client is None and not is_shutting_down():
            try:
                _shared_redis_client = RedisService()
                logger.debug("🔌 Created new Redis client for ingestion sink")
            except Exception as e:
                logger.error(f"❌ Error creating Redis client: {e}")
                return None
        return _shared_redis_client


def reset_shared_redis_client():
    """Reset the shared Redis client"""
    global _shared_redis_client, _is_shutting_down

    with _shutdown_lock:
        _is_shutting_down = True

    with _redis_lock:
        client = _shared_redis_client
        _shared_redis_client = None

        if client:
            try:
                client.close()
                logger.info("🔌 Closed shared Redis client")
            except Exception as e:
                logger.warning(f"⚠️ Error closing Redis client: {e}")

    return True


def mark_shutdown_started():
    """Mark the system as shutting down"""
    global _is_shutting_down
    with _shutdown_lock:
        _is_shutting_down = True


def is_shutting_down():
    """Check if system is in shutdown mode"""
    return _is_shutting_down or is_system_shutting_down()


def publish_order_book(exchange: str, symbol: str, order_book: OrderBookUpdate):
    """Publish an order book to Redis"""
    if is_shutting_down():
        return False

    redis_service = get_shared_redis_client()
    if not redis_service:
        return False

    try:
        # Assume order_book is always an OrderBookUpdate model
        # Get the channel manager
        channel_manager = get_channel_manager()
        if not channel_manager:
            logger.error("❌ Failed to get Redis channel manager")
            return False

        # Use the channel manager to publish
        result = channel_manager.publish_order_book(order_book, exchange, symbol)

        # Consider publication successful even if there are no subscribers yet
        # (Redis publish returns the number of subscribers that received the message)
        logger.debug(f"Published order book with {result} subscribers")
        return True  # Return True as long as the publish operation didn't fail

    except Exception as e:
        if not is_shutting_down():
            logger.error(f"❌ Error publishing order book: {e}")
        return False


def store_order_book(exchange: str, symbol: str, order_book: OrderBookUpdate) -> bool:
    """
    Store an order book in Redis cache.

    This overwrites any previous order book for the same exchange/symbol pair,
    ensuring we only keep the latest state in cache.
    """
    if is_shutting_down():
        return False

    redis_service = get_shared_redis_client()
    if not redis_service:
        return False

    try:
        # Assume order_book is always an OrderBookUpdate model
        # Store using repository pattern
        order_book_repo = redis_service.get_order_book_repository()
        result = order_book_repo.save(order_book)

        if result:
            logger.debug(f"💾 Updated current order book state for {exchange}:{symbol}")

        return result

    except Exception as e:
        if not is_shutting_down():
            logger.error(f"❌ Error storing order book: {e}")
        return False


def process_order_book(exchange: str, symbol: str, order_book: OrderBookUpdate) -> Tuple[bool, int]:
    """
    Process an order book - store the current state in Redis and publish to subscribers.

    For each exchange-symbol pair, only the latest order book is stored in Redis.
    """
    if is_shutting_down():
        return False, 0

    try:
        # Assume order_book is already an OrderBookUpdate model
        # Get the channel manager
        channel_manager = get_channel_manager()
        if not channel_manager:
            logger.error("❌ Failed to get Redis channel manager")
            return False, 0

        # Get Redis service
        redis_service = get_shared_redis_client()
        if not redis_service:
            return False, 0

        # 1. Store current state in Redis cache (overwrites previous state)
        store_result = redis_service.get_order_book_repository().save(order_book)

        # 2. Publish to subscribers
        publish_result = channel_manager.publish_order_book(order_book, exchange, symbol)

        return store_result, publish_result

    except Exception as e:
        if not is_shutting_down():
            logger.error(f"❌ Error processing order book: {e}")
        return False, 0


class RedisOrderBookSink:
    """Sink for processing order book updates - saves current state and publishes updates."""

    def __init__(self):
        self.redis_service = get_shared_redis_client()

    def __call__(self, item: Dict = None):
        """
        Process an order book - store current state and publish to subscribers.

        For each exchange-symbol pair, only the most recent order book is stored in Redis,
        overwriting any previous state for that pair.
        """
        if is_shutting_down() or not item:
            return None

        try:
            # Extract exchange and symbol
            exchange = item.get("exchange", None) if isinstance(item, dict) else getattr(item, "exchange", None)
            symbol = item.get("symbol", None) if isinstance(item, dict) else getattr(item, "symbol", None)

            if not exchange or not symbol:
                logger.error("❌ Missing exchange or symbol in item")
                return None

            # Process the order book - store current state and publish
            store_success, subscriber_count = process_order_book(exchange, symbol, item)

            if store_success:
                logger.debug(f"💾 Updated current order book state for {exchange}:{symbol}")

            if subscriber_count > 0:
                logger.debug(f"📢 Order book for {exchange}:{symbol} published to {subscriber_count} subscribers")

            return store_success and subscriber_count > 0

        except Exception as e:
            if not is_shutting_down():
                logger.error(f"❌ Error in RedisOrderBookSink: {e}")
            return False
