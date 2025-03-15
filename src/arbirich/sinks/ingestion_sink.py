import logging

from src.arbirich.models.models import OrderBookUpdate
from src.arbirich.sources.arbitrage_source import get_shared_redis_client

logger = logging.getLogger(__name__)

# Use the shared Redis client instead of creating a new one
redis_client = get_shared_redis_client()

# Global cache for deduplication
LAST_ORDER_BOOK = {}


def store_order_book(order_book: OrderBookUpdate):
    """
    Store and publish the processed order book data.
    Deduplicate by comparing a hash of the order book.
    """
    if not order_book:
        return None

    key = (order_book.exchange, order_book.symbol)

    last_hash = LAST_ORDER_BOOK.get(key)

    if order_book.hash == last_hash:
        logger.debug(f"Duplicate update for {key}, skipping publishing.")
        return order_book

    LAST_ORDER_BOOK[key] = order_book.hash

    try:
        redis_client.publish_order_book(order_book)
        logger.debug(
            f"Successfully published to 'order_book' channel: {order_book.symbol}:{order_book.exchange} - {order_book.timestamp}"
        )

        redis_client.store_order_book(order_book)
        logger.debug(
            f"Successfully pushed to cache: {order_book.symbol}:{order_book.exchange} - {order_book.timestamp}"
        )
    except Exception as e:
        logger.error(f"Error storing/publishing order book: {e}")
        # If Redis connection is unhealthy, try to reconnect
        if not redis_client.is_healthy():
            logger.warning("Redis connection unhealthy, attempting to reconnect...")
            redis_client.reconnect_if_needed()

    return order_book
