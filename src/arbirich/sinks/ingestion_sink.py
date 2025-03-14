import logging

from src.arbirich.models.models import OrderBookUpdate
from src.arbirich.services.redis_service import RedisService

logger = logging.getLogger(__name__)

redis_client = RedisService()

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

    redis_client.publish_order_book(order_book)
    logger.info(
        f"Successfully published to 'order_book' channel: {order_book.symbol}:{order_book.exchange} - {order_book.timestamp}"
    )

    redis_client.store_order_book(order_book)
    logger.info(f"Successfully pushed to cache: {order_book.symbol}:{order_book.exchange} - {order_book.timestamp}")
    return order_book
