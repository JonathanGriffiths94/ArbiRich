import hashlib
import json
import logging

from arbirich.models.dtos import OrderBookUpdate
from src.arbirich.config import REDIS_CONFIG
from src.arbirich.redis_manager import MarketDataService

logger = logging.getLogger(__name__)

redis_client = MarketDataService(
    host=REDIS_CONFIG["host"],
    port=REDIS_CONFIG["port"],
    db=REDIS_CONFIG["db"],
)

# Global cache for deduplication
LAST_ORDER_BOOK = {}


def normalise_order_book(order_book: OrderBookUpdate) -> dict:
    return {
        "exchange": order_book.exchange,
        "symbol": order_book.symbol,
        "timestamp": order_book.timestamp,
        "bids": [order.model_dump() for order in order_book.bids],
        "asks": [order.model_dump() for order in order_book.asks],
    }


def get_order_book_hash(order_book: OrderBookUpdate) -> str:
    data = json.dumps(
        {
            "bids": order_book["bids"],
            "asks": order_book["asks"],
        },
        sort_keys=True,
    )
    return hashlib.md5(data.encode("utf-8")).hexdigest()


def store_order_book(order_book: OrderBookUpdate):
    """
    Store and publish the processed order book data.
    Deduplicate by comparing a hash of the order book.
    """
    if not order_book:
        return None

    normalised = normalise_order_book(order_book)

    key = (order_book.exchange, order_book.symbol)
    new_hash = get_order_book_hash(normalised)
    last_hash = LAST_ORDER_BOOK.get(key)

    if new_hash == last_hash:
        logger.debug(f"Duplicate update for {key}, skipping publishing.")
        return normalised

    LAST_ORDER_BOOK[key] = new_hash

    redis_client.publish_order_book(
        normalised["exchange"],
        normalised["symbol"],
        normalised["bids"],
        normalised["asks"],
        normalised["timestamp"],
    )
    logger.info(
        f"Successfully published to 'order_book' channel: {normalised['exchange']}:{normalised['symbol']} - {normalised['timestamp']}"
    )

    redis_client.store_order_book(
        normalised["exchange"],
        normalised["symbol"],
        normalised["bids"],
        normalised["asks"],
        normalised["timestamp"],
    )
    logger.info(
        f"Successfully pushed to cache: {normalised['exchange']}:{normalised['symbol']} - {normalised['timestamp']}"
    )
    return normalised
