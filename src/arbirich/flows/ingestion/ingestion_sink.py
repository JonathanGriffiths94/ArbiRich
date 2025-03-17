import json
import logging
import time
from typing import Dict, Optional

from src.arbirich.models.models import OrderBookUpdate
from src.arbirich.services.redis_service import RedisService

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create a singleton instance of RedisService to be shared by all functions
_redis_service = RedisService()


def publish_order_book(order_book: OrderBookUpdate) -> bool:
    """
    Publish an order book to Redis.

    Parameters:
        order_book: The OrderBookUpdate object to publish

    Returns:
        True if publishing was successful, False otherwise
    """
    try:
        exchange = order_book.exchange
        symbol = order_book.symbol
        logger.debug(f"Publishing order book for {exchange}:{symbol}")

        # Convert to dict for consistent serialization
        if hasattr(order_book, "model_dump"):
            data_dict = order_book.model_dump()
        elif hasattr(order_book, "dict"):
            data_dict = order_book.dict()
        else:
            data_dict = {
                "exchange": exchange,
                "symbol": symbol,
                "bids": order_book.bids if hasattr(order_book, "bids") else {},
                "asks": order_book.asks if hasattr(order_book, "asks") else {},
                "timestamp": order_book.timestamp if hasattr(order_book, "timestamp") else time.time(),
            }

        # Convert to JSON for publishing
        json_data = json.dumps(data_dict)

        # Publish to all relevant channels
        channels = [
            f"order_book:{exchange}:{symbol}",  # Old format
            f"order_book.{symbol}.{exchange}",  # New format
            "order_book",  # Generic channel
        ]

        results = []
        for channel in channels:
            try:
                result = _redis_service.client.publish(channel, json_data)
                results.append(result)
                logger.debug(f"Published to {channel}: {result} subscribers")
            except Exception as e:
                logger.error(f"Error publishing to {channel}: {e}")

        return any(result > 0 for result in results)
    except Exception as e:
        logger.error(f"Error publishing order book: {e}", exc_info=True)
        return False


class RedisOrderBookSink:
    """Sink for publishing order book updates to Redis."""

    def __init__(self):
        self.redis_service = _redis_service  # Use the singleton instance

    def __call__(self, item: Optional[Dict] = None):
        """
        Publish an order book to Redis.

        Parameters:
            item: The order book data (can be a dict or an OrderBookUpdate model)
        """
        if not item:
            logger.warning("Received None item to publish")
            return None

        try:
            exchange = item.exchange
            symbol = item.symbol
            order_book = item

            # Debug the input to see what's being passed
            logger.debug(f"Publishing order book for {exchange}:{symbol}")
            result = publish_order_book(order_book)

            if result:
                logger.debug(f"Successfully published order book for {exchange}:{symbol}")
                return True
            else:
                logger.warning(f"Failed to publish order book for {exchange}:{symbol}")
                return False
        except Exception as e:
            logger.error(f"Error in RedisOrderBookSink.__call__: {e}", exc_info=True)
            return False
