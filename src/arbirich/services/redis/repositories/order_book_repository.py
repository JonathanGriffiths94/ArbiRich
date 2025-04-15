import json
import logging
from typing import Dict, List, Optional

from redis.exceptions import RedisError

from src.arbirich.constants import ORDER_BOOK_CHANNEL, ORDER_BOOK_KEY_PREFIX
from src.arbirich.models.models import OrderBookUpdate

logger = logging.getLogger(__name__)


class OrderBookRepository:
    """Repository for managing order book data."""

    def __init__(self, redis_client):
        self.redis_client = redis_client

    def save(self, order_book: OrderBookUpdate) -> bool:
        """Store an order book in Redis with appropriate key."""
        try:
            key = f"{ORDER_BOOK_KEY_PREFIX}:{order_book.exchange}:{order_book.symbol}"

            data = order_book.model_dump()

            self.redis_client.set(key, json.dumps(data))

            self.redis_client.expire(key, 300)

            return True
        except RedisError as e:
            logger.error(f"Error storing order book for {order_book.exchange}:{order_book.symbol}: {e}")
            return False

    def get(self, exchange: str, symbol: str) -> Optional[OrderBookUpdate]:
        """Retrieve order book for a specific exchange and symbol."""
        try:
            key = f"{ORDER_BOOK_KEY_PREFIX}:{exchange}:{symbol}"
            data = self.redis_client.get(key)

            if not data:
                return None

            return OrderBookUpdate(**json.loads(data))
        except RedisError as e:
            logger.error(f"Error retrieving order book for {exchange}:{symbol}: {e}")
            return None

    def publish(self, order_book: OrderBookUpdate, exchange: str = None, symbol: str = None) -> int:
        """Publish an order book update to the appropriate channel."""
        try:
            exchange = exchange or order_book.exchange
            symbol = symbol or order_book.symbol

            channel = f"{ORDER_BOOK_CHANNEL}:{exchange}:{symbol}"

            data = order_book.model_dump()

            result = self.redis_client.publish(channel, json.dumps(data))
            return result
        except RedisError as e:
            logger.error(f"Error publishing order book for {exchange}:{symbol}: {e}")
            return 0

    def get_latest_all_exchanges(self, symbol: str) -> Dict[str, OrderBookUpdate]:
        """Get the latest order book for a specific symbol across all exchanges."""
        try:
            pattern = f"{ORDER_BOOK_KEY_PREFIX}:*:{symbol}"
            keys = self.redis_client.keys(pattern)

            result = {}
            for key in keys:
                # Extract exchange from the key
                key_parts = key.decode("utf-8").split(":")
                if len(key_parts) < 3:
                    continue

                exchange = key_parts[1]
                order_book = self.get(exchange, symbol)
                if order_book:
                    result[exchange] = order_book

            return result
        except RedisError as e:
            logger.error(f"Error retrieving order books for all exchanges with symbol {symbol}: {e}")
            return {}

    def get_exchanges_with_symbol(self, symbol: str) -> List[str]:
        """Get all exchanges that have order books for a specific symbol."""
        try:
            pattern = f"{ORDER_BOOK_KEY_PREFIX}:*:{symbol}"
            keys = self.redis_client.keys(pattern)

            exchanges = []
            for key in keys:
                key_parts = key.decode("utf-8").split(":")
                if len(key_parts) >= 3:
                    exchanges.append(key_parts[1])

            return exchanges
        except RedisError as e:
            logger.error(f"Error retrieving exchanges with symbol {symbol}: {e}")
            return []
