import logging
from typing import Dict, Optional

from src.arbirich.services.redis_service import RedisService

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set to DEBUG for more detailed logs

# Create a singleton instance of RedisService to be shared by all functions
_redis_service = RedisService()


def publish_order_book(exchange: str, symbol: str, order_book):
    """
    Publish an order book to Redis.

    Parameters:
        exchange: The exchange name
        symbol: The trading pair symbol
        order_book: The order book data to publish (dict or Pydantic model)

    Returns:
        True if publishing was successful, False otherwise
    """
    try:
        logger.debug(f"Publishing order book for {exchange}:{symbol}")

        # Convert to proper format if needed
        if hasattr(order_book, "model_dump"):
            data_to_publish = order_book.model_dump()
        elif hasattr(order_book, "dict"):
            data_to_publish = order_book.dict()
        else:
            data_to_publish = order_book

        # Ensure we have the basic fields
        if isinstance(data_to_publish, dict):
            if "exchange" not in data_to_publish:
                data_to_publish["exchange"] = exchange
            if "symbol" not in data_to_publish:
                data_to_publish["symbol"] = symbol

            # Ensure bids and asks are in the correct format
            if "bids" in data_to_publish and not isinstance(data_to_publish["bids"], dict):
                logger.warning(f"Bids is not a dictionary: {type(data_to_publish['bids'])}")
                data_to_publish["bids"] = {}

            if "asks" in data_to_publish and not isinstance(data_to_publish["asks"], dict):
                logger.warning(f"Asks is not a dictionary: {type(data_to_publish['asks'])}")
                data_to_publish["asks"] = {}

            # Now publish
            logger.debug(f"Publishing data to Redis: {exchange}:{symbol}")
            result = _redis_service.publish_order_book(exchange, symbol, data_to_publish)

            if result > 0:
                logger.info(f"Successfully published order book for {exchange}:{symbol} to {result} subscribers")
                return True
            else:
                logger.warning(f"Order book published but no subscribers for {exchange}:{symbol}")
                return True  # Still consider it a success even without subscribers
        else:
            logger.error(f"Data to publish is not a dictionary: {type(data_to_publish)}")
            return False
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
            # Extract exchange and symbol from the item
            if hasattr(item, "exchange") and hasattr(item, "symbol"):
                # Item is a Pydantic model
                exchange = item.exchange
                symbol = item.symbol
                order_book = item
            else:
                # Item is a dict
                exchange = item.get("exchange")
                symbol = item.get("symbol")
                order_book = item

            if not exchange or not symbol:
                logger.error("Missing required fields exchange or symbol in item")
                return None

            # Debug the input to see what's being passed
            logger.debug(f"Publishing order book for {exchange}:{symbol}")

            # Call the publish_order_book function with all required parameters
            result = publish_order_book(exchange, symbol, order_book)

            if result:
                logger.debug(f"Successfully published order book for {exchange}:{symbol}")
                return True
            else:
                logger.warning(f"Failed to publish order book for {exchange}:{symbol}")
                return False
        except Exception as e:
            logger.error(f"Error in RedisOrderBookSink.__call__: {e}", exc_info=True)
            return False
