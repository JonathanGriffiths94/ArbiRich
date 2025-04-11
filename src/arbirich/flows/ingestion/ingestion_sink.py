import logging
import threading
from typing import Dict, Optional

from src.arbirich.services.redis.redis_service import RedisService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Set to DEBUG for more detailed logs

# Create a singleton instance of RedisService to be shared by all functions
_shared_redis_client = None
_is_shutting_down = False
_shutdown_lock = threading.Lock()
_redis_lock = threading.Lock()


def get_shared_redis_client():
    """Get or create a shared Redis client for this module"""
    global _shared_redis_client
    with _redis_lock:
        if _shared_redis_client is None:
            try:
                _shared_redis_client = RedisService()
                logger.debug("Created new Redis client for ingestion sink")
            except Exception as e:
                logger.error(f"Error creating Redis client: {e}")
                return None
        return _shared_redis_client


def reset_shared_redis_client():
    """Reset the shared Redis client for clean restarts"""
    global _shared_redis_client
    with _redis_lock:
        if _shared_redis_client is not None:
            try:
                _shared_redis_client.close()
                logger.info("Closed shared Redis client in ingestion sink")
            except Exception as e:
                logger.warning(f"Error closing Redis client: {e}")
            _shared_redis_client = None


def mark_shutdown_started():
    """Mark the system as shutting down to prevent new publishes"""
    global _is_shutting_down
    with _shutdown_lock:
        _is_shutting_down = True
        logger.info("Ingestion sink marked as shutting down")


def is_shutting_down():
    """Check if system is in shutdown mode"""
    with _shutdown_lock:
        return _is_shutting_down


def get_order_book_channel(exchange, symbol):
    """Get standardized channel name for order book data"""
    return f"order_book:{exchange}:{symbol}"


def publish_order_book(exchange: str, symbol: str, order_book):
    """
    Publish an order book to Redis.

    Parameters:
        exchange: The exchange name
        symbol: The trading pair symbol
        order_book: The order book data to publish

    Returns:
        True if publishing was successful, False otherwise
    """
    # Check global shutdown flag first - fast early return
    if is_shutting_down():
        logger.debug(f"System shutting down, skipping publish for {exchange}:{symbol}")
        return False

    # Get the shared Redis client
    redis_service = get_shared_redis_client()

    # Check if Redis service is available
    if not redis_service or hasattr(redis_service, "_closed") and redis_service._closed:
        logger.debug(f"Skipping publish for {exchange}:{symbol} - Redis service is closed")
        return False

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
            result = redis_service.publish_order_book(exchange, symbol, order_book)

            if result > 0:
                logger.info(f"Successfully published order book for {exchange}:{symbol} to {result} subscribers")
                return True
            else:
                # Reduce warning level since this is normal during testing/development
                logger.debug(f"Order book published but no subscribers for {exchange}:{symbol}")
                return True  # Still consider it a success even without subscribers
        else:
            logger.error(f"Data to publish is not a dictionary: {type(data_to_publish)}")
            return False
    except AttributeError as e:
        # This is likely due to Redis shutdown - handle gracefully
        if "NoneType" in str(e) and ("publish" in str(e) or "feed" in str(e)):
            logger.debug(f"Redis connection closed, skipping publish for {exchange}:{symbol}")
        else:
            logger.error(f"Error publishing order book: {e}")
        return False
    except Exception as e:
        if "Bad file descriptor" in str(e) or "Connection reset" in str(e):
            logger.debug(f"Redis connection closed during publish: {e}")
        else:
            logger.error(f"Error publishing order book: {e}", exc_info=True)
        return False


class RedisOrderBookSink:
    """Sink for publishing order book updates to Redis."""

    def __init__(self):
        self.redis_service = get_shared_redis_client()

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
