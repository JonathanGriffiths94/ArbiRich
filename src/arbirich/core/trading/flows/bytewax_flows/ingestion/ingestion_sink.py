import logging
import threading
from typing import Dict, Optional

from src.arbirich.core.state.system_state import is_system_shutting_down
from src.arbirich.services.redis.redis_service import RedisService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Set to DEBUG for more detailed logs

# Create a singleton instance of RedisService to be shared by all functions
_shared_redis_client = None
_is_shutting_down = False
_shutdown_lock = threading.Lock()
_redis_lock = threading.RLock()  # Use RLock for nested acquire


def get_shared_redis_client():
    """Get or create a shared Redis client for this module with shutdown awareness"""
    global _shared_redis_client, _is_shutting_down

    # Quick check for shutdown state without lock contention
    if _is_shutting_down or is_system_shutting_down():
        logger.debug("Skipping Redis client creation - system is shutting down")
        return None

    with _redis_lock:
        # Double-check shutdown after acquiring lock
        if _is_shutting_down or is_system_shutting_down():
            logger.debug("Skipping Redis client creation (locked check) - system is shutting down")
            return None

        if _shared_redis_client is None:
            try:
                _shared_redis_client = RedisService()
                logger.debug("Created new Redis client for ingestion sink")
            except Exception as e:
                logger.error(f"Error creating Redis client: {e}")
                return None
        return _shared_redis_client


def reset_shared_redis_client():
    """Reset the shared Redis client for clean restarts with better cleanup"""
    global _shared_redis_client, _is_shutting_down

    logger.info("Resetting shared Redis client for ingestion sink")

    # Mark as shutting down first to prevent new usage
    with _shutdown_lock:
        _is_shutting_down = True

    # Then handle the actual client
    with _redis_lock:
        client_to_close = _shared_redis_client
        _shared_redis_client = None

        if client_to_close is not None:
            try:
                client_to_close.close()
                logger.info("Closed shared Redis client in ingestion sink")
            except Exception as e:
                logger.warning(f"Error closing Redis client: {e}")

    return True


def mark_shutdown_started():
    """Mark the system as shutting down to prevent new publishes"""
    global _is_shutting_down
    with _shutdown_lock:
        _is_shutting_down = True
        logger.info("Ingestion sink marked as shutting down")


def is_shutting_down():
    """Check if system is in shutdown mode (local or global)"""
    # Check both our local flag and the system-wide flag
    return _is_shutting_down or is_system_shutting_down()


def get_order_book_channel(exchange, symbol):
    """Get standardized channel name for order book data"""
    return f"order_book:{exchange}:{symbol}"


def publish_order_book(exchange: str, symbol: str, order_book):
    """
    Publish an order book to Redis with better shutdown handling.
    """
    # Check both shutdown flags first - fast early return
    if is_shutting_down():
        logger.debug(f"System shutting down, skipping publish for {exchange}:{symbol}")
        return False

    # Get the shared Redis client - might return None during shutdown
    redis_service = get_shared_redis_client()
    if not redis_service:
        logger.debug(f"No Redis client available for {exchange}:{symbol}, skipping publish")
        return False

    # Additional closed check
    if hasattr(redis_service, "_closed") and redis_service._closed:
        logger.debug(f"Redis service is closed for {exchange}:{symbol}, skipping publish")
        return False

    try:
        # Final shutdown check before publish attempt
        if is_shutting_down():
            return False

        logger.debug(f"Publishing order book for {exchange}:{symbol}")

        # Convert to proper format if needed
        if hasattr(order_book, "model_dump"):
            data_to_publish = order_book.model_dump()
        elif hasattr(order_book, "dict"):
            data_to_publish = order_book.dict()
        else:
            data_to_publish = order_book

        # Ensure we have the basic fields and publish
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

            # Final shutdown check right before publishing
            if is_shutting_down():
                return False

            # Now publish
            result = redis_service.publish_order_book(exchange, symbol, order_book)

            if result > 0:
                logger.info(f"Successfully published order book for {exchange}:{symbol} to {result} subscribers")
                return True
            else:
                logger.debug(f"Order book published but no subscribers for {exchange}:{symbol}")
                return True  # Still consider it a success even without subscribers
        else:
            logger.error(f"Data to publish is not a dictionary: {type(data_to_publish)}")
            return False
    except AttributeError as e:
        # Don't spam logs during shutdown
        if is_shutting_down():
            return False

        # This is likely due to Redis shutdown - handle gracefully
        if "NoneType" in str(e) and ("publish" in str(e) or "feed" in str(e)):
            logger.debug(f"Redis connection closed, skipping publish for {exchange}:{symbol}")
        else:
            logger.error(f"Error publishing order book: {e}")
        return False
    except Exception as e:
        # Don't spam logs during shutdown
        if is_shutting_down():
            return False

        if "Bad file descriptor" in str(e) or "Connection reset" in str(e):
            logger.debug(f"Redis connection closed during publish: {e}")
        else:
            logger.error(f"Error publishing order book: {e}", exc_info=True)
        return False


class RedisOrderBookSink:
    """Sink for publishing order book updates to Redis."""

    def __init__(self):
        # Get a Redis client but handle shutdown scenario
        self.redis_service = None
        if not is_shutting_down():
            self.redis_service = get_shared_redis_client()

    def __call__(self, item: Optional[Dict] = None):
        """
        Publish an order book to Redis with shutdown awareness.
        """
        # Skip processing during shutdown
        if is_shutting_down():
            return None

        if not item:
            logger.debug("Received None item to publish")
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

            # Call the publish_order_book function with all required parameters
            result = publish_order_book(exchange, symbol, order_book)
            return result

        except Exception as e:
            # Skip error logging during shutdown
            if is_shutting_down():
                return False

            logger.error(f"Error in RedisOrderBookSink.__call__: {e}", exc_info=True)
            return False
