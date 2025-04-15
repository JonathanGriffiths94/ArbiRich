import logging
import threading
from typing import Dict, List

import redis

from src.arbirich.config.config import REDIS_CONFIG
from src.arbirich.models.models import OrderBookUpdate, TradeExecution, TradeOpportunity
from src.arbirich.services.redis.repositories.metrics_repository import MetricsRepository
from src.arbirich.services.redis.repositories.order_book_repository import OrderBookRepository
from src.arbirich.services.redis.repositories.trade_execution_repository import TradeExecutionRepository
from src.arbirich.services.redis.repositories.trade_opportunity_repository import TradeOpportunityRepository
from src.arbirich.utils.redis_cleanup import register_redis_client

logger = logging.getLogger(__name__)

# Global tracking for Redis clients
_redis_clients = []
_redis_lock = threading.RLock()

# Shared Redis client instance
_shared_redis_client = None
_shared_redis_lock = threading.RLock()


def is_redis_connection_valid(redis_client) -> bool:
    """
    Check if a Redis connection is valid and usable.

    Args:
        redis_client: The Redis client to check

    Returns:
        bool: True if the connection is valid, False otherwise
    """
    if not redis_client:
        return False

    try:
        # Try to access the client property to see if it exists
        if hasattr(redis_client, "client") and redis_client.client:
            # Try a ping operation with a short timeout
            result = redis_client.client.ping()
            return bool(result)
        elif hasattr(redis_client, "ping"):
            # Direct client object
            result = redis_client.ping()
            return bool(result)
        return False
    except Exception as e:
        logger.debug(f"Redis connection check failed: {e}")
        return False


class RedisService:
    """Service for interacting with Redis."""

    def __init__(self):
        """Initialize Redis service with connection and repositories."""
        try:
            # Get Redis config
            host = REDIS_CONFIG.get("host", "localhost")
            port = REDIS_CONFIG.get("port", 6379)
            db = REDIS_CONFIG.get("db", 0)

            # Create Redis client
            self.client = redis.Redis(host=host, port=port, db=db, decode_responses=False)

            # Test the connection
            self.client.ping()
            logger.info(f"Connected to Redis at {host}:{port}")

            # Register client for tracking
            with _redis_lock:
                _redis_clients.append(self.client)

            # Initialize repositories
            self.order_book_repository = OrderBookRepository(self.client)
            self.trade_opportunity_repository = TradeOpportunityRepository(self.client)
            self.trade_execution_repository = TradeExecutionRepository(self.client)
            self.metrics_repository = MetricsRepository(self.client)

        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    def close(self):
        """Close the Redis connection."""
        try:
            # First deregister the client
            with _redis_lock:
                if self.client in _redis_clients:
                    _redis_clients.remove(self.client)

            # Then close the connection
            self.client.close()
            logger.debug("Redis connection closed")
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}")

    def is_healthy(self) -> bool:
        """Check if the Redis connection is healthy and usable."""
        try:
            if self.client:
                # Ping with a short timeout
                result = self.client.ping()
                return bool(result)
            return False
        except Exception as e:
            logger.warning(f"Redis health check failed: {e}")
            return False

    # Repository accessor methods
    def get_order_book_repository(self) -> OrderBookRepository:
        """Get the order book repository."""
        return self.order_book_repository

    def get_trade_opportunity_repository(self) -> TradeOpportunityRepository:
        """Get the trade opportunity repository."""
        return self.trade_opportunity_repository

    def get_trade_execution_repository(self) -> TradeExecutionRepository:
        """Get the trade execution repository."""
        return self.trade_execution_repository

    def get_metrics_repository(self) -> MetricsRepository:
        """Get the metrics repository."""
        return self.metrics_repository

    # === Compatibility methods for order books ===
    def store_order_book(self, order_book: OrderBookUpdate) -> None:
        """Store order book using repository pattern."""
        self.order_book_repository.save(order_book)

    def publish_order_book(self, exchange: str, symbol: str, order_book: OrderBookUpdate) -> int:
        """Publish an order book update using repository pattern."""
        return self.order_book_repository.publish(order_book, exchange, symbol)

    # === Compatibility methods for trade opportunities ===
    def store_trade_opportunity(self, opportunity: TradeOpportunity) -> None:
        """Store trade opportunity using repository pattern."""
        self.trade_opportunity_repository.save(opportunity)

    def publish_trade_opportunity(self, opportunity: TradeOpportunity) -> int:
        """Publish a trade opportunity using repository pattern."""
        return self.trade_opportunity_repository.publish(opportunity)

    # === Compatibility methods for trade executions ===
    def store_trade_execution(self, execution: TradeExecution) -> None:
        """Store trade execution using repository pattern."""
        self.trade_execution_repository.save(execution)

    def publish_trade_execution(self, execution: TradeExecution) -> int:
        """Publish a trade execution using repository pattern."""
        return self.trade_execution_repository.publish(execution)

    # === Compatibility methods for metrics ===
    def record_metric(self, metric_name: str, value: float) -> bool:
        """Record a metric using repository pattern."""
        return self.metrics_repository.record_metric(metric_name, value)

    def get_recent_metrics(self, metric_name: str, count: int = 60) -> List[Dict]:
        """Get recent metrics using repository pattern."""
        return self.metrics_repository.get_recent_metrics(metric_name, count)


def get_shared_redis_client():
    """
    Get or create a shared Redis client for use across the application.

    Returns:
        RedisService: A shared instance of RedisService
    """
    global _shared_redis_client

    with _shared_redis_lock:
        if _shared_redis_client is None:
            try:
                _shared_redis_client = RedisService()
                logger.debug("Created new shared Redis client")
                # Register with cleanup utility
                register_redis_client(_shared_redis_client)
            except Exception as e:
                logger.error(f"Failed to create shared Redis client: {e}")
                return None

        return _shared_redis_client


def reset_redis_pool():
    """
    Reset all Redis connection pools and close existing connections.

    Returns:
        bool: True if successful, False otherwise
    """
    global _shared_redis_client

    try:
        # Close shared client if it exists
        with _shared_redis_lock:
            if _shared_redis_client is not None:
                try:
                    _shared_redis_client.close()
                except Exception as e:
                    logger.warning(f"Error closing shared Redis client: {e}")
                _shared_redis_client = None

        # Close all tracked clients
        with _redis_lock:
            for client in _redis_clients[:]:
                try:
                    client.close()
                except Exception as e:
                    logger.warning(f"Error closing Redis client: {e}")
                _redis_clients.remove(client)

        # Reset connection pools in redis package
        try:
            pools = getattr(redis.Redis, "connection_pool", {}).get("pools", {})
            for key, pool in pools.items():
                try:
                    pool.disconnect()
                    logger.debug(f"Disconnected Redis connection pool: {key}")
                except Exception as e:
                    logger.warning(f"Error disconnecting pool {key}: {e}")
        except Exception as e:
            logger.warning(f"Error resetting Redis connection pools: {e}")

        logger.info("All Redis connections and pools reset")
        return True
    except Exception as e:
        logger.error(f"Error in reset_redis_pool: {e}")
        return False


def reset_all_registered_redis_clients() -> bool:
    """
    Reset all registered Redis clients and clean up resources.
    This is a coordinated cleanup that uses the redis_cleanup utilities.

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # First reset the Redis pool to close active connections
        reset_success = reset_redis_pool()

        # Then use the redis_cleanup utility to clean up all registered resources
        from src.arbirich.utils.redis_cleanup import cleanup_all_redis_resources

        cleanup_success = cleanup_all_redis_resources()

        # Reset the shared client
        global _shared_redis_client
        with _shared_redis_lock:
            _shared_redis_client = None

        logger.info("All registered Redis clients reset and cleaned up")
        return reset_success and cleanup_success
    except Exception as e:
        logger.error(f"Error resetting all registered Redis clients: {e}")
        return False


async def check_redis_health(redis_client, pubsub=None, channels=None) -> bool:
    """
    Check if Redis connection and subscriptions are healthy.

    Args:
        redis_client: The Redis client to check
        pubsub: Optional Redis PubSub instance to verify
        channels: Optional list of channels to verify subscription status

    Returns:
        True if healthy, False otherwise
    """
    if not redis_client:
        logger.error("❌ Redis client not available")
        return False

    try:
        # Check if it's a RedisService or a direct Redis client
        if hasattr(redis_client, "is_healthy"):
            # Use the built-in health check method
            if not redis_client.is_healthy():
                logger.error("❌ Redis health check failed using is_healthy()")
                return False
        else:
            # Direct Redis client - check ping
            try:
                if hasattr(redis_client, "ping"):
                    # Direct Redis client
                    if not redis_client.ping():
                        logger.error("❌ Redis ping failed")
                        return False
                elif hasattr(redis_client, "client") and hasattr(redis_client.client, "ping"):
                    # Redis client wrapped in a service
                    if not redis_client.client.ping():
                        logger.error("❌ Redis ping failed")
                        return False
                else:
                    logger.error("❌ Unknown Redis client type, can't check health")
                    return False
            except Exception as e:
                logger.error(f"❌ Error pinging Redis: {e}")
                return False

        # Check PubSub status if provided
        if pubsub:
            try:
                # Validate PubSub connection
                pubsub_is_valid = not pubsub.connection.closed
                if not pubsub_is_valid:
                    logger.error("❌ PubSub connection is closed")
                    return False

                # Check subscriptions if channels were provided
                if channels:
                    subscriptions = pubsub.channels
                    for channel in channels:
                        # For bytes channels (Redis internal representation)
                        if isinstance(channel, str) and channel.encode("utf-8") not in subscriptions:
                            # For string channels (user-friendly representation)
                            if channel not in subscriptions:
                                logger.warning(f"⚠️ Not subscribed to channel: {channel}")

            except Exception as e:
                logger.error(f"❌ Error checking PubSub status: {e}")
                return False

        # All checks passed
        return True

    except Exception as e:
        logger.error(f"❌ Redis health check failed: {e}")
        return False
