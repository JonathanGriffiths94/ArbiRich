import json
import logging
import threading
import time
from typing import Any, Callable, Dict, Generator, List, Optional, Set

import redis
from redis.exceptions import ConnectionError, RedisError

from src.arbirich.config.config import REDIS_CONFIG
from src.arbirich.constants import ORDER_BOOK_CHANNEL, TRADE_EXECUTIONS_CHANNEL, TRADE_OPPORTUNITIES_CHANNEL
from src.arbirich.models.models import OrderBookUpdate, TradeExecution, TradeOpportunity

logger = logging.getLogger(__name__)

_redis_clients = []
_redis_lock = threading.RLock()

_shared_redis_client = None

# Global registry of Redis clients to reset
_redis_clients_registry = {}


def register_redis_client(module_name, reset_function):
    """Register a module's Redis client reset function."""
    _redis_clients_registry[module_name] = reset_function


def reset_all_registered_redis_clients():
    """Reset all registered Redis clients across modules."""
    for module_name, reset_function in _redis_clients_registry.items():
        try:
            reset_function()
            logger.info(f"Reset Redis client for {module_name}")
        except Exception as e:
            logger.error(f"Error resetting Redis client for {module_name}: {e}")

    # Also reset the channel manager
    try:
        from src.arbirich.services.redis.redis_channel_manager import reset_channel_manager

        reset_channel_manager()
    except Exception as e:
        logger.error(f"Error resetting channel manager: {e}")


def get_shared_redis_client():
    """Get or create a shared Redis client for the application."""
    global _shared_redis_client
    with _redis_lock:
        if _shared_redis_client is None:
            try:
                _shared_redis_client = RedisService()
                logger.info("Created shared Redis client")
            except Exception as e:
                logger.error(f"Error creating shared Redis client: {e}")
                return None
        return _shared_redis_client


def reset_shared_redis_client():
    """Reset the shared Redis client for clean restarts."""
    global _shared_redis_client
    with _redis_lock:
        if _shared_redis_client is not None:
            try:
                _shared_redis_client.close()
                logger.info("Closed shared Redis client")
            except Exception as e:
                logger.warning(f"Error closing shared Redis client: {e}")
            _shared_redis_client = None


def reset_redis_connection():
    """
    Reset all Redis connections and pools.
    This is a comprehensive reset function that combines other reset operations.
    """
    logger.info("Performing comprehensive Redis connection reset")

    # Reset the shared Redis client first
    reset_shared_redis_client()

    # Reset all registered Redis clients
    reset_all_registered_redis_clients()

    # Reset the Redis connection pools
    reset_redis_pool()

    # Try to reset channel manager if it exists
    try:
        from src.arbirich.services.redis.redis_channel_manager import reset_channel_manager

        reset_channel_manager()
    except ImportError:
        logger.debug("Channel manager module not available, skipping reset")

    logger.info("Redis connections reset complete")
    return True


def reset_redis_pool():
    """
    Reset Redis connection pools.
    This helps clean up when restarting the system.
    """
    try:
        from redis.connection import ConnectionPool

        # Get all connection pools
        if hasattr(ConnectionPool, "connection_pools"):
            pools = ConnectionPool.connection_pools.copy()

            # Reset each pool
            for key, pool in pools.items():
                try:
                    # Disconnect all connections in the pool
                    pool.disconnect()
                    logger.info(f"Disconnected Redis pool: {key}")
                except Exception as e:
                    logger.warning(f"Error disconnecting Redis pool {key}: {e}")

            # Clear the pools dictionary
            ConnectionPool.connection_pools.clear()

        logger.info("Redis connection pools reset successfully")
        return True
    except Exception as e:
        logger.error(f"Error resetting Redis pools: {e}")
        return False


class RedisService:
    """Handles real-time market data storage, retrieval, and event publishing using Redis"""

    # Class variable to track subscribed channels across all instances
    _subscribed_channels: Set[str] = set()
    _pubsub_lock = threading.Lock()
    _shared_pubsub = None

    def __init__(self, retry_attempts=3, retry_delay=1):
        """Initialize Redis connection with retry logic"""
        self.host = REDIS_CONFIG["host"]
        self.port = REDIS_CONFIG["port"]
        self.db = REDIS_CONFIG["db"]
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self.pubsub = None
        self.client = None
        self._closed = False  # Add this to track if client was closed

        # Connect and register this client
        self._connect()

        # Register this instance in the global registry
        with _redis_lock:
            _redis_clients.append(self)

    def _connect(self) -> None:
        """Establish connection to Redis with retries"""
        logger.info(f"Attempting to connect to Redis at {self.host}:{self.port}")

        # Try with the specified host first
        self.client = redis.Redis(host=self.host, port=self.port, db=self.db, decode_responses=True)

        # Test the connection
        for attempt in range(1, self.retry_attempts + 1):
            try:
                self.client.ping()
                logger.info(f"Connected to Redis at {self.host}:{self.port}")
                return
            except redis.exceptions.ConnectionError as e:
                if attempt == self.retry_attempts:
                    logger.error(f"Failed to connect to Redis after {attempt} attempts: {str(e)}")

                    # If original host isn't working and it's not localhost, try localhost as fallback
                    if self.host != "localhost":
                        logger.info("Trying fallback connection to localhost")
                        try:
                            self.client = redis.Redis(
                                host="localhost", port=self.port, db=self.db, decode_responses=True
                            )
                            self.client.ping()
                            self.host = "localhost"
                            logger.info(f"Connected to Redis at localhost:{self.port}")
                            return
                        except Exception as fallback_e:
                            logger.error(f"Fallback connection failed: {str(fallback_e)}")

                    # If we reach here, all connection attempts failed
                    raise

                logger.warning(
                    f"Redis connection attempt {attempt} failed: {str(e)}. Retrying in {self.retry_delay}s..."
                )
                time.sleep(self.retry_delay)

    def _reconnect(self):
        """Try to reconnect to Redis."""
        for attempt in range(1, self.retry_attempts + 1):
            try:
                logger.warning(f"Attempting Redis reconnect ({attempt}/{self.retry_attempts})...")
                self._connect()
                return True
            except redis.ConnectionError:
                time.sleep(2)  # Wait before retrying
        logger.critical("Failed to reconnect to Redis after multiple attempts.")
        return False

    def close(self):
        """Close the Redis connection (synchronous version)"""
        try:
            # Close pubsub first if it exists
            if hasattr(self, "pubsub") and self.pubsub:
                try:
                    self.pubsub.close()
                    self.pubsub = None
                except Exception as e:
                    logger.error(f"Error closing Redis pubsub: {e}")

            # Then close the client
            if hasattr(self, "client") and self.client:
                self.client.close()
                logger.info("Redis connection closed (sync)")

            # Mark as closed
            self._closed = True

            # Remove from registry
            with _redis_lock:
                if self in _redis_clients:
                    _redis_clients.remove(self)

        except Exception as e:
            logger.error(f"Error closing Redis connection (sync): {e}")

    async def async_close(self) -> None:
        """Close Redis connection safely (async version)"""
        try:
            with self._pubsub_lock:
                if self._shared_pubsub:
                    self._shared_pubsub.close()
                    self._shared_pubsub = None
                    self._subscribed_channels.clear()

            if self.pubsub:
                self.pubsub.close()
                self.pubsub = None

            if self.client:
                self.client.close()
                self.client = None

            # Mark as closed
            self._closed = True

            # Remove from registry
            with _redis_lock:
                if self in _redis_clients:
                    _redis_clients.remove(self)

            logger.info("Redis connection closed (async)")
        except Exception as e:
            logger.error(f"Error closing Redis connection (async): {e}")

    async def __aclose__(self) -> None:
        """Async context manager support for 'async with' statements"""
        await self.async_close()

    @staticmethod
    async def close_all_connections():
        """Close all Redis connections across the application (async version)"""
        logger.info("Closing all Redis connections (async)")

        with _redis_lock:
            # Make a copy since we'll be modifying the list during iteration
            clients_to_close = _redis_clients.copy()

            for client in clients_to_close:
                try:
                    # If client has async_close, use it, otherwise use regular close
                    if hasattr(client, "async_close") and callable(client.async_close):
                        await client.async_close()
                    else:
                        client.close()
                except Exception as e:
                    logger.error(f"Error closing Redis client: {e}")

            # Clear the list
            _redis_clients.clear()

        logger.info("All Redis connections closed (async)")

        # Close connection pools
        try:
            # Close connection pools more carefully
            try:
                # Get all connection pools from redis package
                from redis.connection import ConnectionPool

                pools = ConnectionPool.connection_pools.copy() if hasattr(ConnectionPool, "connection_pools") else {}

                # Reset each pool individually
                for key, pool in pools.items():
                    try:
                        pool.disconnect()
                        logger.debug(f"Disconnected pool: {key}")
                    except Exception as e:
                        logger.warning(f"Error disconnecting pool {key}: {e}")

                # Also try to reset the pools dictionary
                if hasattr(ConnectionPool, "connection_pools"):
                    ConnectionPool.connection_pools.clear()

                logger.info("Disconnected all Redis connection pools")
            except Exception as e:
                logger.warning(f"Error while disconnecting connection pools: {e}")
        except Exception as e:
            logger.error(f"Error during Redis cleanup: {e}")

    @staticmethod
    def close_all_connections_sync():
        """Close all Redis connections across the application (synchronous version)"""
        logger.info("Closing all Redis connections (sync)")

        with _redis_lock:
            # Make a copy since we'll be modifying the list during iteration
            clients_to_close = _redis_clients.copy()

            for client in clients_to_close:
                try:
                    # Use synchronous close for all clients
                    client.close()
                except Exception as e:
                    logger.error(f"Error closing Redis client: {e}")

            # Clear the list
            _redis_clients.clear()

        logger.info("All Redis connections closed (sync)")

        # Close connection pools
        try:
            from redis.connection import ConnectionPool

            if hasattr(ConnectionPool, "connection_pools"):
                for key, pool in ConnectionPool.connection_pools.items():
                    try:
                        pool.disconnect()
                    except Exception as e:
                        logger.warning(f"Error disconnecting pool {key}: {e}")
                ConnectionPool.connection_pools.clear()
                logger.info("Redis connection pools reset")
        except Exception as e:
            logger.error(f"Error resetting Redis connection pools: {e}")

    def store_order_book(self, order_book: OrderBookUpdate) -> None:
        """
        Stores the order book update in Redis using a key formatted as:
        "order_book:{symbol}:{exchange}".

        This function uses a Redis pipeline to atomically delete any existing value
        and then set the new JSON-serialized order book update.
        """
        try:
            # Create a consistent key; note the order of symbol and exchange.
            key = f"order_book:{order_book.symbol}:{order_book.exchange}"

            # Create a pipeline for atomic execution of commands.
            pipeline = self.client.pipeline()
            pipeline.delete(key)
            order_book_json = order_book.model_dump_json()
            pipeline.set(key, order_book_json)
            # Optionally, you can set an expiry (e.g., 60 seconds) if needed:
            # pipeline.expire(key, 60)

            # Execute all commands atomically.
            pipeline.execute()

            # Check the results if needed (typically, an exception would be raised on error)
            logger.debug(f"Successfully stored order book update to Redis at key {key}")

        except Exception as e:
            logger.exception(f"Error storing order book update for key {key}: {e}")
            # Optionally, re-raise or handle the error as appropriate.
            raise

    def publish_order_book(self, exchange: str, symbol: str, order_book):
        """
        Publish an order book update to Redis using multiple channel formats
        for maximum compatibility.

        Parameters:
            exchange: The exchange name
            symbol: The trading pair symbol
            order_book: The order book data to publish (can be dict or OrderBookUpdate model)

        Returns:
            Total number of subscribers reached
        """
        try:
            # Convert Pydantic model to dict if needed
            if hasattr(order_book, "model_dump"):
                order_data = order_book.model_dump()
            elif hasattr(order_book, "dict"):
                order_data = order_book.dict()  # For older Pydantic versions
            else:
                # Assume it's already a dict
                order_data = order_book

            # Add exchange and symbol if not present in the order book
            if "exchange" not in order_data:
                order_data["exchange"] = exchange
            if "symbol" not in order_data:
                order_data["symbol"] = symbol

            # Add timestamp if missing
            if "timestamp" not in order_data:
                order_data["timestamp"] = time.time()

            # Convert to JSON for publishing
            message_json = json.dumps(order_data)

            total_subscribers = 0

            primary_channel = f"{ORDER_BOOK_CHANNEL}:{exchange}:{symbol}"
            primary_subs = self.client.publish(primary_channel, message_json)

            total_subscribers = primary_subs

            if total_subscribers > 0:
                logger.debug(f"Published order book for {exchange}:{symbol} to {total_subscribers} subscribers")
            else:
                logger.warning(f"Published order book for {exchange}:{symbol} but no subscribers")

            return total_subscribers
        except Exception as e:
            logger.error(f"Error publishing order book to Redis: {e}", exc_info=True)
            return 0

    def get_order_book(self, exchange: str, symbol: str) -> Optional[OrderBookUpdate]:
        key = f"order_book:{symbol}:{exchange}"
        try:
            data = self.client.get(key)
            if data:
                return OrderBookUpdate.model_validate_json(data.decode("utf-8"))
        except Exception as e:
            logger.error(f"Error retrieving order book: {e}")
        return None

    async def get_message(self, timeout=0.01) -> Optional[Dict[str, Any]]:
        """
        Async wrapper for getting a message from a Redis Pub/Sub channel.

        Args:
            timeout: How long to wait for a message

        Returns:
            The message or None if no message is available
        """
        try:
            # Ensure we have a pubsub client
            if not hasattr(self, "pubsub") or self.pubsub is None:
                if hasattr(self, "_get_shared_pubsub"):
                    self.pubsub = self._get_shared_pubsub()
                else:
                    self.pubsub = self.client.pubsub(ignore_subscribe_messages=True)

            # Get message from pubsub client
            # Use non-blocking mode so it works in async context
            message = self.pubsub.get_message(ignore_subscribe_messages=True, timeout=timeout)
            return message
        except Exception as e:
            logger.error(f"Error getting message: {e}")
            return None

    async def subscribe(self, channel: str) -> bool:
        """
        Async wrapper for subscribing to a Redis Pub/Sub channel.

        Args:
            channel: The channel to subscribe to

        Returns:
            True if subscription was successful, False otherwise
        """
        try:
            # Ensure we have a pubsub client
            if not hasattr(self, "pubsub") or self.pubsub is None:
                self.pubsub = self.client.pubsub(ignore_subscribe_messages=True)

            # Subscribe to channel
            self.pubsub.subscribe(channel)
            logger.info(f"Subscribed to channel: {channel}")
            return True
        except Exception as e:
            logger.error(f"Error subscribing to channel {channel}: {e}")
            return False

    async def publish(self, channel: str, message: str) -> int:
        """
        Async wrapper for publishing a message to a Redis Pub/Sub channel.

        Args:
            channel: The channel to publish to
            message: The message to publish

        Returns:
            Number of clients that received the message
        """
        try:
            return self.client.publish(channel, message)
        except Exception as e:
            logger.error(f"Error publishing to channel {channel}: {e}")
            return 0

    def _ensure_subscribed(self, channel: str) -> None:
        """
        Ensure we're subscribed to the specified channel using the shared PubSub client.
        """
        with self._pubsub_lock:
            if channel not in self._subscribed_channels:
                try:
                    self._get_shared_pubsub().subscribe(channel)
                    self._subscribed_channels.add(channel)
                    logger.info(f"Subscribed to channel: {channel}")
                except Exception as e:
                    logger.error(f"Error subscribing to channel {channel}: {e}")

    def _get_shared_pubsub(self):
        """
        Get or create the shared PubSub client.
        """
        with self._pubsub_lock:
            if self._shared_pubsub is None:
                if self.client is None:
                    self._connect()
                self._shared_pubsub = self.client.pubsub(ignore_subscribe_messages=True)
                logger.info("Created shared PubSub client")
            return self._shared_pubsub

    async def create_subscriber(self, channel_name):
        """
        Create a Redis pubsub subscriber for the given channel.

        Args:
            channel_name (str): The name of the channel to subscribe to

        Returns:
            redis.client.PubSub: A pubsub object subscribed to the channel
        """
        try:
            # Use channel manager for consistent channel naming if available
            try:
                from src.arbirich.services.redis.redis_channel_manager import get_channel_manager

                channel_manager = get_channel_manager()

                # Check if this is a known channel type and format it correctly
                if channel_name == "trade_opportunities":
                    channel_name = channel_manager.get_opportunity_channel()
                elif channel_name == "trade_executions":
                    channel_name = channel_manager.get_execution_channel()
                elif channel_name == "order_books":
                    channel_name = channel_manager.ORDER_BOOK
            except ImportError:
                # If channel manager not available, use constants
                from src.arbirich.constants import (
                    ORDER_BOOK_CHANNEL,
                    TRADE_EXECUTIONS_CHANNEL,
                    TRADE_OPPORTUNITIES_CHANNEL,
                )

                if channel_name == "trade_opportunities":
                    channel_name = TRADE_OPPORTUNITIES_CHANNEL
                elif channel_name == "trade_executions":
                    channel_name = TRADE_EXECUTIONS_CHANNEL
                elif channel_name == "order_books":
                    channel_name = ORDER_BOOK_CHANNEL

            # Use synchronous pubsub since we're using synchronous Redis client
            pubsub = self.client.pubsub()
            # Synchronous subscribe (no await)
            pubsub.subscribe(channel_name)
            logger.info(f"Subscribed to Redis channel: {channel_name}")
            return pubsub
        except Exception as e:
            logger.error(f"Failed to create subscriber for channel {channel_name}: {str(e)}")
            raise

    def subscribe_to_order_book_updates(
        self, channel: str, callback: Optional[Callable] = None
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Subscribe to a Redis Pub/Sub channel and yield messages indefinitely.
        Handles connection failures and restarts automatically.
        """
        pubsub = self.client.pubsub()
        pubsub.subscribe(channel)
        logger.info(f"Subscribed to Redis channel: {channel}")

        while True:
            try:
                message = pubsub.get_message(timeout=1)
                if message and message["type"] == "message":
                    try:
                        data = json.loads(message["data"])
                        if callback:
                            callback(data)  # Optional callback function
                        yield data
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON decoding error in {channel}: {e}")

                time.sleep(0.01)  # Prevent CPU overuse

            except KeyboardInterrupt:
                logger.info("Keyboard interrupt detected, unsubscribing...")
                pubsub.unsubscribe()
                pubsub.close()
                return  # Return instead of raising StopIteration

            except redis.exceptions.ConnectionError as e:
                logger.error(f"Redis connection error: {e}, reconnecting in 2s...")
                time.sleep(2)  # Pause before reconnecting
                try:
                    pubsub = self.client.pubsub()  # Reinitialize pubsub
                    pubsub.subscribe(channel)
                    logger.info(f"Reconnected to Redis and resubscribed to {channel}")
                except Exception as reconnect_error:
                    logger.error(f"Reconnection failed: {reconnect_error}")
                    time.sleep(5)  # Wait longer before retrying

    # === Trade Opportunity Methods ===

    def publish_trade_opportunity(self, opportunity: TradeOpportunity, strategy_name=None, retries_left=None) -> dict:
        """
        Store a trade opportunity in Redis with an expiration of 300 seconds and publish it.

        Parameters:
            opportunity: The TradeOpportunity to publish
            strategy_name: Optional strategy name to use for channel selection
            retries_left: Number of retry attempts remaining
        """
        if retries_left is None:
            retries_left = self.retry_attempts  # Initialize retries

        opportunity_id = opportunity.id or f"opp:{opportunity.opportunity_timestamp}"

        # Add strategy name to key if provided
        key_prefix = f"strategy:{strategy_name}:" if strategy_name else ""
        key = f"trade_opportunity:{key_prefix}{opportunity_id}"

        for attempt in range(1, self.retry_attempts + 1):
            try:
                self.client.setex(key, 300, opportunity.model_dump_json())

                # Use strategy-specific channel if provided
                channel = f"trade_opportunities:{strategy_name}" if strategy_name else "trade_opportunities"
                self.client.publish(channel, opportunity.model_dump_json())

                logger.debug(f"Published trade opportunity to {channel}: {opportunity}")
                return opportunity
            except (redis.ConnectionError, redis.TimeoutError) as e:
                logger.error(f"Redis error storing trade execution: {e}")
                if attempt < self.retry_attempts:
                    logger.warning(
                        f"Retrying trade opportunity storage ({self.retry_attempts - attempt} retries left)..."
                    )
                    self._reconnect()
                else:
                    logger.critical("Failed to store trade opportunity after multiple retries.")
                    return None
            except Exception as e:
                logger.error(f"Unexpected error in Redis: {e}", exc_info=True)
                return None

    def get_trade_opportunity(self, opportunity_id: str, strategy_name=None) -> Optional[Dict[str, Any]]:
        """
        Get trade opportunity by ID with error handling

        Parameters:
            opportunity_id: The ID of the opportunity to retrieve
            strategy_name: Optional strategy name to use for key prefix
        """
        try:
            key_prefix = f"strategy:{strategy_name}:" if strategy_name else ""
            key = f"trade_opportunity:{key_prefix}{opportunity_id}"
            data = self.client.get(key)
            return json.loads(data) if data else None
        except RedisError as e:
            logger.error(f"Redis error retrieving opportunity {opportunity_id}: {e}")
            return None
        except json.JSONDecodeError:
            logger.error(f"Malformed JSON for opportunity {opportunity_id}")
            return None

    def get_recent_opportunities(self, count: int = 10, strategy_name=None) -> List[Dict[str, Any]]:
        """
        Get most recent trade opportunities

        Parameters:
            count: Number of opportunities to retrieve
            strategy_name: Optional strategy name to filter opportunities
        """
        try:
            # Use strategy-specific sorted set if provided
            sorted_set = (
                f"trade_opportunities_by_time:{strategy_name}" if strategy_name else "trade_opportunities_by_time"
            )

            # Get latest opportunity IDs from sorted set
            opp_ids = self.client.zrevrange(sorted_set, 0, count - 1)
            opportunities = []

            # Fetch each opportunity
            for opp_id in opp_ids:
                opp_data = self.get_trade_opportunity(opp_id, strategy_name)
                if opp_data:
                    opportunities.append(opp_data)

            return opportunities
        except RedisError as e:
            logger.error(f"Redis error retrieving recent opportunities: {e}")
            return []

    def subscribe_to_trade_opportunities(self, callback: Optional[Callable] = None) -> None:
        """
        Subscribe to trade opportunities channel using the shared PubSub client.

        Args:
            callback: An optional callback function to execute for each message received.
        """
        logger.info(f"Subscribing to trade opportunities channel: {TRADE_OPPORTUNITIES_CHANNEL}")
        self._ensure_subscribed(TRADE_OPPORTUNITIES_CHANNEL)

        # Also subscribe to strategy-specific channels
        from src.arbirich.config.config import STRATEGIES

        for strategy_name in STRATEGIES.keys():
            strategy_channel = f"{TRADE_OPPORTUNITIES_CHANNEL}:{strategy_name}"
            logger.info(f"Subscribing to strategy-specific channel: {strategy_channel}")
            self._ensure_subscribed(strategy_channel)

    def get_opportunity(self) -> Optional[Dict[str, Any]]:
        """
        Get the next trade opportunity from the subscribed channel.

        Returns:
            The trade opportunity as a dictionary, or None if no message is available.
        """
        try:
            # Ensure we're subscribed
            self._ensure_subscribed(TRADE_OPPORTUNITIES_CHANNEL)

            # Get message from the shared pubsub client
            message = self._get_shared_pubsub().get_message(ignore_subscribe_messages=True, timeout=0.01)

            if not message:
                return None

            if message.get("type") != "message":
                return None

            # Verify it's from the trade_opportunities channel
            channel = message.get("channel")
            if isinstance(channel, bytes):
                channel = channel.decode("utf-8")

            if channel != TRADE_OPPORTUNITIES_CHANNEL:
                return None

            data = message.get("data")
            if not data:
                return None

            # Try to decode and parse the message data
            try:
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                return json.loads(data)
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                logger.error(f"Could not parse message data: {e}")
                return None

        except Exception as e:
            logger.error(f"Error getting opportunity from Redis: {e}")
            return None

    def publish_trade_execution(
        self, execution_data: TradeExecution, strategy_name=None, expiry_seconds: int = 3600
    ) -> str:
        """
        Store trade execution in Redis with expiration and handle failures.

        Parameters:
            execution_data: The TradeExecution to publish
            strategy_name: Optional strategy name for channel selection
            expiry_seconds: Time in seconds before key expires
        """
        logger.info(f"Storing trade execution: {execution_data}")

        # Use strategy from the execution data if not explicitly provided
        if not strategy_name and execution_data.strategy:
            strategy_name = execution_data.strategy

        # Ensure execution ID
        execution_id = execution_data.id

        # Add strategy name to key if provided
        key_prefix = f"strategy:{strategy_name}:" if strategy_name else ""
        key = f"trade_execution:{key_prefix}{execution_id}"
        logger.info(f"execution_id: {execution_id}")

        # Check if trade execution already exists (to prevent duplicates)
        if self.client.exists(key):
            logger.warning(f"Trade execution already exists. Skipping duplicate: {execution_id}")
            return execution_id  # Skip storing duplicate execution

        # Store execution in Redis with retry logic
        for attempt in range(1, self.retry_attempts + 1):
            try:
                # Store trade execution with expiration
                self.client.set(key, execution_data.model_dump_json(), ex=expiry_seconds)

                # Publish to strategy-specific channel if provided, ensuring the channel exists
                channel = f"{TRADE_EXECUTIONS_CHANNEL}:{strategy_name}" if strategy_name else TRADE_EXECUTIONS_CHANNEL
                # self._ensure_subscribed(channel)  # Ensure channel exists
                self.client.publish(channel, execution_data.model_dump_json())

                # Add execution to strategy-specific sorted set if provided
                sorted_set = (
                    f"trade_executions_by_time:{strategy_name}" if strategy_name else "trade_executions_by_time"
                )
                self.client.zadd(
                    sorted_set,
                    {execution_id: execution_data.execution_timestamp},
                )

                # Link to trade opportunity if available
                if execution_data.opportunity_id:
                    opp_key_prefix = f"strategy:{strategy_name}:" if strategy_name else ""
                    self.client.set(
                        f"execution_for_opportunity:{opp_key_prefix}{execution_data.opportunity_id}",
                        execution_id,
                        ex=expiry_seconds,
                    )

                logger.info(f"Trade execution stored successfully: {execution_data}")
                return execution_id  # Success, exit function
            except (redis.ConnectionError, redis.TimeoutError) as e:
                logger.error(f"Redis error storing trade execution: {e}")
                if attempt < self.retry_attempts:
                    logger.warning(
                        f"Retrying trade execution storage ({self.retry_attempts - attempt} retries left)..."
                    )
                    self._reconnect()
                else:
                    logger.critical("Failed to store trade execution after multiple retries.")
                    return None  # Return None after all retries fail
            except Exception as e:
                logger.error(f"Unexpected error storing trade execution: {e}", exc_info=True)
                return None

        return None  # If it reaches here, execution failed

    def get_trade_execution(self, execution_id: str, strategy_name=None) -> Optional[Dict[str, Any]]:
        """
        Get trade execution by ID with error handling

        Parameters:
            execution_id: The ID of the execution to retrieve
            strategy_name: Optional strategy name for key prefixing
        """
        try:
            key_prefix = f"strategy:{strategy_name}:" if strategy_name else ""
            key = f"trade_execution:{key_prefix}{execution_id}"
            data = self.client.get(key)
            return json.loads(data) if data else None
        except RedisError as e:
            logger.error(f"Redis error retrieving execution {execution_id}: {e}")
            return None
        except json.JSONDecodeError:
            logger.error(f"Malformed JSON for execution {execution_id}")
            return None

    def get_recent_executions(self, count: int = 10, strategy_name=None) -> List[Dict[str, Any]]:
        """
        Get most recent trade executions

        Parameters:
            count: Number of executions to retrieve
            strategy_name: Optional strategy name to filter executions
        """
        try:
            # Use strategy-specific sorted set if provided
            sorted_set = f"trade_executions_by_time:{strategy_name}" if strategy_name else "trade_executions_by_time"

            # Get latest execution IDs from sorted set
            exec_ids = self.client.zrevrange(sorted_set, 0, count - 1)
            executions = []

            # Fetch each execution
            for exec_id in exec_ids:
                exec_data = self.get_trade_execution(exec_id, strategy_name)
                if exec_data:
                    executions.append(exec_data)

            return executions
        except RedisError as e:
            logger.error(f"Redis error retrieving recent executions: {e}")
            return []

    def subscribe_to_trade_executions(
        self, callback: Callable, channel: str = "trade_executions", strategy_name=None
    ) -> None:
        """
        Subscribe to trade executions with proper error handling

        Parameters:
            callback: Function to call for each execution
            channel: Base channel name to subscribe to
            strategy_name: Optional strategy name to append to channel
        """
        # Use strategy-specific channel if provided
        if strategy_name:
            channel = f"{channel}:{strategy_name}"

        pubsub = self.client.pubsub()
        pubsub.subscribe(channel)
        logger.info(f"Subscribed to {channel} channel.")

        try:
            for message in pubsub.listen():
                if message["type"] == "message":
                    try:
                        data = json.loads(message["data"])
                        callback(data)
                    except json.JSONDecodeError as e:
                        logger.error(f"Error decoding trade execution message: {e}")
                    except Exception as e:
                        logger.error(f"Error in trade execution callback: {e}")
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt detected in execution subscription")
        except ConnectionError as e:
            logger.error(f"Redis connection error in execution subscription: {e}")
        finally:
            try:
                pubsub.unsubscribe()
                pubsub.close()
                logger.info("Cleaned up execution subscription")
            except Exception as e:
                logger.error(f"Error during execution subscription cleanup: {e}")

    # === Performance Tracking Methods ===
    def record_performance_metric(self, metric_name: str, value: float) -> bool:
        """Record a performance metric with timestamp"""
        try:
            timestamp = int(time.time())
            self.client.zadd(f"metrics:{metric_name}", {timestamp: value})
            return True
        except RedisError as e:
            logger.error(f"Error recording metric {metric_name}: {e}")
            return False

    def get_recent_metrics(self, metric_name: str, count: int = 60) -> List[Dict[str, Any]]:
        """Get recent metrics for a given name"""
        try:
            # Get timestamp-value pairs
            raw_metrics = self.client.zrevrange(f"metrics:{metric_name}", 0, count - 1, withscores=True)

            # Convert to list of dicts
            metrics = [{"timestamp": int(timestamp), "value": value} for value, timestamp in raw_metrics]

            return metrics
        except RedisError as e:
            logger.error(f"Error retrieving metrics for {metric_name}: {e}")
            return []

    # === Health Check Methods ===

    def is_healthy(self) -> bool:
        """Check if the Redis connection is healthy"""
        try:
            if self.client is None or self._closed:
                logger.warning("Redis client is None or marked as closed")
                return False

            # Attempt to ping Redis
            result = self.client.ping()
            if result:
                return True
            else:
                logger.warning("Redis ping returned false")
                return False
        except redis.RedisError as e:
            logger.warning(f"Redis health check failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error in Redis health check: {e}")
            return False

    def reconnect_if_needed(self) -> bool:
        """Reconnect to Redis if connection is unhealthy"""
        # Check system shutdown state first
        try:
            from arbirich.core.state.system_state import is_system_shutting_down

            if is_system_shutting_down():
                # Don't attempt reconnection during shutdown
                logger.debug("System is shutting down, skipping Redis reconnection")
                return False
        except ImportError:
            pass

        if not self.is_healthy():
            try:
                logger.info("Redis connection unhealthy, attempting reconnection...")
                self._connect()
                return True
            except Exception as e:
                logger.error(f"Failed to reconnect to Redis: {e}")
                return False
        return False

    def ensure_execution_channels_exist(self):
        """
        Ensure that trade execution channels exist by creating subscriptions.
        """
        logger.info("Ensuring trade execution channels exist")

        # Subscribe to main executions channel
        self._ensure_subscribed(TRADE_EXECUTIONS_CHANNEL)

        # Subscribe to strategy-specific execution channels
        from src.arbirich.config.config import STRATEGIES

        for strategy_name in STRATEGIES.keys():
            strategy_channel = f"{TRADE_EXECUTIONS_CHANNEL}:{strategy_name}"
            self._ensure_subscribed(strategy_channel)
            logger.info(f"Ensured execution channel exists: {strategy_channel}")


async def check_redis_health(redis_client, pubsub=None, channels=None):
    """
    Async function to check Redis connection health and repair if needed.

    Args:
        redis_client: The RedisService instance
        pubsub: Optional pubsub object to check
        channels: Optional list of channels to resubscribe

    Returns:
        bool: True if healthy, False otherwise
    """
    if not redis_client:
        logger.error("No Redis client provided for health check")
        return False

    try:
        # First check the Redis connection directly
        is_healthy = redis_client.is_healthy()

        if not is_healthy:
            logger.warning("Redis connection is unhealthy, attempting reconnect")
            # Attempt to reconnect
            redis_client._connect()

            # Check again after reconnect
            if not redis_client.is_healthy():
                logger.error("Redis reconnection failed")
                return False

            # Recreate pubsub if provided
            if pubsub and channels:
                try:
                    # Close existing pubsub
                    pubsub.unsubscribe()
                    pubsub.close()
                except Exception as e:
                    logger.warning(f"Error closing pubsub during reconnect: {e}")

                # Create new pubsub and resubscribe
                new_pubsub = redis_client.client.pubsub(ignore_subscribe_messages=True)
                for channel in channels:
                    new_pubsub.subscribe(channel)

                # Return the new pubsub to caller
                return True

        return True
    except Exception as e:
        logger.error(f"Error checking Redis health: {e}")
        return False


async def initialize_redis(channels=None):
    """
    Initialize Redis client and PubSub for the given channels.

    Args:
        channels: List of channel names to subscribe to

    Returns:
        tuple: (redis_client, pubsub) - the Redis client and PubSub objects
    """
    logger.info("Initializing Redis client and PubSub")

    if channels is None:
        channels = []

    try:
        # Get a Redis client
        redis_client = get_shared_redis_client()
        if not redis_client:
            logger.error("Failed to get Redis client")
            return None, None

        # Create PubSub object
        pubsub = redis_client.client.pubsub(ignore_subscribe_messages=True)

        # Subscribe to channels if provided
        if channels:
            for channel in channels:
                pubsub.subscribe(channel)
            logger.info(f"Subscribed to {len(channels)} channels")

        return redis_client, pubsub
    except Exception as e:
        logger.error(f"Error initializing Redis: {e}")
        return None, None
