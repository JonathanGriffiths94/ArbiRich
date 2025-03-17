import json
import logging
import threading
import time
from typing import Any, Callable, Dict, Generator, List, Optional, Set

import redis
from redis.exceptions import ConnectionError, RedisError

from src.arbirich.config import REDIS_CONFIG
from src.arbirich.models.models import OrderBookUpdate, TradeExecution, TradeOpportunity

logger = logging.getLogger(__name__)

TRADE_OPPORTUNITIES_CHANNEL = "trade_opportunities"


class RedisService:
    """Handles real-time market data storage, retrieval, and event publishing using Redis"""

    # Class variable to track subscribed channels across all instances
    _subscribed_channels: Set[str] = set()
    _pubsub_lock = threading.Lock()
    _shared_pubsub = None

    # Class variable to store all instances for proper cleanup
    _instances = []

    def __init__(self, retry_attempts=3, retry_delay=1):
        """Initialize Redis connection with retry logic"""
        self.host = REDIS_CONFIG["host"]
        self.port = REDIS_CONFIG["port"]
        self.db = REDIS_CONFIG["db"]
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self.pubsub = None
        self.client = None
        self._connect()

        # Add self to instances list for cleanup
        RedisService._instances.append(self)

    def _connect(self) -> None:
        """Establish connection to Redis with retries"""
        for attempt in range(self.retry_attempts):
            try:
                # Log the connection attempt with the actual host being used
                logger.info(f"Attempting to connect to Redis at {self.host}:{self.port}")

                self.client = redis.Redis(
                    host=self.host,
                    port=self.port,
                    db=self.db,
                    decode_responses=True,
                    socket_timeout=5,
                    socket_connect_timeout=5,
                    health_check_interval=30,
                )
                # Test connection
                self.client.ping()
                logger.info(f"Connected to Redis at {self.host}:{self.port}")
                return
            except (ConnectionError, RedisError) as e:
                if attempt < self.retry_attempts - 1:
                    logger.warning(
                        f"Redis connection attempt {attempt + 1} failed: {e}. Retrying in {self.retry_delay}s..."
                    )
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"Failed to connect to Redis after {self.retry_attempts} attempts: {e}")
                    raise

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

    def close(self) -> None:
        """Close Redis connection safely"""
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

            logger.info("Redis connection closed")
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}")

        # Remove self from instances list
        if self in RedisService._instances:
            RedisService._instances.remove(self)

    @classmethod
    def close_all_connections(cls):
        """Close all Redis connections created by this service."""
        logger.info(f"Closing all Redis connections ({len(cls._instances)} instances)")
        for instance in list(cls._instances):
            instance.close()

        # Clear the instances list to be safe
        cls._instances = []

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
            results = pipeline.execute()

            # Check the results if needed (typically, an exception would be raised on error)
            logger.debug(f"Successfully stored order book update to Redis at key {key}")

        except Exception as e:
            logger.exception(f"Error storing order book update for key {key}: {e}")
            # Optionally, re-raise or handle the error as appropriate.
            raise

    def publish_order_book_update(self, order_book: OrderBookUpdate) -> int:
        """
        Publish an order book update to Redis channels using both the old and new channel formats.

        Parameters:
            order_book: The OrderBookUpdate to publish

        Returns:
            The total number of subscribers that received the message
        """
        try:
            # Ensure we have the required fields
            if not order_book.exchange or not order_book.symbol:
                logger.error("Order book missing required exchange or symbol fields")
                return 0

            new_channel = f"order_book.{order_book.symbol}.{order_book.exchange}"
            message = order_book.model_dump_json()
            result = self.client.publish(new_channel, message)

            if result > 0:
                logger.debug(
                    f"Published order book to {result} subscribers for {order_book.exchange}:{order_book.symbol}"
                )
            else:
                logger.debug(f"Published order book but no subscribers for {order_book.exchange}:{order_book.symbol}")

            # Also store the order book in Redis for later retrieval
            key = f"order_book:{order_book.symbol}:{order_book.exchange}"
            self.client.set(key, message, ex=60)  # Expire after 60 seconds

            return result
        except Exception as e:
            logger.error(f"Error publishing order book update: {e}", exc_info=True)
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

    def get_message(self, channel: str) -> Optional[str]:
        """
        Get a message from a Redis Pub/Sub channel using shared PubSub client.
        """
        try:
            # Subscribe to the channel if not already subscribed
            self._ensure_subscribed(channel)

            # Get message from the shared pubsub client with timeout
            message = self._get_shared_pubsub().get_message(ignore_subscribe_messages=True, timeout=0.01)

            if message and message.get("type") == "message":
                # Only return messages from the requested channel
                if message.get("channel") == channel.encode() or message.get("channel") == channel:
                    return message.get("data")

            return None
        except Exception as e:
            logger.error(f"Error getting message from channel {channel}: {e}")
            return None

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

    def subscribe_to_order_book_updates(
        self, channel: str = "order_book", callback: Optional[Callable] = None
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
        self._ensure_subscribed(TRADE_OPPORTUNITIES_CHANNEL)

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

                # Publish to strategy-specific channel if provided
                channel = f"trade_executions:{strategy_name}" if strategy_name else "trade_executions"
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
        """Check if Redis connection is healthy"""
        try:
            return bool(self.client.ping())
        except Exception:
            return False

    def reconnect_if_needed(self) -> bool:
        """Reconnect to Redis if connection is unhealthy"""
        if not self.is_healthy():
            try:
                logger.info("Redis connection unhealthy, attempting reconnection...")
                self._connect()
                return True
            except Exception as e:
                logger.error(f"Failed to reconnect to Redis: {e}")
                return False
        return False
