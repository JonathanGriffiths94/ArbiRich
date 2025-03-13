import json
import logging
import os
import time
from typing import Any, Callable, Dict, Generator, List, Optional

import redis
from redis.exceptions import ConnectionError, RedisError

from src.arbirich.models.dtos import OrderBookUpdate, TradeExecution, TradeOpportunity

logger = logging.getLogger(__name__)


class ArbiDataService:
    """Handles real-time market data storage, retrieval, and event publishing using Redis"""

    def __init__(self, host=None, port=6379, db=0, retry_attempts=3, retry_delay=1):
        """Initialize Redis connection with retry logic"""
        self.host = host or os.getenv("REDIS_HOST", "localhost")
        self.port = port
        self.db = db
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self._connect()

    def _connect(self) -> None:
        """Establish connection to Redis with retries"""
        for attempt in range(self.retry_attempts):
            try:
                self.redis_client = redis.Redis(
                    host=self.host,
                    port=self.port,
                    db=self.db,
                    decode_responses=True,
                    socket_timeout=5,
                    socket_connect_timeout=5,
                    health_check_interval=30,
                )
                # Test connection
                self.redis_client.ping()
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
            self.redis_client.close()
            logger.info("Redis connection closed")
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}")

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
            pipeline = self.redis_client.pipeline()
            pipeline.delete(key)
            order_book_json = order_book.model_dump_json()
            pipeline.set(key, order_book_json)
            # Optionally, you can set an expiry (e.g., 60 seconds) if needed:
            # pipeline.expire(key, 60)

            # Execute all commands atomically.
            results = pipeline.execute()

            # Check the results if needed (typically, an exception would be raised on error)
            logger.info(f"Successfully stored order book update to Redis at key {key}")

        except Exception as e:
            logger.exception(f"Error storing order book update for key {key}: {e}")
            # Optionally, re-raise or handle the error as appropriate.
            raise

    def publish_order_book(self, order_book: OrderBookUpdate, channel: str = "order_book"):
        """
        Publish top order book data to a Redis Pub/Sub channel.
        """
        try:
            self.redis_client.publish(channel, order_book.model_dump_json())
            logger.debug(f"Published order book update: {order_book.symbol} {order_book.exchange}")
        except Exception as e:
            logger.error(f"Error publishing order book data: {e}")

    def get_order_book(self, exchange: str, symbol: str) -> Optional[OrderBookUpdate]:
        key = f"order_book:{symbol}:{exchange}"
        try:
            data = self.redis_client.get(key)
            if data:
                return OrderBookUpdate.model_validate_json(data.decode("utf-8"))
        except Exception as e:
            logger.error(f"Error retrieving order book: {e}")
        return None

    def subscribe_to_order_book_updates(
        self, channel: str = "trade_opportunities", callback: Optional[Callable] = None
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Subscribe to a Redis Pub/Sub channel and yield messages indefinitely.
        Handles connection failures and restarts automatically.
        """
        pubsub = self.redis_client.pubsub()
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
                    pubsub = self.redis_client.pubsub()  # Reinitialize pubsub
                    pubsub.subscribe(channel)
                    logger.info(f"Reconnected to Redis and resubscribed to {channel}")
                except Exception as reconnect_error:
                    logger.error(f"Reconnection failed: {reconnect_error}")
                    time.sleep(5)  # Wait longer before retrying

    # === Trade Opportunity Methods ===
    def publish_trade_opportunity(self, opportunity: TradeOpportunity, retries_left=None) -> dict:
        """
        Store a trade opportunity in Redis with an expiration of 300 seconds and publish it.
        Ensures it does not enter infinite recursion.
        """
        if retries_left is None:
            retries_left = self.retry_attempts  # Initialize retries

        opportunity_id = opportunity.id or f"opp:{opportunity.timestamp}"
        key = f"trade_opportunity:{opportunity_id}"

        for attempt in range(1, self.retry_attempts + 1):
            try:
                self.redis_client.setex(key, 300, opportunity.model_dump_json())

                self.redis_client.publish("trade_opportunities", opportunity.model_dump_json())

                logger.debug(f"Published trade opportunity: {opportunity}")
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

    def get_trade_opportunity(self, opportunity_id: str) -> Optional[Dict[str, Any]]:
        """Get trade opportunity by ID with error handling"""
        try:
            key = f"trade_opportunity:{opportunity_id}"
            data = self.redis_client.get(key)
            return json.loads(data) if data else None
        except RedisError as e:
            logger.error(f"Redis error retrieving opportunity {opportunity_id}: {e}")
            return None
        except json.JSONDecodeError:
            logger.error(f"Malformed JSON for opportunity {opportunity_id}")
            return None

    def get_recent_opportunities(self, count: int = 10) -> List[Dict[str, Any]]:
        """Get most recent trade opportunities"""
        try:
            # Get latest opportunity IDs from sorted set
            opp_ids = self.redis_client.zrevrange("trade_opportunities_by_time", 0, count - 1)
            opportunities = []

            # Fetch each opportunity
            for opp_id in opp_ids:
                opp_data = self.get_trade_opportunity(opp_id)
                if opp_data:
                    opportunities.append(opp_data)

            return opportunities
        except RedisError as e:
            logger.error(f"Redis error retrieving recent opportunities: {e}")
            return []

    def subscribe_to_trade_opportunities(
        self, callback: Optional[Callable], channel: str = "trade_opportunities"
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Subscribe to the "trade_opportunity" channel using non-blocking polling.
        Yields messages as they arrive.
        """
        pubsub = self.redis_client.pubsub()
        pubsub.subscribe(channel)
        logger.info("Subscribed to trade_opportunities channel.")
        try:
            while True:
                try:
                    message = pubsub.get_message(timeout=1)
                    if message and message["type"] == "message":
                        try:
                            data = json.loads(message["data"])
                            if callback:
                                callback(data)

                            logger.info(f"Data: {data}")
                            yield data
                        except json.JSONDecodeError as e:
                            logger.error(f"Error decoding Redis message: {e}")
                    # Sleep briefly to avoid busy-waiting.
                    time.sleep(0.01)
                except KeyboardInterrupt:
                    logger.info("Keyboard interrupt detected in subscription")
                    pubsub.unsubscribe()
                    pubsub.close()
                    raise StopIteration
                except ConnectionError as e:
                    logger.error(f"Redis connection error in subscription: {e}")
                    time.sleep(1)  # Wait before trying again
                    # Try to reconnect
                    try:
                        self._connect()
                        pubsub = self.redis_client.pubsub()
                        pubsub.subscribe("trade_opportunity")
                        logger.info("Reconnected to Redis and resubscribed")
                    except Exception as reconnect_error:
                        logger.error(f"Failed to reconnect: {reconnect_error}")
        finally:
            logger.info("Cleaning up Redis subscription")
            try:
                pubsub.unsubscribe()
                pubsub.close()
            except Exception as e:
                logger.error(f"Error during subscription cleanup: {e}")

    def publish_trade_execution(self, execution_data: TradeExecution, expiry_seconds: int = 3600) -> str:
        """
        Store trade execution in Redis with expiration and handle failures.
        Prevents infinite recursion by using a retry loop instead of calling itself.
        """
        logger.info(f"Storing trade execution: {execution_data}")
        # Ensure execution ID
        execution_id = execution_data.id
        key = f"trade_execution:{execution_id}"
        logger.info(f"execution_id: {execution_id}")

        # Check if trade execution already exists (to prevent duplicates)
        if self.redis_client.exists(key):
            logger.warning(f"Trade execution already exists. Skipping duplicate: {execution_id}")
            return execution_id  # Skip storing duplicate execution

        # Store execution in Redis with retry logic
        for attempt in range(1, self.retry_attempts + 1):
            try:
                # Store trade execution with expiration
                self.redis_client.set(key, execution_data.model_dump_json(), ex=expiry_seconds)

                # Publish trade execution
                self.redis_client.publish("trade_executions", execution_data.model_dump_json())

                # Add execution to sorted set
                self.redis_client.zadd(
                    "trade_executions_by_time",
                    {execution_id: execution_data.execution_timestamp},
                )

                # Link to trade opportunity if available
                if execution_data.opportunity_id:
                    self.redis_client.set(
                        f"execution_for_opportunity:{execution_data.opportunity_id}",
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

    def get_trade_execution(self, execution_id: str) -> Optional[Dict[str, Any]]:
        """Get trade execution by ID with error handling"""
        try:
            key = f"trade_execution:{execution_id}"
            data = self.redis_client.get(key)
            return json.loads(data) if data else None
        except RedisError as e:
            logger.error(f"Redis error retrieving execution {execution_id}: {e}")
            return None
        except json.JSONDecodeError:
            logger.error(f"Malformed JSON for execution {execution_id}")
            return None

    def get_recent_executions(self, count: int = 10) -> List[Dict[str, Any]]:
        """Get most recent trade executions"""
        try:
            # Get latest execution IDs from sorted set
            exec_ids = self.redis_client.zrevrange("trade_executions_by_time", 0, count - 1)
            executions = []

            # Fetch each execution
            for exec_id in exec_ids:
                exec_data = self.get_trade_execution(exec_id)
                if exec_data:
                    executions.append(exec_data)

            return executions
        except RedisError as e:
            logger.error(f"Redis error retrieving recent executions: {e}")
            return []

    def subscribe_to_trade_executions(self, callback: Callable, channel: str = "trade_executions") -> None:
        """Subscribe to trade executions with proper error handling"""
        pubsub = self.redis_client.pubsub()
        pubsub.subscribe(channel)
        logger.info("Subscribed to trade_executions channel.")
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
            self.redis_client.zadd(f"metrics:{metric_name}", {timestamp: value})
            return True
        except RedisError as e:
            logger.error(f"Error recording metric {metric_name}: {e}")
            return False

    def get_recent_metrics(self, metric_name: str, count: int = 60) -> List[Dict[str, Any]]:
        """Get recent metrics for a given name"""
        try:
            # Get timestamp-value pairs
            raw_metrics = self.redis_client.zrevrange(f"metrics:{metric_name}", 0, count - 1, withscores=True)

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
            return bool(self.redis_client.ping())
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
