import json
import logging
import os
import time
from typing import Any, Callable, Dict, Generator, List, Optional

import dateutil.parser
import redis
from redis.exceptions import ConnectionError, RedisError

logger = logging.getLogger(__name__)


class MarketDataService:
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
                    logger.error(
                        f"Failed to connect to Redis after {self.retry_attempts} attempts: {e}"
                    )
                    raise

    def close(self) -> None:
        """Close Redis connection safely"""
        try:
            self.redis_client.close()
            logger.info("Redis connection closed")
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}")

    # === Trade Opportunity Methods ===
    def store_trade_opportunity(self, opportunity_data: Dict[str, Any]) -> None:
        """
        Store a trade opportunity in Redis with an expiration of 300 seconds and publish it.
        """
        try:
            # Generate an ID if not provided.
            opportunity_id = (
                opportunity_data.get("id")
                or f"opp:{opportunity_data.get('timestamp', int(time.time()))}"
            )
            key = f"trade_opportunity:{opportunity_id}"
            self.redis_client.set(key, json.dumps(opportunity_data), ex=300)
            self.publish_trade_opportunity(opportunity_data)
        except RedisError as e:
            logger.error(f"Redis error storing trade opportunity: {e}")
            raise

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
            opp_ids = self.redis_client.zrevrange(
                "trade_opportunities_by_time", 0, count - 1
            )
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

    def publish_trade_opportunity(self, opportunity_data: Dict[str, Any]) -> bool:
        """Publish trade opportunity with error handling"""
        try:
            channel = "trade_opportunity"
            message = json.dumps(opportunity_data)
            result = self.redis_client.publish(channel, message)
            logger.debug(f"Published opportunity to {result} subscribers")
            return result > 0
        except RedisError as e:
            logger.error(f"Redis error publishing trade opportunity: {e}")
            return False
        except Exception as e:
            logger.error(f"Error publishing trade opportunity: {e}")
            return False

    def subscribe_to_trade_opportunities(
        self, callback: Optional[Callable] = None
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Subscribe to the "trade_opportunity" channel using non-blocking polling.
        Yields messages as they arrive.
        """
        pubsub = self.redis_client.pubsub()
        pubsub.subscribe("trade_opportunity")
        logger.info("Subscribed to trade_opportunity channel.")
        try:
            while True:
                try:
                    message = pubsub.get_message(timeout=1)
                    if message and message["type"] == "message":
                        try:
                            data = json.loads(message["data"])
                            if callback:
                                callback(data)
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

    # === Trade Execution Methods ===
    def store_trade_execution(
        self, execution_data: Dict[str, Any], expiry_seconds: int = 3600
    ) -> str:
        """Store trade execution with proper error handling"""
        try:
            # Generate ID if not provided
            execution_id = (
                execution_data.get("id")
                or f"exec:{execution_data.get('timestamp', int(time.time()))}"
            )
            key = f"trade_execution:{execution_id}"

            # Add metadata if not present
            if "timestamp" not in execution_data:
                execution_data["timestamp"] = int(time.time())
            if "id" not in execution_data:
                execution_data["id"] = execution_id

            # Ensure numeric fields are proper floats.
            for field in ["buy_price", "sell_price", "spread"]:
                if field in execution_data:
                    execution_data[field] = float(execution_data[field])

            # Store the execution data.
            self.redis_client.set(key, json.dumps(execution_data), ex=expiry_seconds)
            self.publish_trade_execution(execution_data)

            # Add to sorted set for time-based retrieval.
            ts = execution_data.get("timestamp", int(time.time()))
            if isinstance(ts, str):
                try:
                    ts_converted = dateutil.parser.parse(ts).timestamp()
                    logger.debug(f"Parsed timestamp from '{ts}' to {ts_converted}")
                    ts = ts_converted
                except Exception as e:
                    logger.error(f"Error parsing timestamp {ts}: {e}")
                    ts = time.time()
            elif not isinstance(ts, (int, float)):
                ts = float(ts)
            logger.debug(f"Using timestamp {ts} (type: {type(ts)}) for sorted set")

            self.redis_client.zadd("trade_executions_by_time", {execution_id: ts})

            # If linked to an opportunity, create relationship
            if "opportunity_id" in execution_data:
                self.redis_client.set(
                    f"execution_for_opportunity:{execution_data['opportunity_id']}",
                    execution_id,
                    ex=expiry_seconds,
                )

            return execution_id
        except RedisError as e:
            logger.error(f"Redis error storing trade execution: {e}")
            raise
        except Exception as e:
            logger.error(f"Error storing trade execution: {e}")
            raise

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
            exec_ids = self.redis_client.zrevrange(
                "trade_executions_by_time", 0, count - 1
            )
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

    def publish_trade_execution(self, execution_data: Dict[str, Any]) -> bool:
        """Publish trade execution with error handling"""
        try:
            channel = "trade_executions"
            message = json.dumps(execution_data)
            result = self.redis_client.publish(channel, message)
            logger.debug(f"Published execution to {result} subscribers")
            return result > 0
        except RedisError as e:
            logger.error(f"Redis error publishing trade execution: {e}")
            return False
        except Exception as e:
            logger.error(f"Error publishing trade execution: {e}")
            return False

    def subscribe_to_trade_executions(self, callback: Callable) -> None:
        """Subscribe to trade executions with proper error handling"""
        pubsub = self.redis_client.pubsub()
        pubsub.subscribe("trade_executions")
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

    def get_recent_metrics(
        self, metric_name: str, count: int = 60
    ) -> List[Dict[str, Any]]:
        """Get recent metrics for a given name"""
        try:
            # Get timestamp-value pairs
            raw_metrics = self.redis_client.zrevrange(
                f"metrics:{metric_name}", 0, count - 1, withscores=True
            )

            # Convert to list of dicts
            metrics = [
                {"timestamp": int(timestamp), "value": value}
                for value, timestamp in raw_metrics
            ]

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
