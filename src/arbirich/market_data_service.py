import asyncio
import json
import logging
import os
import time
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Tuple

import dateutil.parser
import redis.asyncio as redis
from redis.exceptions import ConnectionError, RedisError

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


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

    async def _connect(self) -> None:
        """Establish an asynchronous connection to Redis with retries."""
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
                # Test connection asynchronously
                await self.redis_client.ping()
                logger.info(f"Connected to Redis at {self.host}:{self.port}")
                return
            except (ConnectionError, RedisError) as e:
                if attempt < self.retry_attempts - 1:
                    logger.warning(
                        f"Redis connection attempt {attempt + 1} failed: {e}. Retrying in {self.retry_delay}s..."
                    )
                    await asyncio.sleep(self.retry_delay)
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

    # === Price Methods ===
    def get_price(self, exchange, asset):
        """
        Retrieves the latest price of an asset from an exchange in Redis.
        Returns None if no data is found or an error occurs.
        """
        key = f"price:{exchange}:{asset}"
        try:
            data = self.redis_client.get(key)
            if data:
                parsed = json.loads(data)
                return parsed.get("price")
            else:
                logger.debug(f"No price data found for {exchange}:{asset}")
                return None
        except Exception as e:
            logger.error(f"Error retrieving price for {exchange}:{asset} - {e}")
            return None

    def publish_price(self, exchange, asset, price):
        """
        Publishes the latest price of an asset from an exchange to a Redis Pub/Sub channel.
        Also stores it in Redis with a short TTL to ensure freshness.
        """
        key = f"price:{exchange}:{asset}"
        value = json.dumps({"price": price, "timestamp": time.time()})
        try:
            # self.redis_client.setex(key, 10, value)  # Store with 10s TTL (avoids stale prices)
            self.redis_client.set(f"price:{exchange}:{asset}", value)
            self.redis_client.publish(
                "prices",
                json.dumps({"exchange": exchange, "asset": asset, "price": price}),
            )
            logger.debug(f"Published price for {exchange}:{asset} -> {price}")
        except Exception as e:
            logger.error(f"Error publishing price: {e}")

    # async def subscribe_to_prices(
    #         self, callback: Optional[Callable[[Dict[str, Any]], None]] = None
    #     ) -> AsyncGenerator[Tuple[str, str, float], None]:
    #         """
    #         Subscribe to the "prices" channel using non-blocking polling.
    #         Yields a tuple (exchange, asset, price) for each message received.
    #         If no message is received, yields a heartbeat (None) to keep the stream alive.
    #         """
    #         pubsub = self.redis_client.pubsub()
    #         await pubsub.subscribe("prices")
    #         logger.info("Subscribed to prices channel.")
    #         try:
    #             while True:
    #                 try:
    #                     message = await pubsub.get_message(timeout=1)
    #                     if message and message["type"] == "message":
    #                         try:
    #                             data = json.loads(message["data"])
    #                             if callback:
    #                                 await callback(data)
    #                             if "exchange" in data and "asset" in data and "price" in data:
    #                                 yield (data["exchange"], data["asset"], data["price"])
    #                             else:
    #                                 logger.error("Incomplete data received from prices channel")
    #                         except Exception as e:
    #                             logger.error(f"Error decoding message: {e}")
    #                     else:
    #                         # Short sleep to prevent tight loop
    #                         await asyncio.sleep(0.01)
    #                 except KeyboardInterrupt:
    #                     logger.info("Keyboard interrupt detected in subscription")
    #                     await pubsub.unsubscribe()
    #                     await pubsub.close()
    #                     return
    #                 except ConnectionError as e:
    #                     logger.error(f"Redis connection error in subscription: {e}")
    #                     await asyncio.sleep(1)
    #                     try:
    #                         await self._connect()
    #                         pubsub = self.redis_client.pubsub()
    #                         await pubsub.subscribe("prices")
    #                         logger.info("Reconnected to Redis and resubscribed")
    #                     except Exception as reconnect_error:
    #                         logger.error(f"Failed to reconnect: {reconnect_error}")
    #         finally:
    #             logger.info("Cleaning up Redis subscription")
    #             try:
    #                 await pubsub.unsubscribe()
    #                 await pubsub.close()
    #             except Exception as e:
    #                 logger.error(f"Error during subscription cleanup: {e}")

    def subscribe_to_trade_executions(self, callback: Callable) -> None:
        """Subscribe to trade executions with proper error handling"""
        pubsub = self.redis_client.pubsub()
        pubsub.subscribe("price")
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
                    # logger.debug(f"Parsed timestamp from '{ts}' to {ts_converted}")
                    ts = ts_converted
                except Exception as e:
                    logger.error(f"Error parsing timestamp {ts}: {e}")
                    ts = time.time()
            elif not isinstance(ts, (int, float)):
                ts = float(ts)
            # logger.debug(f"Using timestamp {ts} (type: {type(ts)}) for sorted set")

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


# import os
# import json
# import time
# import asyncio
# import logging
# from typing import Any, Callable, Dict, AsyncGenerator, Optional

# import redis.asyncio as redis
# from redis.exceptions import ConnectionError, RedisError
# import dateutil.parser

# logger = logging.getLogger(__name__)
# logger.setLevel("DEBUG")


# class MarketDataService:
#     """Handles real-time market data storage, retrieval, and event publishing using async Redis"""

#     def __init__(
#         self,
#         redis_client: Optional[redis.Redis] = None,
#         host: Optional[str] = None,
#         port: int = 6379,
#         db: int = 0,
#         retry_attempts: int = 3,
#         retry_delay: int = 1,
#     ):
#         self.host = host or os.getenv("REDIS_HOST", "localhost")
#         self.port = port
#         self.db = db
#         self.retry_attempts = retry_attempts
#         self.retry_delay = retry_delay

#         if redis_client is not None:
#             self.redis_client = redis_client
#         else:
#             # Create async Redis client using from_url
#             self.redis_client = redis.Redis.from_url(
#                 f"redis://{self.host}:{self.port}/{self.db}",
#                 decode_responses=True
#             )
#             # In an async context, we should await _connect(), so run it here.
#             asyncio.get_event_loop().run_until_complete(self._connect())

#     async def _connect(self) -> None:
#         """Establish an asynchronous connection to Redis with retries."""
#         for attempt in range(self.retry_attempts):
#             try:
#                 await self.redis_client.ping()
#                 logger.info(f"Connected to Redis at {self.host}:{self.port}")
#                 return
#             except (ConnectionError, RedisError) as e:
#                 if attempt < self.retry_attempts - 1:
#                     logger.warning(
#                         f"Redis connection attempt {attempt + 1} failed: {e}. Retrying in {self.retry_delay}s..."
#                     )
#                     await asyncio.sleep(self.retry_delay)
#                 else:
#                     logger.error(
#                         f"Failed to connect to Redis after {self.retry_attempts} attempts: {e}"
#                     )
#                     raise

#     async def close(self) -> None:
#         """Close Redis connection safely"""
#         try:
#             await self.redis_client.close()
#             logger.info("Redis connection closed")
#         except Exception as e:
#             logger.error(f"Error closing Redis connection: {e}")

#     # === Price Methods ===
#     async def get_price(self, exchange: str, asset: str) -> Optional[float]:
#         """
#         Retrieves the latest price of an asset from an exchange in Redis.
#         Returns None if no data is found or an error occurs.
#         """
#         key = f"price:{exchange}:{asset}"
#         try:
#             data = await self.redis_client.get(key)
#             if data:
#                 parsed = json.loads(data)
#                 return parsed.get("price")
#             else:
#                 logger.debug(f"No price data found for {exchange}:{asset}")
#                 return None
#         except Exception as e:
#             logger.error(f"Error retrieving price for {exchange}:{asset} - {e}")
#             return None

#     async def publish_price(self, exchange: str, asset: str, price: float) -> None:
#         """
#         Publishes the latest price of an asset from an exchange to a Redis Pub/Sub channel.
#         Also stores it in Redis with a short TTL to ensure freshness.
#         """
#         key = f"price:{exchange}:{asset}"
#         value = json.dumps({"price": price, "timestamp": time.time()})
#         try:
#             await self.redis_client.setex(key, 10, value)
#             await self.redis_client.publish(
#                 "prices", json.dumps({"exchange": exchange, "asset": asset, "price": price})
#             )
#             logger.debug(f"Published price for {exchange}:{asset} -> {price}")
#         except Exception as e:
#             logger.error(f"Error publishing price: {e}")

#     async def subscribe_to_prices(
#         self, callback: Optional[Callable[[Dict[str, Any]], None]] = None
#     ) -> AsyncGenerator[tuple[str, str, float], None]:
#         """
#         Subscribe to the "prices" channel using non-blocking polling.
#         Yields a tuple (exchange, asset, price) for each message received.
#         If no message is received, yields nothing (or you can choose to yield a heartbeat).
#         """
#         pubsub = self.redis_client.pubsub()
#         await pubsub.subscribe("prices")
#         logger.info("Subscribed to prices channel.")
#         try:
#             while True:
#                 message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
#                 if message:
#                     try:
#                         data = json.loads(message["data"])
#                         if callback:
#                             await callback(data)
#                         if "exchange" in data and "asset" in data and "price" in data:
#                             yield (data["exchange"], data["asset"], data["price"])
#                         else:
#                             logger.error("Incomplete data received from prices channel")
#                     except Exception as e:
#                         logger.error(f"Error decoding message: {e}")
#                 else:
#                     await asyncio.sleep(0.01)
#         finally:
#             logger.info("Cleaning up Redis subscription")
#             await pubsub.unsubscribe("prices")
#             await pubsub.close()

#     # === Trade Opportunity Methods ===
#     async def store_trade_opportunity(self, opportunity_data: Dict[str, Any]) -> None:
#         """
#         Store a trade opportunity in Redis with an expiration of 300 seconds and publish it.
#         """
#         try:
#             opportunity_id = (
#                 opportunity_data.get("id")
#                 or f"opp:{opportunity_data.get('timestamp', int(time.time()))}"
#             )
#             key = f"trade_opportunity:{opportunity_id}"
#             await self.redis_client.set(key, json.dumps(opportunity_data), ex=300)
#             await self.publish_trade_opportunity(opportunity_data)
#         except RedisError as e:
#             logger.error(f"Redis error storing trade opportunity: {e}")
#             raise

#     async def publish_trade_opportunity(self, opportunity_data: Dict[str, Any]) -> bool:
#         """Publish trade opportunity with error handling"""
#         try:
#             channel = "trade_opportunity"
#             message = json.dumps(opportunity_data)
#             result = await self.redis_client.publish(channel, message)
#             logger.debug(f"Published opportunity to {result} subscribers")
#             return result > 0
#         except RedisError as e:
#             logger.error(f"Redis error publishing trade opportunity: {e}")
#             return False
#         except Exception as e:
#             logger.error(f"Error publishing trade opportunity: {e}")
#             return False

#     # === Trade Execution Methods ===
#     async def store_trade_execution(
#         self, execution_data: Dict[str, Any], expiry_seconds: int = 3600
#     ) -> str:
#         """Store trade execution with proper error handling"""
#         try:
#             execution_id = (
#                 execution_data.get("id")
#                 or f"exec:{execution_data.get('timestamp', int(time.time()))}"
#             )
#             key = f"trade_execution:{execution_id}"
#             if "timestamp" not in execution_data:
#                 execution_data["timestamp"] = int(time.time())
#             if "id" not in execution_data:
#                 execution_data["id"] = execution_id
#             for field in ["buy_price", "sell_price", "spread"]:
#                 if field in execution_data:
#                     execution_data[field] = float(execution_data[field])
#             await self.redis_client.set(key, json.dumps(execution_data), ex=expiry_seconds)
#             await self.publish_trade_execution(execution_data)
#             ts = execution_data.get("timestamp", int(time.time()))
#             if isinstance(ts, str):
#                 try:
#                     ts = dateutil.parser.parse(ts).timestamp()
#                 except Exception as e:
#                     logger.error(f"Error parsing timestamp {ts}: {e}")
#                     ts = time.time()
#             elif not isinstance(ts, (int, float)):
#                 ts = float(ts)
#             await self.redis_client.zadd("trade_executions_by_time", {execution_id: ts})
#             if "opportunity_id" in execution_data:
#                 await self.redis_client.set(
#                     f"execution_for_opportunity:{execution_data['opportunity_id']}",
#                     execution_id,
#                     ex=expiry_seconds,
#                 )
#             return execution_id
#         except RedisError as e:
#             logger.error(f"Redis error storing trade execution: {e}")
#             raise
#         except Exception as e:
#             logger.error(f"Error storing trade execution: {e}")
#             raise

#     async def subscribe_to_trade_opportunities(
#         self, callback: Optional[Callable[[Dict[str, Any]], None]] = None
#     ) -> AsyncGenerator[Dict[str, Any], None]:
#         """
#         Subscribe to the "trade_opportunity" channel using non-blocking polling.
#         Yields a dictionary for each trade opportunity message received.
#         """
#         pubsub = self.redis_client.pubsub()
#         await pubsub.subscribe("trade_opportunity")
#         logger.info("Subscribed to trade_opportunity channel.")
#         try:
#             while True:
#                 message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
#                 if message:
#                     try:
#                         data = json.loads(message["data"])
#                         if callback:
#                             await callback(data)
#                         yield data
#                     except Exception as e:
#                         logger.error(f"Error decoding trade opportunity message: {e}")
#                 else:
#                     await asyncio.sleep(0.01)
#         finally:
#             logger.info("Cleaning up trade opportunity subscription")
#             await pubsub.unsubscribe("trade_opportunity")
#             await pubsub.close()

#     async def get_trade_execution(self, execution_id: str) -> Optional[Dict[str, Any]]:
#         try:
#             key = f"trade_execution:{execution_id}"
#             data = await self.redis_client.get(key)
#             return json.loads(data) if data else None
#         except RedisError as e:
#             logger.error(f"Redis error retrieving execution {execution_id}: {e}")
#             return None
#         except json.JSONDecodeError:
#             logger.error(f"Malformed JSON for execution {execution_id}")
#             return None

#     async def get_recent_executions(self, count: int = 10) -> list[Dict[str, Any]]:
#         try:
#             exec_ids = await self.redis_client.zrevrange("trade_executions_by_time", 0, count - 1)
#             executions = []
#             for exec_id in exec_ids:
#                 exec_data = await self.get_trade_execution(exec_id)
#                 if exec_data:
#                     executions.append(exec_data)
#             return executions
#         except RedisError as e:
#             logger.error(f"Redis error retrieving recent executions: {e}")
#             return []

#     async def publish_trade_execution(self, execution_data: Dict[str, Any]) -> bool:
#         try:
#             channel = "trade_executions"
#             message = json.dumps(execution_data)
#             result = await self.redis_client.publish(channel, message)
#             logger.debug(f"Published execution to {result} subscribers")
#             return result > 0
#         except RedisError as e:
#             logger.error(f"Redis error publishing trade execution: {e}")
#             return False
#         except Exception as e:
#             logger.error(f"Error publishing trade execution: {e}")
#             return False

#     # === Health Check Methods ===
#     async def is_healthy(self) -> bool:
#         try:
#             return bool(await self.redis_client.ping())
#         except Exception:
#             return False

#     async def reconnect_if_needed(self) -> bool:
#         if not await self.is_healthy():
#             try:
#                 logger.info("Redis connection unhealthy, attempting reconnection...")
#                 await self._connect()
#                 return True
#             except Exception as e:
#                 logger.error(f"Failed to reconnect to Redis: {e}")
#                 return False
#         return True
