import json
import logging
import threading
import time
from typing import Any, Optional

import redis

from arbirich.core.state.system_state import is_system_shutting_down
from src.arbirich.services.redis.redis_service import get_shared_redis_client, reset_redis_pool

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Global Redis variables with proper locks
_redis_client_lock = threading.RLock()
_redis_clients = {}  # Client registry by flow_id
_client_closed_flags = {}  # Track which clients are closed
_all_pubsubs = []  # Track all pubsubs for proper cleanup
_pubsub_lock = threading.Lock()

# Health check tracking
_last_health_check = {}
_HEALTH_CHECK_INTERVAL = 30  # Check health every 30 seconds
_MAX_CONNECTION_RETRIES = 3  # Maximum number of retries for creating a Redis connection

# Track repository objects for debugging
_repositories = {}

# Track processed opportunity IDs for deduplication
_processed_opportunity_ids = set()
_MAX_PROCESSED_IDS = 1000


def get_redis_client(flow_id: str = "default") -> Any:
    """
    Get or create a Redis client for a specific flow with health checks.

    Args:
        flow_id: Identifier for the flow using this client

    Returns:
        Redis client instance or None if unavailable
    """
    global _redis_clients, _client_closed_flags, _last_health_check

    # Quick check for shutdown without holding lock
    if is_system_shutting_down():
        return None

    with _redis_client_lock:
        # Double-check shutdown
        if flow_id in _client_closed_flags and _client_closed_flags[flow_id] or is_system_shutting_down():
            return None

        current_time = time.time()

        # Check if we need to validate the health of an existing client
        if (
            flow_id in _redis_clients
            and _redis_clients[flow_id] is not None
            and flow_id in _last_health_check
            and current_time - _last_health_check[flow_id] > _HEALTH_CHECK_INTERVAL
        ):
            # Perform health check on existing connection
            client = _redis_clients[flow_id]
            if not is_redis_client_healthy(client):
                logger.warning(f"Redis client for {flow_id} failed health check, creating new connection")
                close_redis_client(flow_id, silent=True)
                _redis_clients[flow_id] = None

        # Create a new client if needed
        if flow_id not in _redis_clients or _redis_clients[flow_id] is None:
            # Implement retry logic for creating Redis client
            for retry in range(_MAX_CONNECTION_RETRIES):
                try:
                    # Create a new RedisService instance
                    redis_client = get_shared_redis_client()

                    # Verify the client is working with a ping test
                    if redis_client and is_redis_client_healthy(redis_client):
                        _redis_clients[flow_id] = redis_client
                        _client_closed_flags[flow_id] = False
                        _last_health_check[flow_id] = current_time
                        logger.info(f"✅ Created new Redis client for {flow_id}")
                        break  # Success - exit retry loop
                    else:
                        logger.warning(
                            f"❌ Redis client health check failed on attempt {retry + 1}/{_MAX_CONNECTION_RETRIES}"
                        )
                        # Pause before retry
                        time.sleep(0.5 * (retry + 1))

                        # Force reset redis pool before last retry attempt
                        if retry == _MAX_CONNECTION_RETRIES - 2:
                            try:
                                logger.warning("Attempting to reset Redis connection pool before final retry")
                                reset_redis_pool()
                            except Exception as reset_error:
                                logger.error(f"Error resetting Redis pool: {reset_error}")
                except Exception as e:
                    logger.error(
                        f"❌ Error creating Redis client for {flow_id} (attempt {retry + 1}/{_MAX_CONNECTION_RETRIES}): {e}"
                    )
                    # Pause before retry with increasing backoff
                    time.sleep(0.5 * (retry + 1))

            # Check if all retries failed
            if flow_id not in _redis_clients or _redis_clients[flow_id] is None:
                logger.critical(
                    f"❌ All {_MAX_CONNECTION_RETRIES} attempts to create Redis client for {flow_id} failed"
                )
                return None

        # Update the last health check time to avoid frequent checks
        _last_health_check[flow_id] = current_time
        return _redis_clients[flow_id]


def is_redis_client_healthy(client) -> bool:
    """
    Check if a Redis client is healthy by attempting a ping.

    Args:
        client: The Redis client to check

    Returns:
        bool: True if healthy, False otherwise
    """
    if client is None:
        return False

    try:
        # Try different client structures
        if hasattr(client, "is_healthy") and callable(client.is_healthy):
            # Use built-in health check if available
            return client.is_healthy()
        elif hasattr(client, "client") and hasattr(client.client, "ping"):
            # Service wrapper with client attribute
            return bool(client.client.ping())
        elif hasattr(client, "ping") and callable(client.ping):
            # Direct redis client
            return bool(client.ping())
        else:
            logger.warning("Unknown Redis client structure, can't verify health")
            return False
    except Exception as e:
        logger.debug(f"Redis health check failed: {e}")
        return False


def close_redis_client(flow_id: str = "default", silent: bool = False) -> bool:
    """
    Close a specific Redis client and mark it as closed.

    Args:
        flow_id: Identifier for the flow using this client
        silent: Whether to suppress log messages

    Returns:
        True if client was closed successfully, False otherwise
    """
    global _redis_clients, _client_closed_flags

    with _redis_client_lock:
        if flow_id in _redis_clients and _redis_clients[flow_id] is not None:
            try:
                client = _redis_clients[flow_id]
                # Handle different client types
                if hasattr(client, "client") and hasattr(client.client, "close"):
                    client.client.close()
                elif hasattr(client, "close"):
                    client.close()

                _redis_clients[flow_id] = None
                _client_closed_flags[flow_id] = True

                if not silent:
                    logger.info(f"Redis client for {flow_id} closed successfully")
                return True
            except Exception as e:
                if not silent:
                    logger.error(f"Error closing Redis client for {flow_id}: {e}")

        return False


def register_pubsub(pubsub) -> None:
    """
    Register a PubSub instance for global tracking and cleanup.

    Args:
        pubsub: Redis PubSub instance to register
    """
    with _pubsub_lock:
        _all_pubsubs.append(pubsub)


def close_all_pubsubs() -> None:
    """Close all tracked PubSub connections."""
    with _pubsub_lock:
        for pubsub in _all_pubsubs:
            try:
                if pubsub:
                    try:
                        pubsub.unsubscribe()
                    except Exception:
                        pass
                    try:
                        pubsub.close()
                    except Exception:
                        pass
                    logger.info("Closed pubsub connection")
            except Exception as e:
                logger.warning(f"Error closing pubsub: {e}")

        # Clear the list
        _all_pubsubs.clear()
        logger.info("All PubSub connections closed and cleared")


def reset_all_redis_connections() -> None:
    """Forcefully reset all Redis connections and pools."""
    # Close all tracked pubsubs first
    close_all_pubsubs()

    # Close all clients
    with _redis_client_lock:
        for flow_id in list(_redis_clients.keys()):
            close_redis_client(flow_id)

        # Clear dictionaries
        _redis_clients.clear()
        _client_closed_flags.clear()

    # Force clean TCP connections to Redis
    try:
        # Reset all connection pools
        reset_redis_pool()
        logger.info("Reset all Redis connection pools")

        # More aggressive connection pool cleanup
        try:
            pools = redis.Redis.connection_pool.pools if hasattr(redis.Redis, "connection_pool") else {}
            for key, pool in pools.items():
                try:
                    pool.disconnect()
                    logger.info(f"Disconnected pool: {key}")
                except Exception as e:
                    logger.warning(f"Error disconnecting pool {key}: {e}")
        except Exception:
            # More graceful handling of Redis connection errors
            try:
                redis.Redis().connection_pool.disconnect()
            except Exception as e:
                logger.warning(f"Error disconnecting default Redis pool: {e}")

        logger.info("Forcibly disconnected all Redis connections")
    except Exception as e:
        logger.warning(f"Error during force Redis disconnect: {e}")


def publish_message(channel: str, message: Any, component_id: str) -> int:
    """
    Publish a message to Redis with consistent formatting.

    Args:
        channel: Channel to publish to
        message: Message to publish
        component_id: Component ID for tracking

    Returns:
        Number of subscribers that received the message
    """
    try:
        redis_client = get_shared_redis_client().client

        # Check if message is already a string
        if isinstance(message, str):
            json_message = message
        elif isinstance(message, dict):
            # Direct dict - serialize without event wrapper
            json_message = json.dumps(message)
            logger.debug(f"[{component_id}] Publishing dict directly with keys: {list(message.keys())}")
        elif hasattr(message, "json"):
            # Pydantic v1
            json_message = message.json()
            logger.debug(f"[{component_id}] Publishing using Pydantic json() method")
        elif hasattr(message, "model_dump_json"):
            # Pydantic v2
            json_message = message.model_dump_json()
            logger.debug(f"[{component_id}] Publishing using Pydantic model_dump_json() method")
        else:
            # Unknown type - use string representation
            json_message = json.dumps(str(message))
            logger.warning(f"[{component_id}] Unknown message type: {type(message)}")

        # Publish the message
        subscribers = redis_client.publish(channel, json_message)
        logger.debug(f"[{component_id}] Published message to {channel}: {subscribers} subscribers")

        return subscribers
    except Exception as e:
        logger.error(f"[{component_id}] Error publishing message to {channel}: {e}")
        return 0


def get_repository(repo_type: str, component_id: str) -> Optional[Any]:
    """
    Get a repository with additional debugging.

    Args:
        repo_type: Type of repository to get
        component_id: Component requesting the repository

    Returns:
        The repository object
    """
    redis_client = get_shared_redis_client()
    if not redis_client:
        logger.error(f"Cannot create {repo_type} repository: No Redis client available")
        return None

    try:
        # Create repository instance
        repo = None
        if repo_type == "trade_opportunity":
            from src.arbirich.services.redis.repositories.trade_opportunity_repository import TradeOpportunityRepository

            repo = TradeOpportunityRepository(redis_client.client)

            # Sync with database repository
            try:
                from src.arbirich.services.database.repositories.trade_opportunity_repository import (
                    _processed_opportunity_ids as db_ids,
                )

                # Share IDs between repositories for better coordination
                for id_value in db_ids:
                    if hasattr(repo, "mark_as_published"):
                        repo.mark_as_published(id_value)
            except Exception as sync_error:
                logger.warning(f"Could not sync with database repository: {sync_error}")

        elif repo_type == "trade_execution":
            from src.arbirich.services.redis.repositories.trade_execution_repository import TradeExecutionRepository

            repo = TradeExecutionRepository(redis_client.client)
        elif repo_type == "order_book":
            from src.arbirich.services.redis.repositories.order_book_repository import OrderBookRepository

            repo = OrderBookRepository(redis_client.client)
        elif repo_type == "metrics":
            from src.arbirich.services.redis.repositories.metrics_repository import MetricsRepository

            repo = MetricsRepository(redis_client.client)
        else:
            logger.warning(f"Unknown repository type: {repo_type}")
            return None

        # Store for debugging
        if repo:
            _repositories[f"{component_id}:{repo_type}"] = repo

        # Add additional debug methods if needed
        if not hasattr(repo, "publish_raw"):
            # Add a publish_raw method that skips event wrapping
            def publish_raw(data):
                """Publish raw data without event wrapping"""
                try:
                    logger.debug(
                        f"Publishing raw data with keys: {list(data.keys()) if isinstance(data, dict) else 'non-dict'}"
                    )

                    # Get the publish channel(s) from the repository
                    channels = []
                    if repo_type == "trade_opportunity":
                        from src.arbirich.constants import TRADE_OPPORTUNITIES_CHANNEL

                        channels.append(TRADE_OPPORTUNITIES_CHANNEL)
                    elif repo_type == "trade_execution":
                        from src.arbirich.constants import TRADE_EXECUTIONS_CHANNEL

                        channels.append(TRADE_EXECUTIONS_CHANNEL)
                    elif repo_type == "order_book":
                        from src.arbirich.constants import ORDER_BOOK_CHANNEL

                        # For order books, need exchange and symbol
                        if isinstance(data, dict) and "exchange" in data and "symbol" in data:
                            channel = f"{ORDER_BOOK_CHANNEL}:{data['exchange']}:{data['symbol']}"
                            channels.append(channel)

                    # For opportunity repos, also publish to strategy-specific channel
                    if hasattr(data, "strategy") or (isinstance(data, dict) and "strategy" in data):
                        from src.arbirich.models.enums import ChannelName

                        strategy = data.strategy if hasattr(data, "strategy") else data.get("strategy")
                        if strategy:
                            strategy_channel = f"{ChannelName.TRADE_OPPORTUNITIES.value}:{strategy}"
                            channels.append(strategy_channel)

                    # Publish to all channels
                    total_subscribers = 0
                    redis_client = get_shared_redis_client().client

                    for channel in channels:
                        # Convert data to JSON string
                        if isinstance(data, dict):
                            message = json.dumps(data)
                        elif hasattr(data, "json"):
                            message = data.json()
                        elif hasattr(data, "model_dump_json"):
                            message = data.model_dump_json()
                        else:
                            message = json.dumps(str(data))

                        # Publish directly without wrapping
                        subscribers = redis_client.publish(channel, message)
                        logger.debug(f"Published raw data to {channel}: {subscribers} subscribers")
                        total_subscribers += subscribers

                    return total_subscribers
                except Exception as e:
                    logger.error(f"Error in publish_raw: {e}")
                    return 0

            # Add the method to the repository
            repo.publish_raw = publish_raw

        logger.debug(f"Retrieved repository {repo_type} for {component_id}")
        return repo
    except Exception as e:
        logger.error(f"Error creating {repo_type} repository: {e}")
        return None


def sync_opportunity_tracking() -> bool:
    """
    Synchronize opportunity tracking between Redis and database repositories.
    This ensures that both repositories are aware of the same processed opportunities.

    Returns:
        bool: True if sync was successful, False otherwise
    """
    try:
        from src.arbirich.services.database.repositories.trade_opportunity_repository import (
            _processed_opportunity_ids as db_ids,
        )
        from src.arbirich.services.redis.repositories.trade_opportunity_repository import TradeOpportunityRepository

        # Get Redis repository
        redis_client = get_shared_redis_client()
        if not redis_client:
            logger.warning("Cannot sync repositories: No Redis client available")
            return False

        redis_repo = TradeOpportunityRepository(redis_client.client)

        # Get IDs from Redis
        redis_ids = redis_repo.get_recently_published_ids()

        # Update database tracking with Redis IDs
        for id_value in redis_ids:
            db_ids.add(id_value)

        # Update Redis tracking with database IDs
        for id_value in db_ids:
            redis_repo.mark_as_published(id_value)

        logger.debug(f"Synchronized opportunity tracking: {len(db_ids)} database IDs, {len(redis_ids)} Redis IDs")
        return True
    except Exception as e:
        logger.error(f"Error synchronizing opportunity tracking: {e}")
        return False
