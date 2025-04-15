import logging
import threading
from typing import Any

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


def get_redis_client(flow_id: str = "default") -> Any:
    """
    Get or create a Redis client for a specific flow.

    Args:
        flow_id: Identifier for the flow using this client

    Returns:
        Redis client instance or None if unavailable
    """
    global _redis_clients, _client_closed_flags

    # Quick check for shutdown without holding lock
    if is_system_shutting_down():
        return None

    with _redis_client_lock:
        # Double-check shutdown
        if flow_id in _client_closed_flags and _client_closed_flags[flow_id] or is_system_shutting_down():
            return None

        if flow_id not in _redis_clients or _redis_clients[flow_id] is None:
            try:
                # Create a new RedisService instance
                redis_client = get_shared_redis_client()
                _redis_clients[flow_id] = redis_client
                _client_closed_flags[flow_id] = False
                logger.info(f"Created new Redis client for {flow_id}")
            except Exception as e:
                logger.error(f"Error creating Redis client for {flow_id}: {e}")
                return None

        return _redis_clients[flow_id]


def close_redis_client(flow_id: str = "default") -> bool:
    """
    Close a specific Redis client and mark it as closed.

    Args:
        flow_id: Identifier for the flow using this client

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
                logger.info(f"Redis client for {flow_id} closed successfully")
                return True
            except Exception as e:
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
