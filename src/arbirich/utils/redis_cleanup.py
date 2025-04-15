"""
Redis cleanup utilities to ensure safe connection closing across the application.
"""

import logging
import threading
from typing import Any, List

logger = logging.getLogger(__name__)

# Registry of Redis-related resources to clean up
_resources_lock = threading.RLock()
_pubsub_resources = []
_redis_client_resources = []
_registered_cleanup_funcs = []
_named_cleanup_funcs = {}  # New dictionary to store named cleanup functions


def register_pubsub(pubsub: Any) -> None:
    """Register a Redis PubSub instance for cleanup during shutdown."""
    with _resources_lock:
        if pubsub is not None and pubsub not in _pubsub_resources:
            _pubsub_resources.append(pubsub)
            logger.debug(f"Registered Redis PubSub for cleanup (total: {len(_pubsub_resources)})")


def register_redis_client(client_or_name: Any, cleanup_func: callable = None) -> None:
    """
    Register a Redis client instance or named cleanup function for cleanup during shutdown.

    This function supports two usage patterns:
    1. register_redis_client(client) - Register a Redis client object
    2. register_redis_client("name", cleanup_func) - Register a named cleanup function

    Args:
        client_or_name: Either a Redis client object or a name string
        cleanup_func: Optional cleanup function if client_or_name is a string
    """
    with _resources_lock:
        # Case 1: Called with client object only
        if cleanup_func is None:
            client = client_or_name
            if client is not None and client not in _redis_client_resources:
                _redis_client_resources.append(client)
                logger.debug(f"Registered Redis client for cleanup (total: {len(_redis_client_resources)})")
        # Case 2: Called with name and cleanup function
        else:
            name = client_or_name
            if isinstance(name, str) and cleanup_func is not None:
                _named_cleanup_funcs[name] = cleanup_func
                logger.debug(f"Registered named cleanup function for '{name}'")


def register_cleanup_function(func: callable) -> None:
    """Register a cleanup function to be called during shutdown."""
    with _resources_lock:
        if func is not None and func not in _registered_cleanup_funcs:
            _registered_cleanup_funcs.append(func)
            logger.debug(f"Registered Redis cleanup function (total: {len(_registered_cleanup_funcs)})")


def get_registered_pubsubs() -> List[Any]:
    """Get the list of registered PubSub objects."""
    with _resources_lock:
        return list(_pubsub_resources)


def get_registered_clients() -> List[Any]:
    """Get the list of registered Redis clients."""
    with _resources_lock:
        return list(_redis_client_resources)


def get_registered_cleanup_functions() -> List[callable]:
    """Get the list of registered cleanup functions."""
    with _resources_lock:
        return list(_registered_cleanup_funcs)


def safe_close_pubsub(pubsub: Any) -> bool:
    """
    Safely close a Redis PubSub object.
    Returns True if successful or if object was None, False on error.
    """
    if pubsub is None:
        return True

    try:
        # First try to unsubscribe
        try:
            pubsub.unsubscribe()
        except Exception as e:
            logger.debug(f"Non-critical error during PubSub unsubscribe: {e}")

        # Then close the PubSub
        pubsub.close()

        # Remove from registry if registered
        with _resources_lock:
            if pubsub in _pubsub_resources:
                _pubsub_resources.remove(pubsub)

        return True
    except Exception as e:
        logger.warning(f"Error closing Redis PubSub: {e}")
        return False


def safe_close_redis_client(client: Any) -> bool:
    """
    Safely close a Redis client object.
    Returns True if successful or if object was None, False on error.
    """
    if client is None:
        return True

    try:
        # Check for client attribute - handle RedisService object
        if hasattr(client, "client") and client.client is not None:
            client.client.close()
        elif hasattr(client, "close"):
            client.close()

        # Remove from registry if registered
        with _resources_lock:
            if client in _redis_client_resources:
                _redis_client_resources.remove(client)

        return True
    except Exception as e:
        logger.warning(f"Error closing Redis client: {e}")
        return False


def cleanup_all_redis_resources() -> bool:
    """
    Clean up all registered Redis resources.
    Returns True if all cleanups were successful, False otherwise.
    """
    all_success = True
    errors = []

    # Call registered cleanup functions first
    with _resources_lock:
        cleanup_funcs = list(_registered_cleanup_funcs)
        named_funcs = dict(_named_cleanup_funcs)  # Make a copy to avoid modification during iteration

    # Call regular cleanup functions
    for func in cleanup_funcs:
        try:
            func()
        except Exception as e:
            all_success = False
            errors.append(f"Error in cleanup function {func.__name__}: {e}")

    # Call named cleanup functions
    for name, func in named_funcs.items():
        try:
            func()
            logger.info(f"Executed cleanup function for '{name}'")
        except Exception as e:
            all_success = False
            errors.append(f"Error in named cleanup function '{name}': {e}")

    # Close all PubSubs
    with _resources_lock:
        pubsubs = list(_pubsub_resources)
        _pubsub_resources.clear()

    for pubsub in pubsubs:
        try:
            safe_close_pubsub(pubsub)
        except Exception as e:
            all_success = False
            errors.append(f"Error closing PubSub: {e}")

    # Close all Redis clients
    with _resources_lock:
        clients = list(_redis_client_resources)
        _redis_client_resources.clear()

    for client in clients:
        try:
            safe_close_redis_client(client)
        except Exception as e:
            all_success = False
            errors.append(f"Error closing Redis client: {e}")

    # Log all errors if any
    if errors:
        for error in errors:
            logger.error(error)

    return all_success


async def safe_close_redis_resources():
    """
    Async wrapper around cleanup_all_redis_resources.
    Doesn't actually use await but provides compatibility with async shutdown sequences.
    """
    return cleanup_all_redis_resources()
