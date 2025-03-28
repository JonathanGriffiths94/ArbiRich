"""System-wide state management for graceful shutdown coordination."""

import logging
import threading
from typing import Dict, Set

logger = logging.getLogger(__name__)

# Global shutdown flag with thread safety
_system_shutting_down = False
_shutdown_lock = threading.Lock()

# Track which components have already received shutdown signals
_notified_components: Dict[str, Set[str]] = {
    "ingestion": set(),
    "arbitrage": set(),
    "execution": set(),
    "reporting": set(),
}
_components_lock = threading.RLock()


def mark_system_shutdown(shutting_down: bool):
    """
    Mark the system as shutting down or not shutting down.

    Args:
        shutting_down: True to mark as shutting down, False otherwise.
    """
    global _system_shutting_down
    with _shutdown_lock:
        _system_shutting_down = shutting_down
        logger.info(f"System shutdown flag set to {shutting_down}")
        logger.debug(f"Current shutdown flag state: {_system_shutting_down}")


def is_system_shutting_down() -> bool:
    """
    Check if the system is shutting down.

    Returns:
        True if the system is shutting down, False otherwise.
    """
    with _shutdown_lock:
        logger.debug(f"Checking shutdown flag: {_system_shutting_down}")
        return _system_shutting_down


# Update the mark_component_notified function to reduce debug logging
def mark_component_notified(component_type: str, component_id: str) -> bool:
    """
    Mark a specific component as having been notified of shutdown.
    Returns True if this is the first notification for this component, False if already notified.

    This function is optimized to reduce excessive logging during shutdown.
    """
    with _components_lock:
        # Initialize component type if needed
        if component_type not in _notified_components:
            _notified_components[component_type] = set()

        # Check if already notified - silently return false
        if component_id in _notified_components[component_type]:
            return False  # Already notified

        # First time notification - log and track
        _notified_components[component_type].add(component_id)
        logger.info(f"Component {component_id} of type {component_type} marked as notified for shutdown")
        return True  # First notification


def reset_notification_state():
    """Reset the notification state and shutdown flag for system restart"""
    global _system_shutting_down, _notified_components

    with _shutdown_lock:
        _system_shutting_down = False

    with _components_lock:
        for component_type in _notified_components:
            _notified_components[component_type].clear()

    logger.info("System state reset - shutdown flag and notification state cleared")
    logger.debug(f"Current shutdown flag: {_system_shutting_down}, Notified components: {_notified_components}")


def set_stop_event():
    """Signal all execution partitions to stop."""
    mark_system_shutdown(True)
    logger.debug("Stop event set for all execution partitions")
