import logging
import threading
from typing import Dict, Set

logger = logging.getLogger(__name__)

# Global state flags
_system_shutting_down = False
_shutdown_lock = threading.RLock()

# Track which components have been notified of shutdown
_notified_components: Dict[str, Set[str]] = {
    "detection": set(),
    "execution": set(),
    "ingestion": set(),
    "reporting": set(),
}
_notification_lock = threading.RLock()


def is_system_shutting_down() -> bool:
    """Check if the system is in the process of shutting down.

    Returns:
        bool: True if the system is shutting down, False otherwise
    """
    global _system_shutting_down
    return _system_shutting_down


def mark_system_shutdown(shutting_down: bool = True) -> None:
    """Set the system shutdown flag.

    Args:
        shutting_down: Whether the system is shutting down
    """
    global _system_shutting_down
    with _shutdown_lock:
        old_value = _system_shutting_down
        _system_shutting_down = shutting_down

        # Log only if the value changes
        if old_value != shutting_down:
            action = "starting" if not shutting_down else "shutting down"
            logger.info(f"System state changed: {action}")


def mark_component_notified(component_type: str, component_id: str) -> bool:
    """Mark a component as having been notified of shutdown.

    This helps prevent duplicate shutdown messages/actions for the same component.

    Args:
        component_type: Type of component ("detection", "execution", etc.)
        component_id: Unique identifier for the specific component instance

    Returns:
        bool: True if this is the first notification for this component, False if already notified
    """
    global _notified_components

    with _notification_lock:
        # Ensure the component type exists
        if component_type not in _notified_components:
            _notified_components[component_type] = set()

        # Check if this component was already notified
        if component_id in _notified_components[component_type]:
            return False

        # Mark as notified
        _notified_components[component_type].add(component_id)
        return True


def reset_notification_state() -> None:
    """Reset the notification state for all components.

    This should be called after a shutdown to prepare for next startup.
    """
    global _notified_components

    with _notification_lock:
        for component_type in _notified_components:
            _notified_components[component_type].clear()

        logger.info("Component notification states reset")


def get_notification_stats() -> Dict[str, int]:
    """Get statistics about notified components.

    Returns:
        Dict mapping component types to the number of notified instances
    """
    with _notification_lock:
        return {component_type: len(instances) for component_type, instances in _notified_components.items()}


def set_stop_event() -> None:
    """Signal all execution partitions to stop.

    This is a shortcut function to set the system shutdown flag.
    """
    mark_system_shutdown(True)
    logger.debug("Stop event set for all execution partitions")

    # Add this new section to close websocket connections
    try:
        # Try to import and access the connection manager to close active WebSockets
        from src.arbirich.web.websockets import manager

        if hasattr(manager, "active_connections") and manager.active_connections:
            logger.info(f"Forcefully closing {len(manager.active_connections)} active WebSocket connections")

            # We can't use async code here directly, so we'll just remove the connections from the list
            # This will prevent further messages from being sent
            manager.active_connections.clear()
            logger.info("WebSocket connection list cleared")
    except ImportError:
        logger.debug("WebSocket manager not available, skipping connection cleanup")
    except Exception as e:
        logger.error(f"Error cleaning up websocket connections: {e}")
