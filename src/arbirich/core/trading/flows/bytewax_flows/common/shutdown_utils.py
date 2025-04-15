import logging
import os
import threading

from arbirich.core.state.system_state import is_system_shutting_down, mark_component_notified, mark_system_shutdown

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Global shutdown tracking variables
_force_kill_flags = {}
_force_kill_lock = threading.Lock()
_component_shutdown_notified = {}
_component_notified_lock = threading.Lock()


def mark_force_kill(flow_id: str) -> None:
    """
    Mark a specific flow for force-kill, ensuring all partitions exit immediately.

    Args:
        flow_id: The ID of the flow to force kill
    """
    global _force_kill_flags
    with _force_kill_lock:
        _force_kill_flags[flow_id] = True
        logger.warning(f"{flow_id.upper()} FORCE KILL FLAG SET - all partitions will exit immediately")

    # Set system-wide shutdown flag
    mark_system_shutdown(True)


def is_force_kill_set(flow_id: str) -> bool:
    """
    Check if force kill has been requested for a specific flow.

    Args:
        flow_id: The ID of the flow to check

    Returns:
        True if force kill is set, False otherwise
    """
    with _force_kill_lock:
        return _force_kill_flags.get(flow_id, False)


def handle_component_shutdown(flow_id: str, component_id: str) -> bool:
    """
    Handle shutdown notification for a component with deduplication.

    Args:
        flow_id: The ID of the flow containing this component
        component_id: The ID of the component being notified

    Returns:
        True if component was newly notified, False if already notified
    """
    key = f"{flow_id}:{component_id}"

    # Skip redundant checks if we've already been notified
    with _component_notified_lock:
        if key in _component_shutdown_notified and _component_shutdown_notified[key]:
            return False

    # Only check and log if system is shutting down
    if is_system_shutting_down():
        # Check if this component was already notified
        if mark_component_notified(flow_id, component_id):
            logger.info(f"Stop event detected for {component_id} in {flow_id}")
            # Mark this instance as notified
            with _component_notified_lock:
                _component_shutdown_notified[key] = True
            return True

    return False


def setup_force_exit_timer(timeout: float = 5.0) -> threading.Timer:
    """
    Set up a failsafe timer to force exit the process if normal shutdown gets stuck.

    Args:
        timeout: Time in seconds before forcing exit

    Returns:
        Threading Timer object
    """

    def _force_exit_failsafe():
        logger.critical(f"FAILSAFE TRIGGERED - Force exiting program after {timeout} seconds")
        os._exit(1)

    # Create a timer to force exit after the timeout
    force_exit_timer = threading.Timer(timeout, _force_exit_failsafe)
    force_exit_timer.daemon = True
    force_exit_timer.start()

    return force_exit_timer


def is_shutdown_condition() -> bool:
    """
    Check if any shutdown condition is met (system shutdown or any force kill flag).

    Returns:
        True if shutdown condition is met, False otherwise
    """
    # Check system shutdown first
    if is_system_shutting_down():
        return True

    # Then check any force kill flags
    with _force_kill_lock:
        return any(_force_kill_flags.values())
