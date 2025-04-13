"""
Emergency shutdown mechanism for when normal shutdown fails.
"""

import logging
import os
import signal
import threading
import time

import psutil

logger = logging.getLogger(__name__)

# Flag to track if emergency shutdown has been initiated
_emergency_shutdown_active = False
_emergency_lock = threading.Lock()

# Timer thread for force exit
_exit_timer_thread = None
_force_exit_timeout = 10.0  # 10 seconds until forced exit


def is_emergency_shutdown_active():
    """Check if emergency shutdown is currently in progress."""
    global _emergency_shutdown_active
    return _emergency_shutdown_active


def _exit_timer_worker():
    """Worker function for the exit timer thread."""
    logger.critical(f"EMERGENCY SHUTDOWN: System will forcefully exit in {_force_exit_timeout} seconds")
    time.sleep(_force_exit_timeout)
    logger.critical("EMERGENCY SHUTDOWN: Force exiting now")

    # Use os._exit to bypass normal cleanup - this is a last resort
    os._exit(1)


def force_shutdown(timeout=None, kill_processes=True):
    """
    Force the application to shut down, using aggressive measures if necessary.

    Args:
        timeout: Seconds to wait before forcing exit (overrides default timeout)
        kill_processes: Whether to attempt to kill child processes
    """
    global _emergency_shutdown_active, _exit_timer_thread, _force_exit_timeout

    # Use provided timeout if specified
    if timeout is not None and timeout > 0:
        _force_exit_timeout = timeout

    # Only proceed if we haven't already started emergency shutdown
    with _emergency_lock:
        if _emergency_shutdown_active:
            logger.warning("Emergency shutdown already in progress")
            return
        _emergency_shutdown_active = True

    # Log that we're initiating emergency shutdown
    logger.critical("Initiating EMERGENCY SHUTDOWN - normal shutdown has failed")

    # 1. Set system shutdown flag
    try:
        from src.arbirich.core.state.system_state import mark_system_shutdown

        mark_system_shutdown(True)
    except Exception as e:
        logger.error(f"Error marking system shutdown: {e}")
        pass

    # 2. Kill child processes if requested
    if kill_processes:
        _kill_child_processes()

    # 3. Stop all flows
    try:
        from src.arbirich.core.trading.flows.flow_manager import FlowManager

        FlowManager.stop_all_flows()
    except Exception as e:
        logger.error(f"Error stopping flows: {e}")
        pass

    # 4. Reset Redis connections
    try:
        from src.arbirich.services.redis.redis_service import reset_redis_pool

        reset_redis_pool()
    except Exception as e:
        logger.error(f"Error resetting Redis pool: {e}")
        pass

    # 5. Start a timer thread that will forcefully exit the process if needed
    _exit_timer_thread = threading.Thread(target=_exit_timer_worker, name="emergency_exit_timer", daemon=True)
    _exit_timer_thread.start()

    # 6. Send SIGINT to ourselves (like Ctrl+C) to trigger normal shutdown handlers
    try:
        os.kill(os.getpid(), signal.SIGINT)
    except Exception as e:
        logger.error(f"Error sending SIGINT: {e}")
        pass

    logger.warning(f"Emergency shutdown initiated, process will exit in {_force_exit_timeout} seconds")
    return True


def _kill_child_processes():
    """Kill all child processes of the current process."""
    try:
        # Get current process
        current_process = psutil.Process()

        # Find all children (recursively)
        children = current_process.children(recursive=True)
        logger.warning(f"Found {len(children)} child processes to kill during emergency shutdown")

        # Try SIGTERM first
        for child in children:
            try:
                logger.warning(f"Sending SIGTERM to process {child.pid}")
                child.terminate()
            except Exception as e:
                logger.error(f"Error terminating child process {child.pid}: {e}")
                pass

        # Give them a moment to terminate
        _, still_alive = psutil.wait_procs(children, timeout=2)

        # Use SIGKILL for any that are still alive
        for child in still_alive:
            try:
                logger.warning(f"Sending SIGKILL to process {child.pid}")
                child.kill()
            except Exception as e:
                logger.error(f"Error killing child process {child.pid}: {e}")
                pass

        logger.warning(
            f"Killed {len(children) - len(still_alive)} processes with SIGTERM, {len(still_alive)} with SIGKILL"
        )

    except Exception as e:
        logger.error(f"Error killing child processes: {e}")
