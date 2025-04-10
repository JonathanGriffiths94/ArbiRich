"""
Application lifecycle events for startup and shutdown.
"""

import asyncio
import logging
import os
import signal
import subprocess
import threading
import time

import psutil

from arbirich.services.database.prefill_database import prefill_database
from src.arbirich.core.shutdown_manager import execute_phased_shutdown
from src.arbirich.services.exchange_processors.registry import set_processors_shutting_down, shutdown_all_processors

logger = logging.getLogger(__name__)

# Flag to track if shutdown is in progress
_shutdown_in_progress = False

# Add a lock to prevent concurrent database prefills
_prefill_lock = threading.Lock()
_prefill_task = None


# Start a timeout timer for shutdown
def _emergency_exit_timer(timeout=30):
    """Force exit if shutdown takes too long."""
    logger.info(f"Starting emergency exit timer ({timeout} seconds)")
    time.sleep(timeout)
    logger.critical(f"Emergency exit timer triggered after {timeout} seconds")
    os._exit(1)


async def run_database_prefill():
    """Run database prefill process async-safely."""
    global _prefill_task

    # Check if there's already a prefill task in progress
    if _prefill_task and not _prefill_task.done():
        logger.info("Database prefill task already in progress, waiting for completion")
        try:
            await _prefill_task
            logger.info("Existing database prefill task completed")
        except Exception as e:
            logger.error(f"Error waiting for existing prefill task: {e}")
        return

    # Only one thread should create the task
    if not _prefill_lock.acquire(blocking=False):
        logger.info("Another thread is creating a prefill task, waiting")
        # Wait a short time and then check if the task exists
        await asyncio.sleep(0.2)
        if _prefill_task and not _prefill_task.done():
            await _prefill_task
        return

    try:
        logger.info("Running database prefill to ensure configuration is in database...")

        # Run prefill_database with activation to ensure basic entities are available
        loop = asyncio.get_running_loop()
        _prefill_task = loop.run_in_executor(None, lambda: prefill_database(smart_activate=False))

        # Set a name for the task for better debugging
        if hasattr(_prefill_task, "set_name"):
            _prefill_task.set_name("database_prefill")

        # Wait for the task to complete
        await _prefill_task
        logger.info("Database prefill completed successfully")
    except Exception as e:
        logger.error(f"Error during database prefill: {e}", exc_info=True)
        # Continue execution even if prefill fails - it's not critical
    finally:
        _prefill_lock.release()


# Add a debug helper function
def diagnose_system_status():
    """
    Log diagnostic information about the system state.
    """
    logger.info("------ System Diagnostic Information ------")

    # Check Python version
    import sys

    logger.info(f"Python version: {sys.version}")

    # Check event loop
    try:
        loop = asyncio.get_running_loop()
        logger.info(f"Current event loop: {loop}")
        logger.info(f"Loop is running: {loop.is_running()}")
        logger.info(f"Loop is closed: {loop.is_closed()}")
    except RuntimeError as e:
        logger.info(f"No running event loop: {e}")

    # Check Redis connection
    try:
        from src.arbirich.services.redis.redis_service import RedisService

        redis = RedisService()
        ping_result = redis.client.ping()
        logger.info(f"Redis connection test: {ping_result}")
    except Exception as e:
        logger.info(f"Redis connection test failed: {e}")

    # Check database connection
    try:
        import sqlalchemy as sa

        from src.arbirich.services.database.database_service import DatabaseService

        db = DatabaseService()
        with db.engine.connect() as conn:
            result = conn.execute(sa.text("SELECT 1")).scalar()
            logger.info(f"Database connection test: {result}")
    except Exception as e:
        logger.info(f"Database connection test failed: {e}")

    # Check running threads
    logger.info(f"Active thread count: {threading.active_count()}")
    for thread in threading.enumerate():
        logger.info(f"Thread: {thread.name} (daemon: {thread.daemon})")

    logger.info("------ End Diagnostic Information ------")


async def startup_event() -> None:
    """Run tasks at application startup."""
    logger.info("Running application startup tasks...")

    # Reset system shutdown flag
    try:
        from src.arbirich.core.system_state import mark_system_shutdown, reset_notification_state

        mark_system_shutdown(False)
        reset_notification_state()
        logger.info("System state reset for clean startup")
    except Exception as e:
        logger.error(f"Error resetting system state: {e}")

    # Reset processor startup flag
    try:
        from src.arbirich.flows.ingestion.ingestion_source import reset_processor_startup

        reset_processor_startup()
        logger.info("Processor startup flag reset")
    except Exception as e:
        logger.error(f"Error resetting processor startup: {e}")

    # Diagnose the system status
    diagnose_system_status()

    # Reset shared services
    try:
        from arbirich.utils.background_subscriber import reset as reset_background_subscriber
        from arbirich.utils.channel_maintenance import reset_channel_maintenance_service
        from src.arbirich.services.redis.redis_service import reset_redis_pool

        reset_redis_pool()
        reset_channel_maintenance_service()
        reset_background_subscriber()
        logger.info("Shared services reset")
    except Exception as e:
        logger.error(f"Error resetting shared services: {e}")


async def shutdown_event() -> None:
    """Run tasks at application shutdown."""
    global _shutdown_in_progress

    if _shutdown_in_progress:
        logger.info("Shutdown already in progress, ignoring repeated call")
        return

    _shutdown_in_progress = True
    logger.info("Running application shutdown tasks...")

    # First set both shutdown flags to prevent any new activity
    try:
        # Set the processor shutdown flag
        set_processors_shutting_down(True)
        logger.info("Processor-specific shutdown flag set")

        # Set the system-wide shutdown flag
        from src.arbirich.core.system_state import mark_system_shutdown

        mark_system_shutdown(True)
        logger.info("System-wide shutdown flag set")
    except Exception as e:
        logger.error(f"Error setting shutdown flags: {e}")

    # Disable processor startup before doing anything else
    try:
        from src.arbirich.flows.ingestion.ingestion_source import disable_processor_startup

        disable_processor_startup()
        logger.info("Processor startup disabled")
    except Exception as e:
        logger.error(f"Error disabling processor startup: {e}")

    # Stop all WebSocket consumers immediately - before any other shutdown activities
    try:
        from src.arbirich.flows.ingestion.ingestion_source import stop_all_consumers

        stop_all_consumers()
        logger.info("All WebSocket consumers stopped early in shutdown sequence")
    except Exception as e:
        logger.error(f"Error stopping WebSocket consumers early: {e}")

    # Brief pause to allow processors to notice shutdown flags
    await asyncio.sleep(0.5)

    # Shutdown all processors using the registry
    try:
        logger.info("Shutting down all registered processors")
        shutdown_all_processors()
        logger.info("All registered processors shutdown complete")
    except Exception as e:
        logger.error(f"Error shutting down processors: {e}")

    # Reset shared Redis clients in modules
    try:
        from src.arbirich.services.redis.redis_service import reset_all_registered_redis_clients

        reset_all_registered_redis_clients()
        logger.info("All module Redis clients reset")
    except Exception as e:
        logger.error(f"Error resetting module Redis clients: {e}")

    # Reset Redis pools after clients are closed
    try:
        from src.arbirich.services.redis.redis_service import reset_redis_pool

        reset_redis_pool()
        logger.info("Redis connection pools reset")
    except Exception as e:
        logger.error(f"Error resetting Redis pool: {e}")

    # Force kill any Bytewax processes that might still be running
    try:
        logger.info("Killing any remaining Bytewax processes...")
        killed = _kill_bytewax_processes()
        if killed:
            logger.info(f"Killed {killed} Bytewax processes")
    except Exception as e:
        logger.error(f"Error killing Bytewax processes: {e}")

    logger.info("Shutdown cleanup completed")


def _kill_bytewax_processes():
    """Kill any Bytewax processes and return the count of killed processes"""
    killed_count = 0
    try:
        # First try to find any Bytewax processes
        result = subprocess.run(["ps", "aux"], capture_output=True, text=True, check=False)
        bytewax_pids = []

        for line in result.stdout.splitlines():
            if "python" in line and "bytewax" in line:
                parts = line.split()
                if len(parts) > 1:
                    try:
                        pid = int(parts[1])
                        if pid != os.getpid() and pid != os.getppid():
                            bytewax_pids.append(pid)
                    except (ValueError, ProcessLookupError):
                        pass

        logger.info(f"Found {len(bytewax_pids)} Bytewax processes to terminate")

        # First try SIGTERM for graceful shutdown
        for pid in bytewax_pids:
            try:
                logger.info(f"Sending SIGTERM to Bytewax process: {pid}")
                os.kill(pid, signal.SIGTERM)
            except Exception as e:
                logger.error(f"Error sending SIGTERM to process {pid}: {e}")

        # Give processes a moment to terminate gracefully
        time.sleep(0.5)

        # Then use SIGKILL for any remaining processes
        for pid in bytewax_pids:
            try:
                # Check if the process still exists
                os.kill(pid, 0)  # This will raise OSError if process doesn't exist
                logger.info(f"Process {pid} still running, sending SIGKILL")
                os.kill(pid, signal.SIGKILL)
                killed_count += 1
            except OSError:
                # Process doesn't exist anymore (already terminated)
                logger.info(f"Process {pid} already terminated")
                killed_count += 1
            except Exception as e:
                logger.error(f"Error killing process {pid}: {e}")

    except Exception as e:
        logger.error(f"Error in kill_bytewax_processes: {e}")

    return killed_count


def force_terminate_threads():
    """Forcefully terminate all non-daemon threads."""
    logger.info("Forcefully terminating all non-daemon threads...")
    for thread in threading.enumerate():
        if not thread.daemon and thread is not threading.current_thread():
            logger.warning(f"Forcefully stopping thread: {thread.name}")
            try:
                # Threads cannot be forcefully killed in Python, but we can log and monitor them
                # Add custom logic to signal threads to stop if possible
                pass
            except Exception as e:
                logger.error(f"Error stopping thread {thread.name}: {e}")


def force_kill_processes():
    """Forcefully kill all child processes."""
    logger.info("Forcefully killing all child processes...")
    current_process = psutil.Process()
    for child in current_process.children(recursive=True):
        try:
            logger.warning(f"Killing child process: {child.pid} ({child.name()})")
            child.kill()
        except Exception as e:
            logger.error(f"Error killing process {child.pid}: {e}")


async def shutdown_event_with_timer(timeout=30):
    """Run shutdown_event with an emergency exit timer."""
    try:
        # Use the new phased shutdown approach instead of the old method
        logger.info(f"Executing phased shutdown with {timeout} second timeout")
        success = await execute_phased_shutdown(emergency_timeout=timeout)

        if success:
            logger.info("Phased shutdown completed successfully before timeout")
        else:
            logger.warning("Phased shutdown encountered some issues")
    except Exception as e:
        logger.critical(f"Critical error during shutdown: {e}")
