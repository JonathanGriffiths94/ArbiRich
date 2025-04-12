import asyncio
import concurrent.futures
import logging
import os
import signal
import subprocess
import threading
import time
from enum import Enum

import psutil

from src.arbirich.core.state.system_state import mark_system_shutdown

logger = logging.getLogger(__name__)

# Global state tracking
_shutdown_in_progress = False
_shutdown_lock = threading.RLock()
_phase_complete = {}
_phase_executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

# Add these global variables to track the exit timer thread
_exit_timer_thread = None
_exit_timer_cancel = threading.Event()
_exit_timer_lock = threading.Lock()


class ShutdownPhase(Enum):
    """Enum for shutdown phases to ensure proper ordering."""

    FLAG_SET = 0
    CONSUMER_STOP = 1
    PROCESSOR_STOP = 2
    BYTEWAX_STOP = 3
    REDIS_CLEANUP = 4
    DATABASE_CLEANUP = 5
    THREAD_TERMINATION = 6
    PROCESS_TERMINATION = 7
    COMPLETED = 8


def mark_phase_complete(phase: ShutdownPhase):
    """Mark a shutdown phase as complete."""
    with _shutdown_lock:
        _phase_complete[phase] = True
        logger.info(f"Shutdown phase {phase.name} completed")


def is_phase_complete(phase: ShutdownPhase) -> bool:
    """Check if a shutdown phase is complete."""
    with _shutdown_lock:
        return _phase_complete.get(phase, False)


def reset_shutdown_state():
    """Reset the shutdown state for testing or system restart."""
    global _shutdown_in_progress
    with _shutdown_lock:
        _shutdown_in_progress = False
        _phase_complete.clear()


def kill_bytewax_processes_aggressive() -> int:
    """
    Kill any Bytewax processes aggressively.

    This function uses multiple approaches to ensure processes are terminated.
    Returns the number of processes killed.
    """
    killed_count = 0
    bytewax_pids = []

    # Identify all Python processes that might be running Bytewax
    try:
        # First use ps for broader detection - works on Unix systems
        try:
            logger.info("Looking for Bytewax processes using ps command...")
            result = subprocess.run(
                ["ps", "-ef"],
                capture_output=True,
                text=True,
                check=False,
                timeout=5,  # Add a timeout to prevent hanging
            )

            for line in result.stdout.splitlines():
                if "python" in line.lower() and "bytewax" in line.lower():
                    parts = line.split()
                    if len(parts) > 1:
                        try:
                            pid = int(parts[1])
                            if pid != os.getpid() and pid != os.getppid():
                                bytewax_pids.append(pid)
                                logger.debug(f"Found Bytewax PID via ps: {pid}")
                        except (ValueError, IndexError):
                            pass
        except (subprocess.SubprocessError, subprocess.TimeoutExpired) as e:
            logger.warning(f"Error running ps command: {e}")

        # Then use psutil for a more programmatic approach
        logger.info("Looking for Bytewax processes using psutil...")
        current_process = psutil.Process()

        # Get all processes
        for proc in psutil.process_iter(["pid", "name", "cmdline"]):
            try:
                # Skip our own process
                if proc.pid == os.getpid() or proc.pid == os.getppid():
                    continue

                # Check if this is a Python process
                if proc.name().lower() in ("python", "python3", "python.exe"):
                    cmdline = proc.cmdline()
                    cmdline_str = " ".join(cmdline).lower()

                    # Check if it's running bytewax
                    if "bytewax" in cmdline_str or any("bytewax" in arg.lower() for arg in cmdline):
                        if proc.pid not in bytewax_pids:
                            bytewax_pids.append(proc.pid)
                            logger.debug(f"Found Bytewax PID via psutil: {proc.pid}, cmdline: {cmdline}")
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue

        logger.info(f"Found {len(bytewax_pids)} Bytewax processes to terminate")

        # Terminate processes using multiple approaches
        for pid in bytewax_pids:
            try:
                logger.info(f"Attempting to terminate Bytewax process {pid}")

                # First try a graceful SIGTERM
                os.kill(pid, signal.SIGTERM)

                # Wait a brief moment for graceful shutdown
                time.sleep(0.5)

                # Check if still running and use SIGKILL if needed
                try:
                    os.kill(pid, 0)  # This raises OSError if process doesn't exist
                    logger.warning(f"Process {pid} still running after SIGTERM, using SIGKILL")
                    os.kill(pid, signal.SIGKILL)
                except OSError:
                    logger.info(f"Process {pid} terminated successfully with SIGTERM")

                # Also try psutil for a more robust approach
                try:
                    proc = psutil.Process(pid)
                    if proc.is_running():
                        proc.kill()  # Force kill
                        logger.info(f"Process {pid} killed via psutil")
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass  # Process already gone or can't access

                killed_count += 1

            except Exception as e:
                logger.error(f"Error killing process {pid}: {e}")

    except Exception as e:
        logger.error(f"Error in kill_bytewax_processes_aggressive: {e}")

    return killed_count


def _short_circuit_exit(timeout=30, exit_code=1):
    """
    Force process exit after timeout seconds.
    This is the nuclear option when all else fails.

    Returns:
        A function that can be called to cancel the exit timer
    """
    global _exit_timer_thread, _exit_timer_cancel

    with _exit_timer_lock:
        # Reset the cancel event in case it was set before
        _exit_timer_cancel.clear()

        # Log that we're scheduling an emergency exit
        logger.warning(f"Short-circuit exit scheduled in {timeout} seconds")

        def _exit_thread():
            """Thread function that handles the delayed exit with cancellation support"""
            remaining = timeout
            check_interval = 0.5  # Check for cancellation every half second

            while remaining > 0:
                if _exit_timer_cancel.is_set():
                    logger.info("Emergency exit timer cancelled successfully")
                    return

                time.sleep(check_interval)
                remaining -= check_interval

            # If we get here without cancellation, force exit
            logger.critical(f"Forcing exit with code {exit_code}")
            os._exit(exit_code)

        # Create and start the timer thread
        _exit_timer_thread = threading.Thread(target=_exit_thread, name="emergency_exit_timer", daemon=True)
        _exit_timer_thread.start()

        def cancel_exit_timer():
            """Function to cancel the exit timer"""
            with _exit_timer_lock:
                if _exit_timer_thread and _exit_timer_thread.is_alive():
                    _exit_timer_cancel.set()
                    logger.info("Emergency exit timer cancellation requested")
                    return True
                return False

        return cancel_exit_timer


async def execute_phased_shutdown(emergency_timeout=30):
    """
    Execute shutdown in phases with timeouts to prevent hanging.

    Args:
        emergency_timeout: Total time allowed before forced exit
    """
    global _shutdown_in_progress

    with _shutdown_lock:
        if _shutdown_in_progress:
            logger.info("Phased shutdown already in progress")
            return
        _shutdown_in_progress = True

    logger.info("Starting phased shutdown sequence")

    # Start emergency exit timer and get cancellation function
    cancel_exit_timer = _short_circuit_exit(timeout=emergency_timeout)

    try:
        # Phase 1: Set all shutdown flags
        logger.info("PHASE 1: Setting shutdown flags")
        mark_system_shutdown(True)
        try:
            # Also set processor-specific shutdown flag
            from src.arbirich.services.exchange_processors.registry import set_processors_shutting_down

            set_processors_shutting_down(True)
        except Exception as e:
            logger.error(f"Error setting processor shutdown flag: {e}")
        mark_phase_complete(ShutdownPhase.FLAG_SET)

        # NEW PHASE: First Stop Reporting Flow - this is critical to prevent hanging
        logger.info("PHASE 1.5: Stopping reporting flow early")
        try:
            from src.arbirich.core.trading.flows.flow_manager import BytewaxFlowManager
            from src.arbirich.core.trading.flows.reporting.reporting_flow import stop_reporting_flow

            # Try to get the reporting flow manager
            reporting_manager = BytewaxFlowManager.get_manager("reporting")
            if reporting_manager and reporting_manager.is_running():
                logger.info("Found active reporting flow manager, stopping")
                await asyncio.wait_for(asyncio.shield(asyncio.to_thread(reporting_manager.stop_flow)), timeout=5)
            else:
                # Direct call to stop_reporting_flow as fallback
                await asyncio.wait_for(asyncio.shield(asyncio.to_thread(stop_reporting_flow)), timeout=5)

            logger.info("Reporting flow stopped early in shutdown sequence")
        except (asyncio.TimeoutError, Exception) as e:
            logger.warning(f"Reporting flow early stop timed out or failed: {e}")

        # Give a moment for cleanup
        await asyncio.sleep(1.0)

        # Phase 2: Stop WebSocket consumers
        logger.info("PHASE 2: Stopping WebSocket consumers")
        try:
            from src.arbirich.core.trading.flows.bytewax_flows.ingestion.ingestion_source import (
                disable_processor_startup,
                stop_all_consumers,
            )

            disable_processor_startup()
            await asyncio.wait_for(asyncio.shield(asyncio.to_thread(stop_all_consumers)), timeout=3)
        except (asyncio.TimeoutError, Exception) as e:
            logger.warning(f"Consumer stop timed out or failed: {e}")
        mark_phase_complete(ShutdownPhase.CONSUMER_STOP)

        # Phase 3: Stop processors
        logger.info("PHASE 3: Stopping processors")
        try:
            from src.arbirich.services.exchange_processors.registry import shutdown_all_processors

            await asyncio.wait_for(asyncio.shield(asyncio.to_thread(shutdown_all_processors)), timeout=3)
        except (asyncio.TimeoutError, Exception) as e:
            logger.warning(f"Processor shutdown timed out or failed: {e}")
        mark_phase_complete(ShutdownPhase.PROCESSOR_STOP)

        # Phase 4: Kill Bytewax processes
        logger.info("PHASE 4: Killing Bytewax processes")
        killed = await asyncio.wait_for(asyncio.shield(asyncio.to_thread(kill_bytewax_processes_aggressive)), timeout=5)
        logger.info(f"Killed {killed} Bytewax processes")
        mark_phase_complete(ShutdownPhase.BYTEWAX_STOP)

        # Phase 5: Clean up Redis
        logger.info("PHASE 5: Cleaning up Redis connections")
        try:
            from src.arbirich.core.trading.flows.bytewax_flows.detection.detection_source import (
                reset_shared_redis_client as reset_arbitrage_redis,
            )
            from src.arbirich.core.trading.flows.bytewax_flows.execution.execution_source import (
                reset_shared_redis_client as reset_execution_redis,
            )
            from src.arbirich.core.trading.flows.bytewax_flows.ingestion.ingestion_sink import (
                reset_shared_redis_client as reset_ingestion_redis,
            )
            from src.arbirich.core.trading.flows.reporting import reset_shared_redis_client as reset_reporting_redis
            from src.arbirich.services.redis.redis_service import reset_redis_pool

            await asyncio.wait_for(
                asyncio.shield(
                    asyncio.gather(
                        asyncio.to_thread(reset_arbitrage_redis),
                        asyncio.to_thread(reset_execution_redis),
                        asyncio.to_thread(reset_ingestion_redis),
                        asyncio.to_thread(reset_reporting_redis),
                        asyncio.to_thread(reset_redis_pool),
                    )
                ),
                timeout=5,
            )
        except (asyncio.TimeoutError, Exception) as e:
            logger.warning(f"Redis cleanup timed out or failed: {e}")
        mark_phase_complete(ShutdownPhase.REDIS_CLEANUP)

        # Phase 6: Clean up database connections
        logger.info("PHASE 6: Cleaning up database connections")
        try:
            from src.arbirich.services.database.database_service import cleanup_db_connections

            await asyncio.wait_for(asyncio.shield(asyncio.to_thread(cleanup_db_connections)), timeout=3)
        except (asyncio.TimeoutError, Exception) as e:
            logger.warning(f"Database cleanup timed out or failed: {e}")
        mark_phase_complete(ShutdownPhase.DATABASE_CLEANUP)

        # Phase 7: Force terminate problematic threads
        logger.info("PHASE 7: Terminating problematic threads")
        non_daemon_threads = [
            thread for thread in threading.enumerate() if not thread.daemon and thread is not threading.current_thread()
        ]

        logger.info(f"Found {len(non_daemon_threads)} non-daemon threads that may block shutdown")
        # Log them but don't try to force kill (Python doesn't support thread termination)
        for thread in non_daemon_threads:
            logger.warning(f"Non-daemon thread may block shutdown: {thread.name}")
        mark_phase_complete(ShutdownPhase.THREAD_TERMINATION)

        # Phase 8: Kill child processes
        logger.info("PHASE 8: Killing child processes")
        killed_count = 0
        try:
            current_process = psutil.Process()
            children = current_process.children(recursive=True)
            logger.info(f"Found {len(children)} child processes to terminate")

            for child in children:
                try:
                    logger.warning(f"Killing child process: {child.pid} ({child.name()})")
                    child.kill()
                    killed_count += 1
                except Exception as e:
                    logger.error(f"Error killing process {child.pid}: {e}")
        except Exception as e:
            logger.error(f"Error terminating child processes: {e}")

        logger.info(f"Killed {killed_count} child processes")
        mark_phase_complete(ShutdownPhase.PROCESS_TERMINATION)

        # Final phase - cleanup complete
        logger.info("Phased shutdown completed successfully")

        # NEW: Reset trading service component states
        try:
            from arbirich.core.trading.trading_service import get_trading_service

            trading_service = get_trading_service()
            if hasattr(trading_service, "_reset_all_component_states"):
                trading_service._reset_all_component_states()
                logger.info("Trading service component states reset")
        except Exception as e:
            logger.error(f"Error resetting component states: {e}")

        mark_phase_complete(ShutdownPhase.COMPLETED)

        # IMPORTANT: Cancel the emergency exit timer since shutdown succeeded
        cancel_exit_timer()

        return True

    except Exception as e:
        logger.critical(f"Error during phased shutdown: {e}")
        return False
