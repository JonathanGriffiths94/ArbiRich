import logging
import os
import signal
import subprocess
import threading
import time

from src.arbirich.core.flow_manager import FlowManager
from src.arbirich.services.channel_maintenance import get_channel_maintenance_service

logger = logging.getLogger(__name__)

flow_manager = FlowManager()
# Flag to track if shutdown is in progress
_shutdown_in_progress = False


# Start a timeout timer for shutdown
def _emergency_exit_timer(timeout=15):
    """
    Start a watchdog timer that will force exit if shutdown takes too long.
    This runs in a separate thread.
    """
    logger.info(f"Starting emergency exit timer ({timeout} seconds)")
    time.sleep(timeout)
    logger.critical(f"Emergency exit timer triggered after {timeout} seconds")
    # Use os._exit(1) for immediate termination
    os._exit(1)


async def startup_event():
    """Event handler for application startup"""
    logger.info("Application startup event triggered")
    try:
        # Start the channel maintenance service
        channel_service = get_channel_maintenance_service()
        channel_service.start()

        # Use the flow manager directly
        await flow_manager.startup()
        logger.info("Flows started via FlowManager.")
    except Exception as e:
        logger.error(f"Error during startup: {e}")
        # Let the error propagate to show in logs, but don't crash the app
        logger.error("Startup completed with errors")


async def shutdown_event():
    """Event handler for application shutdown"""
    global _shutdown_in_progress

    # Prevent multiple shutdown attempts
    if _shutdown_in_progress:
        logger.info("Shutdown already in progress, ignoring repeated call")
        return

    _shutdown_in_progress = True
    logger.info("Application shutdown event triggered")

    # Start emergency timeout timer in a separate thread
    timer_thread = threading.Thread(
        target=_emergency_exit_timer,
        args=(15,),  # 15 second timeout
        daemon=True,
    )
    timer_thread.start()

    try:
        # Stop the channel maintenance service
        channel_service = get_channel_maintenance_service()
        channel_service.stop()
        logger.info("Channel maintenance service stopped")

        # Shut down flow manager with a timeout
        try:
            import asyncio

            await asyncio.wait_for(flow_manager.shutdown(), timeout=10.0)
            logger.info("Flows stopped via FlowManager")
        except asyncio.TimeoutError:
            logger.error("Flow manager shutdown timed out")
        except Exception as e:
            logger.error(f"Error shutting down flow manager: {e}")

        # Force kill any bytewax processes
        try:
            killed = _kill_bytewax_processes()
            if killed:
                logger.info(f"Killed {killed} bytewax processes")
        except Exception as e:
            logger.error(f"Error killing bytewax processes: {e}")

        logger.info("Shutdown sequence completed")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")
    finally:
        # Force exit to ensure the application terminates
        logger.info("Forcing application exit")
        # Short delay to allow logs to be written
        time.sleep(0.5)
        os._exit(0)


def _kill_bytewax_processes():
    """Kill any bytewax processes and return the count of killed processes"""
    killed_count = 0
    try:
        # Find all Python processes running bytewax
        result = subprocess.run(["ps", "aux"], capture_output=True, text=True, check=False)

        for line in result.stdout.splitlines():
            if "python" in line and "bytewax" in line:
                parts = line.split()
                if len(parts) > 1:
                    try:
                        pid = int(parts[1])
                        # Skip processes that can't be this program
                        if pid != os.getpid() and pid != os.getppid():
                            logger.info(f"Killing bytewax process: {pid}")
                            os.kill(pid, signal.SIGKILL)
                            killed_count += 1
                    except (ValueError, ProcessLookupError):
                        pass  # Invalid PID or process already gone
    except Exception as e:
        logger.error(f"Error in kill_bytewax_processes: {e}")

    return killed_count
