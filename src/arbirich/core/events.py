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

from arbirich.core.trading_service import TradingService
from arbirich.services.database.prefill_database import prefill_database
from src.arbirich.services.channel_maintenance import get_channel_maintenance_service

logger = logging.getLogger(__name__)

# Flag to track if shutdown is in progress
_shutdown_in_progress = False

# Add a lock to prevent concurrent database prefills
_prefill_lock = threading.Lock()
_prefill_task = None


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
        # Don't wrap the future in create_task since run_in_executor already returns a future
        loop = asyncio.get_running_loop()
        _prefill_task = loop.run_in_executor(None, lambda: prefill_database(smart_activate=True))

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
        import sqlalchemy as sa  # Add explicit import for sqlalchemy

        from src.arbirich.services.database.database_service import DatabaseService

        db = DatabaseService()
        with db.engine.connect() as conn:
            result = conn.execute(sa.text("SELECT 1")).scalar()
            logger.info(f"Database connection test: {result}")
    except Exception as e:
        logger.info(f"Database connection test failed: {e}")

    # Check running threads
    import threading

    logger.info(f"Active thread count: {threading.active_count()}")
    for thread in threading.enumerate():
        logger.info(f"Thread: {thread.name} (daemon: {thread.daemon})")

    logger.info("------ End Diagnostic Information ------")


async def startup_event() -> None:
    """Run tasks at application startup."""
    logger.info("Running application startup tasks...")

    # Diagnose the system status
    diagnose_system_status()

    # Run database prefill first to ensure data is available
    await run_database_prefill()

    # Create startup event record
    start_time = time.time()

    # Initialize and start the trading service
    try:
        trading_service = TradingService()
        # Only initialize if not already initialized
        if not hasattr(trading_service, "_initialized") or not trading_service._initialized:
            logger.info("Initializing trading service...")
            await trading_service.initialize()
            logger.info("Trading service initialized")

        # Check if we need to activate any strategies in the database
        try:
            import sqlalchemy as sa

            from src.arbirich.services.database.database_service import DatabaseService

            # Check for active strategies
            with DatabaseService() as db:
                active_strategies = db.get_active_strategies()
                if not active_strategies:
                    logger.warning("No active strategies found, activating all strategies...")
                    with db.engine.begin() as conn:
                        result = conn.execute(sa.text("UPDATE strategies SET is_active = TRUE"))
                        logger.info(f"Activated {result.rowcount} strategies in database")
                else:
                    logger.info(f"Found {len(active_strategies)} active strategies")

                # Verify high_frequency_arbitrage is active
                hft_strategy = db.get_strategy_by_name("high_frequency_arbitrage")
                if hft_strategy and not hft_strategy.is_active:
                    logger.info("Activating high_frequency_arbitrage strategy...")
                    with db.engine.begin() as conn:
                        result = conn.execute(
                            sa.text("UPDATE strategies SET is_active = TRUE WHERE name = 'high_frequency_arbitrage'")
                        )
                        if result.rowcount > 0:
                            logger.info("Successfully activated high_frequency_arbitrage strategy")
        except Exception as e:
            logger.error(f"Error checking/activating strategies: {e}")

        # Only try to start components if they're not started
        if not any(comp["active"] for comp in trading_service.components.values()):
            logger.info("Starting trading service components...")
            # Pass activate_strategies=True to activate all strategies
            await trading_service.start_all(activate_strategies=True)
            logger.info("Trading service components started")
        else:
            logger.info("Trading service components already running")
    except Exception as e:
        logger.error(f"Error initializing trading service: {e}", exc_info=True)
        # Continue execution to allow app to start with limited functionality

    # Log successful startup
    startup_duration = time.time() - start_time
    logger.info(f"Application startup completed in {startup_duration:.2f} seconds")


async def shutdown_event() -> None:
    """Run tasks at application shutdown."""
    global _shutdown_in_progress

    # Prevent multiple shutdown attempts
    if _shutdown_in_progress:
        logger.info("Shutdown already in progress, ignoring repeated call")
        return

    _shutdown_in_progress = True
    logger.info("Running application shutdown tasks...")

    # Create shutdown event record
    shutdown_time = time.time()

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

        # Shut down trading service with a timeout
        try:
            import asyncio

            await asyncio.wait_for(trading_service.stop_all(), timeout=10.0)
            logger.info("Trading service stopped successfully")
        except asyncio.TimeoutError:
            logger.error("Trading service shutdown timed out")
        except Exception as e:
            logger.error(f"Error shutting down trading service: {e}")

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

    # Log successful shutdown
    shutdown_duration = time.time() - shutdown_time
    logger.info(f"Application shutdown completed in {shutdown_duration:.2f} seconds")


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
