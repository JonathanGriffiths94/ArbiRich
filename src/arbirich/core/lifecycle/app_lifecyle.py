import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from arbirich.core.lifecycle.events import shutdown_event, startup_event
from arbirich.core.trading.trading_service import TradingService
from src.arbirich.services.database.database_service import DatabaseService
from src.arbirich.web.websockets import websocket_broadcast_task

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI"""
    try:
        logger.info("Starting application services...")

        # Initialize database connection
        try:
            db = DatabaseService()
            app.state.db = db
            logger.info("Database service initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize database service: {e}", exc_info=True)
            raise

        # Run database prefill first to ensure configuration data exists
        try:
            from arbirich.core.lifecycle.events import run_database_prefill

            await run_database_prefill()
            logger.info("Database prefill completed")
        except Exception as e:
            logger.error(f"Error during database prefill: {e}", exc_info=True)
            # Continue despite prefill errors

        # Initialize trading service
        try:
            trading_service = TradingService()
            app.state.trading_service = trading_service
            logger.info("Initializing trading service...")
            await trading_service.initialize(db)
            logger.info("Trading service initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing trading service: {e}", exc_info=True)
            # Continue despite initialization errors

        # Start WebSocket broadcast task for real-time updates
        try:
            logger.info("Starting WebSocket broadcast task...")
            broadcast_task = asyncio.create_task(websocket_broadcast_task(), name="websocket_broadcast")
            app.state.broadcast_task = broadcast_task
            logger.info("WebSocket broadcast task started")
        except Exception as e:
            logger.error(f"Error starting WebSocket broadcast task: {e}", exc_info=True)
            # Continue despite WebSocket errors

        # Run regular startup tasks
        try:
            logger.info("Running application startup event...")
            await startup_event()
            logger.info("Application startup event completed")
        except Exception as e:
            logger.error(f"Error during startup event: {e}", exc_info=True)
            # Continue despite startup errors

        # Display welcome banner right before yielding control
        try:
            from src.arbirich.utils.banner import display_banner

            welcome_message = "ArbiRich server is ready! API endpoints available at /api\nAccess the web interface at http://localhost:8080"
            display_banner(
                extra_info=welcome_message, console_only=True
            )  # Use console_only to prevent duplicate logging
        except Exception as e:
            logger.error(f"Error displaying welcome banner: {e}", exc_info=True)

        logger.info("Application startup complete - yielding control to FastAPI")
        yield
        logger.info("FastAPI shutdown initiated")
    except Exception as e:
        logger.critical(f"Fatal error during application startup: {e}", exc_info=True)
    finally:
        logger.info("Shutting down application services...")

        # Cancel WebSocket broadcast task
        if hasattr(app.state, "broadcast_task") and app.state.broadcast_task:
            logger.info("Cancelling WebSocket broadcast task...")
            app.state.broadcast_task.cancel()
            try:
                await asyncio.wait_for(app.state.broadcast_task, timeout=5.0)
            except asyncio.CancelledError:
                logger.info("WebSocket broadcast task cancelled successfully")
            except asyncio.TimeoutError:
                logger.warning("WebSocket broadcast task did not cancel within timeout")
            except Exception as e:
                logger.error(f"Error cancelling WebSocket broadcast task: {e}")

        # Set system-wide shutdown flag immediately (moved from shutdown_event)
        try:
            from arbirich.core.state.system_state import mark_system_shutdown

            mark_system_shutdown(True)
            logger.info("System-wide shutdown flag set")
        except Exception as e:
            logger.error(f"Error setting system shutdown flag: {e}")

        # Disable processor startup to prevent new processors during shutdown
        try:
            from src.arbirich.flows.ingestion.ingestion_source import disable_processor_startup

            disable_processor_startup()
            logger.info("Processor startup disabled")
        except Exception as e:
            logger.error(f"Error disabling processor startup: {e}")

        # Shutdown exchange processors before trading service
        try:
            from src.arbirich.services.exchange_processors.registry import shutdown_all_processors

            shutdown_all_processors()
            logger.info("Exchange processors shutdown initiated")
            # Brief pause to allow processors to notice shutdown flag
            await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"Error stopping exchange processors: {e}")

        # Now shutdown trading service with timeout
        if hasattr(app.state, "trading_service") and app.state.trading_service:
            logger.info("Stopping trading service with timeout")
            try:
                await asyncio.wait_for(app.state.trading_service.stop_all(), timeout=5.0)
                logger.info("Trading service stopped successfully")
            except asyncio.TimeoutError:
                logger.error("Trading service shutdown timed out")
            except Exception as e:
                logger.error(f"Error shutting down trading service: {e}")

        # Now run the main shutdown event for remaining cleanup
        try:
            logger.info("Running application shutdown event...")
            # Use a timeout to prevent hanging during shutdown
            await asyncio.wait_for(shutdown_event(), timeout=10.0)
            logger.info("Application shutdown event completed")
        except asyncio.TimeoutError:
            logger.error("Shutdown event timed out after 10 seconds")
        except Exception as e:
            logger.error(f"Error during shutdown event: {e}")

        # Close database connection as the last step
        if hasattr(app.state, "db") and app.state.db:
            try:
                logger.info("Closing database connection")
                app.state.db.__exit__(None, None, None)
                logger.info("Database connection closed")
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")

        logger.info("Application shutdown complete")

        # Display shutdown banner after all shutdown processes are complete
        try:
            from src.arbirich.utils.banner import display_banner

            shutdown_message = "ArbiRich server has been successfully stopped.\nThank you for using ArbiRich!"
            display_banner(extra_info=shutdown_message, console_only=True, separator_style="equals")
        except Exception as e:
            logger.error(f"Error displaying shutdown banner: {e}")
