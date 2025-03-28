import asyncio
import logging
import os
from contextlib import asynccontextmanager

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from arbirich.api.router import main_router
from src.arbirich.core.events import shutdown_event, startup_event
from src.arbirich.core.trading_service import TradingService
from src.arbirich.services.database.database_service import DatabaseService
from src.arbirich.utils.banner import display_banner
from src.arbirich.web.controllers.dashboard_controller import router as dashboard_controller_router
from src.arbirich.web.controllers.exchange_controller import router as exchange_router
from src.arbirich.web.controllers.monitor_controller import router as monitor_router
from src.arbirich.web.controllers.setup_controller import router as setup_router
from src.arbirich.web.endpoints import router as websocket_router
from src.arbirich.web.frontend import router as frontend_router
from src.arbirich.web.routes.dashboard_routes import router as dashboard_router
from src.arbirich.web.routes.strategy_routes import router as strategy_router
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
            from src.arbirich.core.events import run_database_prefill

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
            from src.arbirich.core.system_state import mark_system_shutdown

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


def make_app() -> FastAPI:
    """Create and configure the FastAPI application"""
    # Load environment variables
    load_dotenv()

    # Display the banner at startup if available
    try:
        display_banner(
            f"Environment: {os.getenv('ENV', 'development').upper()}",
            log_only=False,
            console_only=True,
        )
    except Exception as e:
        logger.warning(f"Banner display not available: {e}")

    app = FastAPI(
        title="ArbiRich API",
        description="Cryptocurrency Arbitrage Platform API",
        version="0.1.0",
        lifespan=lifespan,
        docs_url=None,  # Disable default docs
        redoc_url=None,  # Disable default redoc
    )

    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # In production, restrict this to specific origins
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Create API router with its own docs
    api_app = FastAPI(
        title="ArbiRich API",
        description="Cryptocurrency Arbitrage Platform API",
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # Include unified API router
    api_app.include_router(main_router)

    # Mount the API router at /api
    app.mount("/api", api_app)

    # Include WebSocket routes
    app.include_router(websocket_router)

    # Include frontend routes
    app.include_router(frontend_router)

    # Include controller routes (from original paste.txt)
    app.include_router(dashboard_controller_router)
    app.include_router(strategy_router)
    app.include_router(monitor_router)
    app.include_router(setup_router)
    app.include_router(exchange_router)

    # Include dashboard routes (from app.py)
    app.include_router(dashboard_router)

    # Custom OpenAPI documentation endpoints for the main app
    @app.get("/docs", include_in_schema=False)
    async def custom_swagger_ui_html():
        return get_swagger_ui_html(
            openapi_url="/openapi.json",
            title=app.title + " - Swagger UI",
            oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
        )

    @app.get("/redoc", include_in_schema=False)
    async def redoc_html():
        return get_redoc_html(
            openapi_url="/openapi.json",
            title=app.title + " - ReDoc",
        )

    # Mount static files
    static_dir = os.path.join(os.path.dirname(__file__), "./web/static")
    if os.path.isdir(static_dir):
        app.mount("/static", StaticFiles(directory=static_dir), name="static")
    else:
        # Create the directory if it doesn't exist
        os.makedirs(static_dir, exist_ok=True)
        os.makedirs(os.path.join(static_dir, "css"), exist_ok=True)
        os.makedirs(os.path.join(static_dir, "js"), exist_ok=True)
        app.mount("/static", StaticFiles(directory=static_dir), name="static")

    # Configure templates if needed
    templates_dir = os.path.join(os.path.dirname(__file__), "./web/templates")
    if os.path.isdir(templates_dir):
        templates = Jinja2Templates(directory=templates_dir)
        app.state.templates = templates

    return app


def run_app():
    """Start the application server"""
    app = make_app()

    # Start the server
    host = os.getenv("WEB_HOST", "0.0.0.0")
    port = int(os.getenv("WEB_PORT", 8080))

    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
        ],
    )

    run_app()
