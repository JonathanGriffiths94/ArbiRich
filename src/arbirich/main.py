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

# Import the unified API router
from src.arbirich.api.uni_router import main_router
from src.arbirich.core.events import shutdown_event, startup_event
from src.arbirich.services.database.database_service import DatabaseService
from src.arbirich.services.trading_service import trading_service
from src.arbirich.utils.banner import display_banner

# Import existing web controllers and routes for backward compatibility
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

# Add more detailed debugging to identify startup failures


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

        logger.info("Application startup complete - yielding control to FastAPI")
        yield
        logger.info("FastAPI shutdown initiated")
    except Exception as e:
        logger.critical(f"Fatal error during application startup: {e}", exc_info=True)
        # Allow the shutdown process to continue
    finally:
        logger.info("Shutting down application services...")

        # Cancel WebSocket broadcast task
        if hasattr(app.state, "broadcast_task") and app.state.broadcast_task:
            app.state.broadcast_task.cancel()
            try:
                await app.state.broadcast_task
            except asyncio.CancelledError:
                pass

        # Cancel database prefill task if it's still running
        if hasattr(app.state, "prefill_task") and app.state.prefill_task:
            if not app.state.prefill_task.done():
                app.state.prefill_task.cancel()
                try:
                    await app.state.prefill_task
                except asyncio.CancelledError:
                    pass

        # Shutdown trading service
        await trading_service.stop_all()

        # Close database connection if it has a close method
        if hasattr(app.state, "db"):
            # Use __exit__ method instead of close directly
            try:
                app.state.db.__exit__(None, None, None)
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")

        # Run regular shutdown tasks
        await shutdown_event()


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

    # Include legacy routers for backward compatibility
    # These can be gradually migrated to the unified API
    try:
        # Fix imports to use src prefix instead of arbirich
        from src.arbirich.api.status import router as status_router

        # Use our main_router directly since we already imported it
        api_router = main_router

        # Include with lower precedence so unified routes take priority
        api_app.include_router(status_router)

        # Import dashboard API if available
        try:
            from src.arbirich.api.dashboard_api import router as dashboard_api_router

            api_app.include_router(dashboard_api_router, prefix="/dashboard")
        except ImportError:
            logger.warning("Dashboard API router not available")

    except ImportError as e:
        logger.warning(f"Some legacy API routers not available: {e}")

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
    static_dir = os.path.join(os.path.dirname(__file__), "./src/arbirich/web/static")
    if os.path.isdir(static_dir):
        app.mount("/static", StaticFiles(directory=static_dir), name="static")
    else:
        # Create the directory if it doesn't exist
        os.makedirs(static_dir, exist_ok=True)
        os.makedirs(os.path.join(static_dir, "css"), exist_ok=True)
        os.makedirs(os.path.join(static_dir, "js"), exist_ok=True)
        app.mount("/static", StaticFiles(directory=static_dir), name="static")

    # Configure templates if needed
    templates_dir = os.path.join(os.path.dirname(__file__), "./src/arbirich/web/templates")
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
