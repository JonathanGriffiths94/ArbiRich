import logging
import os
from pathlib import Path

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from arbirich.api.router import main_router
from src.arbirich.core.app_lifecyle import lifespan
from src.arbirich.utils.banner import display_banner
from src.arbirich.web.controllers.dashboard_controller import router as dashboard_controller_router
from src.arbirich.web.controllers.exchange_controller import router as exchange_router
from src.arbirich.web.controllers.monitor_controller import router as monitor_router
from src.arbirich.web.controllers.setup_controller import router as setup_router
from src.arbirich.web.endpoints import router as websocket_router
from src.arbirich.web.frontend import router as frontend_router
from src.arbirich.web.routes.dashboard_routes import router as dashboard_router
from src.arbirich.web.routes.strategy_routes import router as strategy_router

logger = logging.getLogger(__name__)


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

    api_app.include_router(main_router)

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

    # Mount static files - Improved mounting with more explicit path resolution
    base_dir = Path(__file__).resolve().parent
    static_dir = base_dir / "web" / "static"
    logger.info(f"Static files directory: {static_dir}")

    # Ensure the directory exists
    if not static_dir.exists():
        logger.error(f"Static directory does not exist: {static_dir}")
        # Try to create it if it doesn't exist
        try:
            static_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created static directory: {static_dir}")
        except Exception as e:
            logger.error(f"Could not create static directory: {e}")

    # Check specific files (for debugging)
    logger.info(f"CSS file exists? {(static_dir / 'css' / 'dashboard.css').exists()}")
    logger.info(f"JS file exists? {(static_dir / 'js' / 'dashboard.js').exists()}")

    # Change to absolute path and log full URL for reference
    abs_static_path = str(static_dir.absolute())
    logger.info(f"Mounting static files from absolute path: {abs_static_path}")
    app.mount("/static", StaticFiles(directory=abs_static_path), name="static")

    # Configure templates if needed
    templates_dir = base_dir / "web" / "templates"
    logger.info(f"Templates directory: {templates_dir}")
    if templates_dir.is_dir():
        templates = Jinja2Templates(directory=str(templates_dir))
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
