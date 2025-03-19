import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from src.arbirich.core.events import shutdown_event, startup_event

# Fix the import path for API router
from src.arbirich.routers.status import router
from src.arbirich.web.endpoints import router as websocket_router
from src.arbirich.web.frontend import router as frontend_router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI"""
    try:
        logger.info("Starting application services...")
        await startup_event()
        yield
    finally:
        logger.info("Shutting down application services...")
        await shutdown_event()


def make_app() -> FastAPI:
    """Create and configure the FastAPI application"""
    app = FastAPI(
        title="ArbiRich API", description="Cryptocurrency Arbitrage Platform API", version="0.1.0", lifespan=lifespan
    )

    # Include API routes
    app.include_router(router, prefix="/api/v1")

    # Include WebSocket routes
    app.include_router(websocket_router)

    # Include frontend routes
    app.include_router(frontend_router)

    # Mount static files
    static_dir = os.path.join(os.path.dirname(__file__), "../web/static")
    if os.path.isdir(static_dir):
        app.mount("/static", StaticFiles(directory=static_dir), name="static")
    else:
        # Create the directory if it doesn't exist
        os.makedirs(static_dir, exist_ok=True)
        os.makedirs(os.path.join(static_dir, "css"), exist_ok=True)
        os.makedirs(os.path.join(static_dir, "js"), exist_ok=True)
        app.mount("/static", StaticFiles(directory=static_dir), name="static")

    return app
