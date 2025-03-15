import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.arbirich.core.events import shutdown_event, startup_event
from src.arbirich.routers.status import router

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
    app = FastAPI(lifespan=lifespan)
    app.include_router(router)
    return app
