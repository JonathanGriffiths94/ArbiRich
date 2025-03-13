import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.arbirich.core.events import shutdown_event, startup_event
from src.arbirich.routers import status

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting application services...")
    await startup_event()
    try:
        yield
    finally:
        logger.info("Shutting down application services...")
        await shutdown_event()


def make_app() -> FastAPI:
    app = FastAPI(title="ArbiRich", lifespan=lifespan)

    app.include_router(status.router, prefix="/status", tags=["System Status"])
    return app
