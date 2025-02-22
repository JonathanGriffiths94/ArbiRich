import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from arbirich.core.events import shutdown_event, startup_event
from arbirich.routes import status, trade

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # STARTUP: Initialize all services (e.g., Bytewax flows, scanners, executors)
    await startup_event()
    try:
        yield
    finally:
        # SHUTDOWN: Clean up background tasks and external connections
        await shutdown_event()


def make_app() -> FastAPI:
    app = FastAPI(title="ArbiRich", lifespan=lifespan)

    app.include_router(trade.router, prefix="/trade", tags=["Trade"])
    app.include_router(status.router, prefix="/status", tags=["System Status"])
    return app
