import logging

from fastapi import FastAPI

from arbirich.routes import trade
from src.arbirich.core.events import lifespan
from src.arbirich.routes import status

logger = logging.getLogger(__name__)
API_VERSION: str = "v1"


def make_app() -> FastAPI:
    app = FastAPI(title="ArbiRich", version="0.0.1", lifespan=lifespan)

    app.include_router(trade.router, prefix="/trades", tags=["Trades"])
    app.include_router(status.router, prefix="/status", tags=["System Status"])
    return app
