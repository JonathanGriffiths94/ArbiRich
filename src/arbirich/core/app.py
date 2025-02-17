import logging

from fastapi import FastAPI

from src.arbirich.api import health
from src.arbirich.core.events import lifespan

logger = logging.getLogger(__name__)
API_VERSION: str = "v1"


def make_app() -> FastAPI:
    app = FastAPI(title="ArbiRich", version="0.0.1", lifespan=lifespan)

    app.include_router(health.router, prefix="/health")
    return app
