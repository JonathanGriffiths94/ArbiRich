import logging

from fastapi import FastAPI

from src.arbirich.routers import health

logger = logging.getLogger(__name__)

API_VERSION: str = "v1"


def make_app():
    app = FastAPI(title="ArbiRich", version="0.0.1")

    logger.info("Starting the application...")

    app.include_router(health.router, prefix="/health")

    return app
