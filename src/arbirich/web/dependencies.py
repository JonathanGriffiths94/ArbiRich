import logging
from typing import Generator

from src.arbirich.services.database.database_service import DatabaseService
from src.arbirich.services.metrics.strategy_metrics_service import StrategyMetricsService

logger = logging.getLogger(__name__)


def get_db_service() -> Generator[DatabaseService, None, None]:
    """
    Get a database service instance.

    This function returns a generator that yields a DatabaseService instance.
    When using as a dependency, FastAPI will handle the generator correctly.
    If using the function directly, remember to use next() to get the actual service.

    Example:
        # As a dependency in FastAPI
        @app.get("/route")
        async def route(db: DatabaseService = Depends(get_db_service)):
            # db is already the actual service instance

        # When calling manually
        db_gen = get_db_service()
        db = next(db_gen)  # Get the actual service

    Returns:
        Generator yielding a DatabaseService instance
    """
    db_service = DatabaseService()
    try:
        yield db_service
    finally:
        try:
            db_service.close()
        except Exception as e:
            logger.error(f"Error closing database service: {e}")


def get_db() -> DatabaseService:
    """
    Get a direct DatabaseService instance without using a generator.
    This is simpler to use but doesn't have the cleanup benefits of the generator version.

    Returns:
        DatabaseService instance
    """
    return DatabaseService()


def get_metrics_service() -> Generator[StrategyMetricsService, None, None]:
    """
    Get a metrics service instance.

    Returns:
        Generator yielding a StrategyMetricsService instance
    """
    db_service = DatabaseService()
    metrics_service = StrategyMetricsService(db_service)

    try:
        yield metrics_service
    finally:
        try:
            db_service.close()
        except Exception as e:
            logger.error(f"Error closing database service: {e}")
