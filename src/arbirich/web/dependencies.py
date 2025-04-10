import logging

from fastapi import Depends

from arbirich.services.metrics.strategy_metrics_service import StrategyMetricsService
from src.arbirich.services.database.database_service import DatabaseService

logger = logging.getLogger(__name__)


def get_db_service():
    """Get a database service instance."""
    db = DatabaseService()
    try:
        yield db
    finally:
        try:
            db.close()
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")


def get_metrics_service(db: DatabaseService = Depends(get_db_service)):
    """Get a metrics service instance."""
    return StrategyMetricsService(db)
