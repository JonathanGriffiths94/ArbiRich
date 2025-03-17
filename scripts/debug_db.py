"""
Debug script for database connection and operations
"""

import logging

from sqlalchemy import text

from arbirich.services.database.database_service import DatabaseService, engine
from src.arbirich.config import DATABASE_URL

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def test_connection():
    """Test raw database connection"""
    logger.info("Testing raw database connection")
    try:
        # Test direct connection
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            logger.info(f"Connection successful. Result: {result.fetchone()}")

            # Test simple query
            result = connection.execute(text("SELECT current_database()"))
            logger.info(f"Current database: {result.fetchone()[0]}")

            # Test schema query
            result = connection.execute(
                text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            )
            tables = [row[0] for row in result]
            logger.info(f"Tables in database: {tables}")

            # If exchanges table exists, query its contents
            if "exchanges" in tables:
                result = connection.execute(text("SELECT * FROM exchanges"))
                rows = [row for row in result]
                logger.info(f"Exchanges in database: {rows}")
            else:
                logger.warning("Exchanges table does not exist!")

    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}", exc_info=True)


def test_service_operations():
    """Test operations using the DatabaseService"""
    logger.info("Testing DatabaseService operations")
    try:
        with DatabaseService() as db:
            # Try getting exchanges
            exchanges = db.get_all_exchanges()
            logger.info(f"Found {len(exchanges)} exchanges: {[e.name for e in exchanges]}")

            # Try getting pairs
            pairs = db.get_all_pairs()
            logger.info(f"Found {len(pairs)} pairs: {[p.symbol for p in pairs]}")

            # Try getting strategies
            strategies = db.get_all_strategies()
            logger.info(f"Found {len(strategies)} strategies: {[s.name for s in strategies]}")
    except Exception as e:
        logger.error(f"Service operations test failed: {str(e)}", exc_info=True)


if __name__ == "__main__":
    logger.info(f"Using DATABASE_URL: {DATABASE_URL}")
    logger.info("Starting database debug script")
    test_connection()
    test_service_operations()
    logger.info("Database debug script completed")
