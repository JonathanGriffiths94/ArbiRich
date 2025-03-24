"""
Script to recalculate metrics for all strategies.
Run this after adding or updating the metrics tables.
"""

import datetime as dt
import logging
import os
import sys
from datetime import datetime, timedelta

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Try to load environment variables
try:
    from dotenv import load_dotenv

    load_dotenv()
    logger.info("Environment variables loaded from .env file")
except ImportError:
    logger.warning("python-dotenv not installed, using environment variables as is")

try:
    from arbirich.services.metrics.strategy_metrics_service import StrategyMetricsService
    from src.arbirich.services.database.database_service import DatabaseService
except ImportError as e:
    logger.error(f"Failed to import required modules: {e}")
    logger.error("Make sure you're running this script from the project root directory")
    logger.error("Try: poetry run python scripts/recalculate_metrics.py")
    sys.exit(1)


def recalculate_all_metrics():
    """Recalculate metrics for all strategies."""
    try:
        # Create database and metrics services
        db_service = DatabaseService()
        metrics_service = StrategyMetricsService(db_service)

        # Get all strategies
        strategies = db_service.get_all_strategies()

        if not strategies:
            logger.warning("No strategies found in database")
            return

        logger.info(f"Found {len(strategies)} strategies")

        # Calculate time periods with timezone-aware objects
        now = dt.datetime.now(dt.UTC)
        periods = [
            ("Last 24 hours", now - dt.timedelta(days=1), now),
            ("Last 7 days", now - dt.timedelta(days=7), now),
            ("Last 30 days", now - dt.timedelta(days=30), now),
            ("Last 90 days", now - dt.timedelta(days=90), now),
            ("All time", dt.datetime(2000, 1, 1, tzinfo=dt.UTC), now),
        ]

        # For each strategy, calculate metrics for each period
        for strategy in strategies:
            logger.info(f"Processing strategy: {strategy.name} (ID: {strategy.id})")

            # Get a session from the database service
            session = db_service.session

            for period_name, start_date, end_date in periods:
                logger.info(f"  Calculating metrics for {period_name}")

                # Calculate metrics
                metrics = metrics_service.calculate_strategy_metrics(session, strategy.id, start_date, end_date)

                if metrics:
                    logger.info(
                        f"  ✓ Metrics calculated: Win rate {metrics.win_rate:.2f}%, Profit factor {metrics.profit_factor:.2f}"
                    )
                else:
                    logger.warning(f"  ✗ No metrics calculated for {period_name} (no executions found)")

        logger.info("Metrics calculation complete!")

    except Exception as e:
        logger.error(f"Error recalculating metrics: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    recalculate_all_metrics()
