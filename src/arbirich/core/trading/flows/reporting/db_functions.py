# reporting/db_functions.py

import logging
from typing import Any, Optional

from src.arbirich.services.database.database_service import DatabaseService
from src.arbirich.services.database.repositories.trade_opportunity_repository import TradeOpportunityRepository

logger = logging.getLogger(__name__)


def check_opportunity_exists(opportunity_id: str, db=None, retries=3) -> bool:
    """Check if an opportunity exists in the database"""
    if not opportunity_id:
        return False

    close_db = False
    if db is None:
        from src.arbirich.services.database.database_service import DatabaseService

        db = DatabaseService()
        close_db = True

    try:
        from src.arbirich.services.database.repositories.trade_opportunity_repository import TradeOpportunityRepository

        # Remove the session parameter - BaseRepository doesn't accept it
        repo = TradeOpportunityRepository(engine=db.engine)

        # Use the repository to find the opportunity
        opportunity = repo.get_by_id(opportunity_id)
        exists = opportunity is not None

        logger.debug(f"🔍 Opportunity {opportunity_id} exists: {exists}")
        return exists
    except Exception as e:
        logger.error(f"❌ Error checking opportunity existence: {e}", exc_info=True)
        return False
    finally:
        if close_db and db:
            db.close()
            logger.debug("🔌 Database connection closed")


def get_opportunity(db: DatabaseService, opportunity_id: str) -> Optional[Any]:
    """
    Get an opportunity by ID

    Args:
        db: Database service instance
        opportunity_id: UUID string of the opportunity to get

    Returns:
        Optional[TradeOpportunity]: The opportunity if found, None otherwise
    """
    try:
        if not opportunity_id:
            return None

        # Create repository
        repo = TradeOpportunityRepository(engine=db.engine, session=db.session)

        # Use the repository to get the opportunity
        return repo.get_by_id(opportunity_id)

    except ValueError as ve:
        # Invalid UUID format
        logger.error(f"❌ Invalid UUID format for opportunity_id: {opportunity_id} - {ve}")
        return None
    except Exception as e:
        logger.error(f"❌ Error getting opportunity: {e}", exc_info=True)
        return None


def get_recent_opportunities(db: DatabaseService, count: int = 10, strategy_name: Optional[str] = None) -> list:
    """
    Get recent opportunities

    Args:
        db: Database service instance
        count: Number of opportunities to retrieve
        strategy_name: Optional strategy name to filter by

    Returns:
        List[Dict]: List of recent opportunities
    """
    try:
        # Create repository
        repo = TradeOpportunityRepository(engine=db.engine, session=db.session)

        # Use the repository to get recent opportunities
        return repo.get_recent(count=count, strategy_name=strategy_name)

    except Exception as e:
        logger.error(f"❌ Error getting recent opportunities: {e}", exc_info=True)
        return []


def verify_data_persistence(db: DatabaseService) -> dict:
    """
    Verify that trade opportunities and executions are being persisted correctly.

    Args:
        db: Database service instance

    Returns:
        Dict with counts and status of persistence
    """
    try:
        # Get recent counts from database
        recent_opportunities = db.get_recent_opportunities(count=50)
        recent_executions = db.get_recent_executions(count=50)

        # Get counts from the last hour
        from datetime import datetime, timedelta

        one_hour_ago = datetime.now() - timedelta(hours=1)
        one_hour_timestamp = one_hour_ago.timestamp()

        opportunities_last_hour = sum(
            1 for opp in recent_opportunities if getattr(opp, "opportunity_timestamp", 0) > one_hour_timestamp
        )

        executions_last_hour = sum(
            1 for exec in recent_executions if getattr(exec, "execution_timestamp", 0) > one_hour_timestamp
        )

        logger.info(
            f"📊 Data persistence status: {len(recent_opportunities)} total opportunities, "
            f"{opportunities_last_hour} in the last hour, "
            f"{len(recent_executions)} total executions, "
            f"{executions_last_hour} in the last hour"
        )

        return {
            "total_opportunities": len(recent_opportunities),
            "opportunities_last_hour": opportunities_last_hour,
            "total_executions": len(recent_executions),
            "executions_last_hour": executions_last_hour,
            "persistence_active": True,
        }
    except Exception as e:
        logger.error(f"❌ Error verifying data persistence: {e}", exc_info=True)
        return {"error": str(e), "persistence_active": False}
