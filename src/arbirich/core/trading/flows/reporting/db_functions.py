# reporting/db_functions.py

import logging
from typing import Any, Optional

from src.arbirich.services.database.database_service import DatabaseService
from src.arbirich.services.database.repositories.trade_opportunity_repository import TradeOpportunityRepository

logger = logging.getLogger(__name__)


def check_opportunity_exists(db: DatabaseService, opportunity_id: str) -> bool:
    """
    Check if an opportunity with the given ID exists in the database

    Args:
        db: Database service instance
        opportunity_id: UUID string of the opportunity to check

    Returns:
        bool: True if opportunity exists, False otherwise
    """
    try:
        if not opportunity_id:
            return False

        # Create repository
        repo = TradeOpportunityRepository(engine=db.engine, session=db.session)

        # Use the repository to check if opportunity exists
        opportunity = repo.get_by_id(opportunity_id)

        return opportunity is not None

    except ValueError as ve:
        # Invalid UUID format
        logger.error(f"Invalid UUID format for opportunity_id: {opportunity_id} - {ve}")
        return False
    except Exception as e:
        logger.error(f"Error checking opportunity existence: {e}", exc_info=True)
        return False


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
        logger.error(f"Invalid UUID format for opportunity_id: {opportunity_id} - {ve}")
        return None
    except Exception as e:
        logger.error(f"Error getting opportunity: {e}", exc_info=True)
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
        logger.error(f"Error getting recent opportunities: {e}", exc_info=True)
        return []
