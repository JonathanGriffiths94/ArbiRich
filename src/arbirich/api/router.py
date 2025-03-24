import logging
from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from src.arbirich.api.dashboard_api import router as dashboard_router
from src.arbirich.models.models import Strategy, TradeExecution, TradeOpportunity
from src.arbirich.services.database.database_service import DatabaseService

logger = logging.getLogger(__name__)

# Create the API router
router = APIRouter()

# Include the dashboard router
router.include_router(dashboard_router)


# Dependency for database service
def get_db():
    db = DatabaseService()
    try:
        yield db
    finally:
        pass


#
# Status Endpoints
#
@router.get("/status", tags=["status"])
async def get_status():
    """Get the current status of the ArbiRich API."""
    return {
        "status": "operational",
        "version": "0.1.0",
        "timestamp": datetime.now().isoformat(),
        "uptime": "N/A",  # To be implemented
        "active_strategies": 3,  # To be implemented with actual count
    }


# Strategy Endpoints
@router.get("/strategies", tags=["strategies"], response_model=List[Strategy])
async def get_strategies(db: DatabaseService = Depends(get_db)):
    """Get information about all available trading strategies."""
    try:
        strategies = db.get_all_strategies()
        return strategies
    except Exception as e:
        logger.error(f"Error fetching strategies: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve strategies")


@router.get("/strategies/{strategy_id}", tags=["strategies"], response_model=Strategy)
async def get_strategy(
    strategy_id: int = Path(..., description="The ID of the strategy to retrieve"),
    db: DatabaseService = Depends(get_db),
):
    """Get detailed information about a specific strategy."""
    try:
        strategy = db.get_strategy(strategy_id)
        if not strategy:
            raise HTTPException(status_code=404, detail=f"Strategy with ID {strategy_id} not found")
        return strategy
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching strategy {strategy_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve strategy")


@router.put("/strategies/{strategy_id}/activate", tags=["strategies"])
async def activate_strategy(
    strategy_id: int = Path(..., description="The ID of the strategy to activate"),
    db: DatabaseService = Depends(get_db),
):
    """Activate a strategy."""
    try:
        success = db.activate_strategy(strategy_id)
        if not success:
            raise HTTPException(status_code=404, detail=f"Strategy with ID {strategy_id} not found")
        return {"status": "success", "message": f"Strategy with ID {strategy_id} activated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error activating strategy {strategy_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to activate strategy")


@router.put("/strategies/{strategy_id}/deactivate", tags=["strategies"])
async def deactivate_strategy(
    strategy_id: int = Path(..., description="The ID of the strategy to deactivate"),
    db: DatabaseService = Depends(get_db),
):
    """Deactivate a strategy."""
    try:
        success = db.deactivate_strategy(strategy_id)
        if not success:
            raise HTTPException(status_code=404, detail=f"Strategy with ID {strategy_id} not found")
        return {"status": "success", "message": f"Strategy with ID {strategy_id} deactivated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deactivating strategy {strategy_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to deactivate strategy")


# Opportunity Endpoints
@router.get("/opportunities", tags=["opportunities"])
async def get_opportunities(
    strategy: Optional[str] = Query(None, description="Filter by strategy name"),
    pair: Optional[str] = Query(None, description="Filter by trading pair"),
    limit: int = Query(10, description="Maximum number of opportunities to return", ge=1, le=100),
    db: DatabaseService = Depends(get_db),
):
    """Get recent trading opportunities."""
    try:
        # Basic implementation - to be expanded with filtering
        if strategy:
            opportunities = db.get_opportunities_by_strategy(strategy)
        else:
            # This needs to be implemented - for now return empty list
            opportunities = []

        # Apply additional filters and limits
        return {"opportunities": opportunities[:limit]}
    except Exception as e:
        logger.error(f"Error fetching opportunities: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve opportunities")


#
# Execution Endpoints
#
@router.get("/executions", tags=["executions"])
async def get_executions(
    strategy: Optional[str] = Query(None, description="Filter by strategy name"),
    pair: Optional[str] = Query(None, description="Filter by trading pair"),
    limit: int = Query(10, description="Maximum number of executions to return", ge=1, le=100),
    db: DatabaseService = Depends(get_db),
):
    """Get recent trade executions."""
    try:
        # Get executions based on strategy filter
        if strategy:
            executions = db.get_executions_by_strategy(strategy)
        else:
            # This needs to be implemented - for now collect from all strategies
            executions = []
            strategies = db.get_all_strategies()
            for strat in strategies:
                strat_executions = db.get_executions_by_strategy(strat.name)
                executions.extend(strat_executions)

        # Apply pair filter if provided
        if pair:
            executions = [e for e in executions if e.pair == pair]

        # Sort by timestamp (newest first) and apply limit
        executions = sorted(executions, key=lambda e: e.execution_timestamp, reverse=True)[:limit]

        return {"executions": executions}
    except Exception as e:
        logger.error(f"Error fetching executions: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve executions")
