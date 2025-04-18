"""
Dashboard Controller - Handles dashboard UI and API endpoints
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from src.arbirich.services.database.database_service import DatabaseService
from src.arbirich.web.dependencies import get_db_service

logger = logging.getLogger(__name__)

router = APIRouter()

# Use the consistent approach for template directory
base_dir = Path(__file__).resolve().parent.parent
templates_dir = base_dir / "templates"
logger.info(f"Dashboard controller templates directory: {templates_dir}")
templates = Jinja2Templates(directory=str(templates_dir))


def get_db():
    """Get database service instance"""
    # Re-use the common implementation
    return get_db_service()


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Render the index page."""
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/about", response_class=HTMLResponse)
async def about(request: Request):
    """Render the about page."""
    return templates.TemplateResponse("about.html", {"request": request})


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request, db_gen: DatabaseService = Depends(get_db)):
    """Render the dashboard page"""
    # Get stats for dashboard
    try:
        # Extract the database service from the generator
        db = next(db_gen)

        # Get time period for recent stats (last 24 hours)
        since_time = datetime.now() - timedelta(hours=24)

        # Get counts
        opportunities_count = db.count_opportunities_since(since_time)
        executions_count = db.count_executions_since(since_time)
        profitable_executions = db.count_profitable_executions()

        # Get recent data for widgets
        recent_opportunities = db.get_recent_opportunities(limit=5)
        recent_executions = db.get_recent_executions(limit=5)

        # Get all active exchanges and pairs
        active_exchanges = db.get_active_exchanges()
        active_pairs = db.get_active_pairs()

        # Get all active strategies with their performance metrics
        active_strategies = db.get_active_strategies()

        # Prepare context for the template
        context = {
            "request": request,
            "page_title": "Dashboard",
            "stats": {
                "opportunities": opportunities_count,
                "executions": executions_count,
                "profitable_executions": profitable_executions,
                "profit_rate": profitable_executions / executions_count * 100 if executions_count > 0 else 0,
            },
            "recent_opportunities": recent_opportunities,
            "recent_executions": recent_executions,
            "active_exchanges": active_exchanges,
            "active_pairs": active_pairs,
            "active_strategies": active_strategies,
            "refresh_interval": 30,  # Seconds between auto-refresh
        }

        return templates.TemplateResponse("dashboard.html", context)
    except Exception as e:
        logger.error(f"Error loading dashboard data: {e}")
        # Return a simplified error view
        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "page_title": "Dashboard",
                "error": str(e),
                "refresh_interval": 60,  # Longer refresh on error
            },
        )


@router.get("/api/dashboard/stats")
async def dashboard_stats(db_gen: DatabaseService = Depends(get_db)):
    """Get dashboard statistics"""
    try:
        # Extract the database service from the generator
        db = next(db_gen)

        # Get time period for recent stats (last 24 hours)
        since_time = datetime.now() - timedelta(hours=24)

        # Get counts
        opportunities_count = db.count_opportunities_since(since_time)
        executions_count = db.count_executions_since(since_time)
        profitable_executions = db.count_profitable_executions()

        return {
            "opportunities": opportunities_count,
            "executions": executions_count,
            "profitable_executions": profitable_executions,
            "profit_rate": profitable_executions / executions_count * 100 if executions_count > 0 else 0,
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        logger.error(f"Error getting dashboard stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/dashboard/recent-opportunities")
async def recent_opportunities(limit: int = 5, db_gen: DatabaseService = Depends(get_db)):
    """Get recent trade opportunities"""
    try:
        # Extract the database service from the generator
        db = next(db_gen)

        opportunities = db.get_recent_opportunities(limit=limit)
        return opportunities
    except Exception as e:
        logger.error(f"Error getting recent opportunities: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/dashboard/recent-executions")
async def recent_executions(limit: int = 5, db_gen: DatabaseService = Depends(get_db)):
    """Get recent trade executions"""
    try:
        # Extract the database service from the generator
        db = next(db_gen)

        executions = db.get_recent_executions(limit=limit)
        return executions
    except Exception as e:
        logger.error(f"Error getting recent executions: {e}")
        raise HTTPException(status_code=500, detail=str(e))
