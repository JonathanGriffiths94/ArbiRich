"""
Setup Controller - Renders and handles system setup views.
"""

import logging
from pathlib import Path

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from src.arbirich.services.database.database_service import DatabaseService
from src.arbirich.web.dependencies import get_db_service

logger = logging.getLogger(__name__)

router = APIRouter()

# Use the consistent approach for template directory
base_dir = Path(__file__).resolve().parent.parent
templates_dir = base_dir / "templates"
logger.info(f"Setup controller templates directory: {templates_dir}")
templates = Jinja2Templates(directory=str(templates_dir))


@router.get("/setup", response_class=HTMLResponse)
async def setup(request: Request, db_gen: DatabaseService = Depends(get_db_service)):
    """Render the initial setup page."""
    try:
        # Extract the database service from the generator
        db = next(db_gen)

        # Check if setup has been completed
        setup_completed = True  # Replace with actual check using db

        return templates.TemplateResponse("pages/setup.html", {"request": request, "setup_completed": setup_completed})
    except Exception as e:
        logger.error(f"Error rendering setup page: {e}")
        return templates.TemplateResponse(
            "errors/error.html", {"request": request, "error_message": f"Error loading setup page: {str(e)}"}
        )
