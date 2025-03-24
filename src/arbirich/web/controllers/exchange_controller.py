"""
Exchange Controller - Renders and handles exchange management views.
"""

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.arbirich.models.models import Exchange
from src.arbirich.services.database.database_service import DatabaseService

logger = logging.getLogger(__name__)

router = APIRouter()

# Templates
templates = None


def get_templates(request: Request):
    """Get the Jinja2Templates instance from the app state"""
    return request.app.state.templates


# Database Service Dependency
def get_db():
    db = DatabaseService()
    try:
        yield db
    finally:
        db.close()


@router.get("/exchanges", response_class=HTMLResponse)
async def exchanges_page(request: Request, db: DatabaseService = Depends(get_db)):
    """
    Render the exchange management page.
    """
    # Get all exchanges from database
    exchanges = db.get_all_exchanges()

    return templates.TemplateResponse(
        "exchanges.html", {"request": request, "exchanges": exchanges, "page_title": "Exchange Management"}
    )


@router.get("/api/exchanges")
async def get_all_exchanges(db: DatabaseService = Depends(get_db)):
    """
    Get all exchanges via API.
    """
    try:
        exchanges = db.get_all_exchanges()
        return [exchange.model_dump() for exchange in exchanges]
    except Exception as e:
        logger.error(f"Error getting exchanges: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/exchanges/{exchange_id}")
async def get_exchange(exchange_id: int, db: DatabaseService = Depends(get_db)):
    """
    Get a specific exchange by ID.
    """
    try:
        exchange = db.get_exchange(exchange_id)
        if not exchange:
            raise HTTPException(status_code=404, detail=f"Exchange with ID {exchange_id} not found")
        return exchange.model_dump()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting exchange {exchange_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/exchanges")
async def create_exchange(exchange_data: Dict[str, Any], db: DatabaseService = Depends(get_db)):
    """
    Create a new exchange.
    """
    try:
        # Create Exchange object
        exchange = Exchange(**exchange_data)

        # Save to database
        result = db.create_exchange(exchange)

        return {
            "success": True,
            "message": f"Exchange {result.name} created successfully",
            "exchange": result.model_dump(),
        }
    except Exception as e:
        logger.error(f"Error creating exchange: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/api/exchanges/{exchange_id}")
async def update_exchange(exchange_id: int, exchange_data: Dict[str, Any], db: DatabaseService = Depends(get_db)):
    """
    Update an existing exchange.
    """
    try:
        # Get existing exchange
        existing = db.get_exchange(exchange_id)
        if not existing:
            raise HTTPException(status_code=404, detail=f"Exchange with ID {exchange_id} not found")

        # Update fields
        for key, value in exchange_data.items():
            setattr(existing, key, value)

        # Save to database
        result = db.update_exchange(existing)

        return {
            "success": True,
            "message": f"Exchange {result.name} updated successfully",
            "exchange": result.model_dump(),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating exchange {exchange_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/api/exchanges/{exchange_id}")
async def delete_exchange(exchange_id: int, db: DatabaseService = Depends(get_db)):
    """
    Delete an exchange.
    """
    try:
        # Check if exchange exists
        exchange = db.get_exchange(exchange_id)
        if not exchange:
            raise HTTPException(status_code=404, detail=f"Exchange with ID {exchange_id} not found")

        # Delete from database
        success = db.delete_exchange(exchange_id)

        if success:
            return {"success": True, "message": f"Exchange {exchange.name} deleted successfully"}
        else:
            return {"success": False, "message": f"Failed to delete exchange {exchange.name}"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting exchange {exchange_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/exchanges/{exchange_id}/activate")
async def activate_exchange(exchange_id: int, db: DatabaseService = Depends(get_db)):
    """
    Activate an exchange.
    """
    try:
        success = db.activate_exchange(exchange_id)
        if success:
            exchange = db.get_exchange(exchange_id)
            return {"success": True, "message": f"Exchange {exchange.name} activated successfully"}
        else:
            return {"success": False, "message": f"Failed to activate exchange with ID {exchange_id}"}
    except Exception as e:
        logger.error(f"Error activating exchange {exchange_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/exchanges/{exchange_id}/deactivate")
async def deactivate_exchange(exchange_id: int, db: DatabaseService = Depends(get_db)):
    """
    Deactivate an exchange.
    """
    try:
        success = db.deactivate_exchange(exchange_id)
        if success:
            exchange = db.get_exchange(exchange_id)
            return {"success": True, "message": f"Exchange {exchange.name} deactivated successfully"}
        else:
            return {"success": False, "message": f"Failed to deactivate exchange with ID {exchange_id}"}
    except Exception as e:
        logger.error(f"Error deactivating exchange {exchange_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
