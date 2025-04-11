"""
Exchange Controller - Renders and handles exchange management views.
"""

import logging
from pathlib import Path
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from src.arbirich.services.database.database_service import DatabaseService
from src.arbirich.web.dependencies import get_db_service

logger = logging.getLogger(__name__)

router = APIRouter()

base_dir = Path(__file__).resolve().parent.parent
templates_dir = base_dir / "templates"
logger.info(f"Exchange controller templates directory: {templates_dir}")
templates = Jinja2Templates(directory=str(templates_dir))


@router.get("/exchanges", response_class=HTMLResponse)
async def exchanges(request: Request, db_gen: DatabaseService = Depends(get_db_service)):
    """Render the exchanges management page."""
    try:
        # Extract the database service from the generator
        db = next(db_gen)
        exchanges = db.get_all_exchanges()
        pairs = db.get_all_pairs()

        return templates.TemplateResponse(
            "pages/exchanges.html", {"request": request, "exchanges": exchanges, "pairs": pairs}
        )
    except Exception as e:
        logger.error(f"Error rendering exchanges page: {e}")
        return templates.TemplateResponse(
            "errors/error.html", {"request": request, "error_message": f"Error loading exchanges data: {str(e)}"}
        )


@router.get("/api/exchanges")
async def get_all_exchanges(db_gen: DatabaseService = Depends(get_db_service)):
    """
    Get all exchanges via API.
    """
    try:
        # Extract the database service from the generator
        db = next(db_gen)
        exchanges = db.get_all_exchanges()
        return [exchange.model_dump() for exchange in exchanges]
    except Exception as e:
        logger.error(f"Error getting exchanges: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/exchanges/{exchange_id}")
async def get_exchange(exchange_id: int, db_gen: DatabaseService = Depends(get_db_service)):
    """
    Get a specific exchange by ID.
    """
    try:
        # Extract the database service from the generator
        db = next(db_gen)
        exchange = db.get_exchange(exchange_id)
        if not exchange:
            raise HTTPException(status_code=404, detail=f"Exchange with ID {exchange_id} not found")
        return exchange.model_dump()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting exchange {exchange_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/api/exchanges/{exchange_id}")
async def update_exchange(
    exchange_id: int, exchange_data: Dict[str, Any], db_gen: DatabaseService = Depends(get_db_service)
):
    """
    Update an existing exchange.
    """
    try:
        # Extract the database service from the generator
        db = next(db_gen)

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


@router.get("/api/pairs")
async def get_all_pairs(db_gen: DatabaseService = Depends(get_db_service)):
    """
    Get all trading pairs via API.
    """
    try:
        # Extract the database service from the generator
        db = next(db_gen)
        pairs = db.get_all_trading_pairs()
        return [pair.model_dump() for pair in pairs]
    except Exception as e:
        logger.error(f"Error getting trading pairs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/pairs/{pair_symbol}")
async def get_pair(pair_symbol: str, db_gen: DatabaseService = Depends(get_db_service)):
    """
    Get a specific trading pair by symbol.
    """
    try:
        # Extract the database service from the generator
        db = next(db_gen)
        pair = db.get_trading_pair_by_symbol(pair_symbol)
        if not pair:
            raise HTTPException(status_code=404, detail=f"Trading pair with symbol {pair_symbol} not found")
        return pair.model_dump()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting trading pair {pair_symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
