"""
Exchange Controller - Renders and handles exchange management views.
"""

import logging
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse

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

    # Get all pairs from database with debug logging
    try:
        pairs = db.get_all_trading_pairs()
        logger.info(f"Retrieved {len(pairs)} trading pairs")
        # Log first few pairs for debugging if available
        if pairs:
            sample = pairs[:3]
            logger.info(f"Sample pairs: {[p.model_dump() for p in sample]}")
        else:
            logger.warning("No trading pairs returned from database")
    except Exception as e:
        logger.error(f"Error fetching trading pairs: {e}")
        pairs = []

    # Add the templates instance to context
    if templates is None:
        # Get templates instance from app state
        _templates = request.app.state.templates if hasattr(request.app.state, "templates") else None
    else:
        _templates = templates

    if _templates is None:
        logger.error("Templates instance is not available")
        return "Error: Templates not available"

    return _templates.TemplateResponse(
        "pages/exchanges.html",
        {"request": request, "exchanges": exchanges, "pairs": pairs, "page_title": "Exchange Management"},
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


@router.get("/api/pairs")
async def get_all_pairs(db: DatabaseService = Depends(get_db)):
    """
    Get all trading pairs via API.
    """
    try:
        pairs = db.get_all_trading_pairs()
        return [pair.model_dump() for pair in pairs]
    except Exception as e:
        logger.error(f"Error getting trading pairs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/pairs/{pair_symbol}")
async def get_pair(pair_symbol: str, db: DatabaseService = Depends(get_db)):
    """
    Get a specific trading pair by symbol.
    """
    try:
        pair = db.get_trading_pair_by_symbol(pair_symbol)
        if not pair:
            raise HTTPException(status_code=404, detail=f"Trading pair with symbol {pair_symbol} not found")
        return pair.model_dump()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting trading pair {pair_symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
