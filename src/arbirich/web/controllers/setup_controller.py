"""
Setup Controller - Renders and handles system setup views.
"""

import logging
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from src.arbirich.models.models import Exchange, Pair, Strategy
from src.arbirich.services.database.database_service import DatabaseService

logger = logging.getLogger(__name__)

router = APIRouter()

# Templates
templates = Jinja2Templates(directory="src/arbirich/web/templates")


# Database Service Dependency
def get_db():
    db = DatabaseService()
    try:
        yield db
    finally:
        db.close()


@router.get("/setup", response_class=HTMLResponse)
async def setup_page(request: Request, db: DatabaseService = Depends(get_db)):
    """
    Render the system setup page.
    """
    # Get exchanges, strategies, and other config data from DB
    exchanges = db.get_all_exchanges()
    strategies = db.get_all_strategies()

    # Get database connection status
    db_status = "Connected" if db.is_connected() else "Disconnected"

    return templates.TemplateResponse(
        "setup.html", {"request": request, "exchanges": exchanges, "strategies": strategies, "db_status": db_status}
    )


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

        return {"success": True, "message": f"Exchange {result.name} created successfully", "exchange": result}
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
        existing = db.get_exchange_by_id(exchange_id)
        if not existing:
            raise HTTPException(status_code=404, detail=f"Exchange with ID {exchange_id} not found")

        # Update fields
        for key, value in exchange_data.items():
            setattr(existing, key, value)

        # Save to database
        result = db.update_exchange(existing)

        return {"success": True, "message": f"Exchange {result.name} updated successfully", "exchange": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating exchange {exchange_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/strategies")
async def create_strategy(strategy_data: Dict[str, Any], db: DatabaseService = Depends(get_db)):
    """
    Create a new strategy.
    """
    try:
        # Create Strategy object
        strategy = Strategy(**strategy_data)

        # Save to database
        result = db.create_strategy(strategy)

        return {"success": True, "message": f"Strategy {result.name} created successfully", "strategy": result}
    except Exception as e:
        logger.error(f"Error creating strategy: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/api/strategies/{strategy_id}")
async def update_strategy(strategy_id: int, strategy_data: Dict[str, Any], db: DatabaseService = Depends(get_db)):
    """
    Update an existing strategy.
    """
    try:
        # Get existing strategy
        existing = db.get_strategy_by_id(strategy_id)
        if not existing:
            raise HTTPException(status_code=404, detail=f"Strategy with ID {strategy_id} not found")

        # Update fields
        for key, value in strategy_data.items():
            setattr(existing, key, value)

        # Save to database
        result = db.update_strategy(existing)

        return {"success": True, "message": f"Strategy {result.name} updated successfully", "strategy": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating strategy {strategy_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/pairs")
async def create_pair(pair_data: Dict[str, Any], db: DatabaseService = Depends(get_db)):
    """
    Create a new trading pair.
    """
    try:
        # Create Pair object
        pair = Pair(**pair_data)

        # Save to database
        result = db.create_pair(pair)

        return {"success": True, "message": f"Pair {result.symbol} created successfully", "pair": result}
    except Exception as e:
        logger.error(f"Error creating pair: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/system/backup")
async def create_backup(db: DatabaseService = Depends(get_db)):
    """
    Create a database backup.
    """
    try:
        # Create backup
        backup_path = db.create_backup()

        return {
            "success": True,
            "message": "Database backup created successfully",
            "backup_path": backup_path,
            "timestamp": backup_path.split("_")[-1].split(".")[0] if backup_path else None,
        }
    except Exception as e:
        logger.error(f"Error creating backup: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/system/restore")
async def restore_backup(backup_path: str, db: DatabaseService = Depends(get_db)):
    """
    Restore from a database backup.
    """
    try:
        # Restore from backup
        success = db.restore_from_backup(backup_path)

        return {
            "success": success,
            "message": "Database restored successfully" if success else "Failed to restore database",
        }
    except Exception as e:
        logger.error(f"Error restoring backup: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/system/backups")
async def get_backups(db: DatabaseService = Depends(get_db)):
    """
    Get list of available backups.
    """
    try:
        # Get list of backups
        backups = db.list_backups()

        return {"success": True, "backups": backups}
    except Exception as e:
        logger.error(f"Error listing backups: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/system/settings")
async def get_system_settings(db: DatabaseService = Depends(get_db)):
    """
    Get system settings.
    """
    try:
        # This is a placeholder - actual implementation would fetch settings
        # from a settings table or configuration service
        settings = {
            "auto_start": True,
            "notifications_enabled": True,
            "real_trading_enabled": False,
            "log_level": "INFO",
            "max_concurrent_trades": 3,
        }

        return settings
    except Exception as e:
        logger.error(f"Error getting system settings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/system/settings")
async def update_system_settings(settings: Dict[str, Any], db: DatabaseService = Depends(get_db)):
    """
    Update system settings.
    """
    try:
        # This is a placeholder - actual implementation would update settings
        # in a settings table or configuration service

        # For now, we'll just return the settings as if they were saved
        return {"success": True, "message": "Settings updated successfully", "settings": settings}
    except Exception as e:
        logger.error(f"Error updating system settings: {e}")
        raise HTTPException(status_code=500, detail=str(e))
