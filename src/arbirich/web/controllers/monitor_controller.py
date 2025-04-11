import logging
import platform
from datetime import datetime, timedelta
from pathlib import Path

import psutil
from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from src.arbirich.services.database.database_service import DatabaseService
from src.arbirich.web.dependencies import get_db_service

logger = logging.getLogger(__name__)

router = APIRouter()

base_dir = Path(__file__).resolve().parent.parent
templates_dir = base_dir / "templates"
logger.info(f"Monitor controller templates directory: {templates_dir}")
templates = Jinja2Templates(directory=str(templates_dir))


@router.get("/monitor", response_class=HTMLResponse)
async def monitor(request: Request, db_gen: DatabaseService = Depends(get_db_service)):
    """Render the system monitoring page."""
    try:
        # Extract the database service from the generator
        db = next(db_gen)

        # Get system metrics
        system_info = await get_system_status()
        processes = await get_processes()
        exchange_status = await get_exchange_status(db)
        trading_activity = await get_trading_activity()

        return templates.TemplateResponse(
            "pages/monitor.html",
            {
                "request": request,
                "system_info": system_info,
                "processes": processes,
                "exchange_status": exchange_status,
                "trading_activity": trading_activity,
            },
        )
    except Exception as e:
        logger.error(f"Error rendering monitor page: {e}")
        return templates.TemplateResponse(
            "errors/error.html", {"request": request, "error_message": f"Error loading monitoring data: {str(e)}"}
        )


async def get_system_status():
    """Get system status information."""
    try:
        # Get system information
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage("/")

        return {
            "cpu_percent": psutil.cpu_percent(),
            "memory_percent": memory.percent,
            "memory_used": f"{memory.used / (1024**3):.2f} GB",
            "memory_total": f"{memory.total / (1024**3):.2f} GB",
            "disk_percent": disk.percent,
            "disk_used": f"{disk.used / (1024**3):.2f} GB",
            "disk_total": f"{disk.total / (1024**3):.2f} GB",
            "platform": platform.platform(),
            "python_version": platform.python_version(),
            "boot_time": datetime.fromtimestamp(psutil.boot_time()).strftime("%Y-%m-%d %H:%M:%S"),
        }
    except Exception as e:
        logger.error(f"Error getting system status: {e}")
        return {"error": str(e)}


async def get_processes():
    """Get process information."""
    try:
        # Mock data for demo purposes - in a real implementation, you would get actual process info
        return [
            {
                "name": "ArbiRich Web Server",
                "status": "Running",
                "pid": "12345",
                "memory_usage": "45MB",
                "cpu_usage": "2%",
            },
            {"name": "Ingestion Flow", "status": "Running", "pid": "12346", "memory_usage": "78MB", "cpu_usage": "5%"},
            {"name": "Arbitrage Flow", "status": "Running", "pid": "12347", "memory_usage": "92MB", "cpu_usage": "7%"},
            {"name": "Execution Flow", "status": "Running", "pid": "12348", "memory_usage": "64MB", "cpu_usage": "3%"},
            {"name": "Reporting Flow", "status": "Running", "pid": "12349", "memory_usage": "53MB", "cpu_usage": "2%"},
        ]
    except Exception as e:
        logger.error(f"Error getting process information: {e}")
        return [{"name": "Error loading processes", "status": "Error", "pid": "N/A"}]


async def get_exchange_status(db: DatabaseService):
    """Get exchange status information."""
    try:
        # The function now receives the actual database service instance, not a generator
        exchanges = db.get_all_exchanges()
        exchange_status = []

        for exchange in exchanges:
            # Mock latency and status data
            exchange_status.append(
                {
                    "name": exchange.name,
                    "status": "Online" if exchange.is_active else "Offline",
                    "latency": f"{50 + hash(exchange.name) % 100}ms",
                    "last_update": datetime.now().strftime("%H:%M:%S"),
                    "api_rate_limit": exchange.api_rate_limit,
                }
            )

        return exchange_status
    except Exception as e:
        logger.error(f"Error getting exchange status: {e}")
        return [{"name": "Error loading exchange status", "status": "Error"}]


async def get_trading_activity():
    """Get trading activity chart data."""
    try:
        # Mock data for trading activity chart
        labels = []
        opportunities_data = []
        executions_data = []

        # Generate data for the last 24 hours
        now = datetime.now()
        for i in range(24):
            time_point = now - timedelta(hours=i)
            labels.insert(0, time_point.strftime("%H:00"))

            # Mock data with some randomization
            opportunities = 10 + (hash(str(i)) % 40)
            executions = max(0, opportunities // 2 - (hash(str(i + 1)) % 10))

            opportunities_data.insert(0, opportunities)
            executions_data.insert(0, executions)

        return {
            "labels": labels,
            "datasets": [
                {
                    "label": "Opportunities Found",
                    "data": opportunities_data,
                    "borderColor": "#85bb65",
                    "backgroundColor": "rgba(133, 187, 101, 0.1)",
                },
                {
                    "label": "Trades Executed",
                    "data": executions_data,
                    "borderColor": "#3abe78",
                    "backgroundColor": "rgba(58, 190, 120, 0.1)",
                },
            ],
        }
    except Exception as e:
        logger.error(f"Error getting trading activity data: {e}")
        return {"labels": [], "datasets": []}
