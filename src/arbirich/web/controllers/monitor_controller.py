import logging
import time
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

        # Calculate uptime in a human-readable format
        boot_time = psutil.boot_time()
        uptime_seconds = time.time() - boot_time
        uptime_days = int(uptime_seconds // (60 * 60 * 24))
        uptime_hours = int((uptime_seconds % (60 * 60 * 24)) // (60 * 60))
        uptime = f"{uptime_days} days, {uptime_hours} hours"

        return {
            "cpu_usage": psutil.cpu_percent(),
            "memory_usage": memory.percent,
            "disk_usage": disk.percent,
            "uptime": uptime,
            "services": {
                "redis": True,  # Placeholder - implement actual check if needed
                "database": True,  # Placeholder - implement actual check if needed
                "web_server": True,  # Always True since we're responding
                "processors": check_processor_status(),  # Helper function to check processor status
            },
        }
    except Exception as e:
        logger.error(f"Error getting system status: {e}")
        return {"error": str(e)}


def check_processor_status():
    """Check if processors are running properly."""
    try:
        # This is a simplified check - you can expand it with actual logic
        # to verify if your exchange processors are running
        process_count = 0
        for proc in psutil.process_iter(["name", "cmdline"]):
            try:
                if proc.info["name"] and "python" in proc.info["name"].lower():
                    if proc.info["cmdline"] and any("processor" in cmd.lower() for cmd in proc.info["cmdline"]):
                        process_count += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        return process_count > 0
    except Exception as e:
        logger.error(f"Error checking processor status: {e}")
        return False


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

        from src.arbirich.models.enums import Status

        for exchange in exchanges:
            # Mock latency and status data
            exchange_status.append(
                {
                    "name": exchange.name,
                    "status": Status.ACTIVE.value if exchange.is_active else Status.INACTIVE.value,
                    "latency": f"{50 + hash(exchange.name) % 100}ms",
                    "last_update": datetime.now().strftime("%H:%M:%S"),
                    "api_rate_limit": exchange.api_rate_limit,
                }
            )

        return exchange_status
    except Exception as e:
        logger.error(f"Error getting exchange status: {e}")
        return [{"name": "Error loading exchange status", "status": Status.ERROR.value}]


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
