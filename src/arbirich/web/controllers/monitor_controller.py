"""
Monitor Controller - Renders and handles system monitoring views.
"""

import logging
import platform
from datetime import datetime, timedelta

import psutil
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from arbirich.core.trading_service import TradingService
from src.arbirich.services.database.database_service import DatabaseService
from src.arbirich.services.redis.redis_service import RedisService

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


@router.get("/monitor", response_class=HTMLResponse)
async def monitor_page(request: Request, db: DatabaseService = Depends(get_db)):
    """
    Render the system monitoring page.
    """
    return templates.TemplateResponse("monitor.html", {"request": request})


@router.get("/api/monitor/system-status")
async def get_system_status():
    """
    Get the current system status (CPU, memory, disk usage, etc.).
    """
    try:
        # Get CPU usage
        cpu_percent = psutil.cpu_percent(interval=0.5)

        # Get memory usage
        memory = psutil.virtual_memory()
        memory_percent = memory.percent

        # Get disk usage
        disk = psutil.disk_usage("/")
        disk_percent = disk.percent

        # Get system uptime (depends on platform)
        if platform.system() == "Windows":
            uptime_seconds = psutil.boot_time()
            uptime = datetime.now() - datetime.fromtimestamp(uptime_seconds)
        else:
            uptime_seconds = psutil.boot_time()
            uptime = datetime.now() - datetime.fromtimestamp(uptime_seconds)

        # Format uptime as days and hours
        days = uptime.days
        hours, remainder = divmod(uptime.seconds, 3600)
        uptime_str = f"{days} days, {hours} hours"

        return {
            "cpu_usage": cpu_percent,
            "memory_usage": memory_percent,
            "disk_usage": disk_percent,
            "uptime": uptime_str,
            "uptime_seconds": int((datetime.now() - datetime.fromtimestamp(uptime_seconds)).total_seconds()),
        }
    except Exception as e:
        logger.error(f"Error getting system status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/monitor/processes")
async def get_processes():
    """
    Get the current running processes related to ArbiRich.
    """
    try:
        # Get trading service components
        trading_service = TradingService()
        trading_status = await trading_service.get_status()

        # Format as a list of processes
        processes = []

        for component, active in trading_status.items():
            if component != "overall" and active:
                # This is a placeholder for a real PID
                processes.append(
                    {
                        "name": component.capitalize(),
                        "status": "Running" if active else "Stopped",
                        "pid": f"1234{len(processes)}",  # Fake PID
                        "memory_usage": f"{5 + len(processes) * 2}MB",  # Fake memory usage
                        "cpu_usage": f"{2 + len(processes)}%",  # Fake CPU usage
                    }
                )

        # Always add the web server
        processes.append(
            {"name": "Web Server", "status": "Running", "pid": "12340", "memory_usage": "25MB", "cpu_usage": "3%"}
        )

        return processes
    except Exception as e:
        logger.error(f"Error getting processes: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/monitor/exchange-status")
async def get_exchange_status(db: DatabaseService = Depends(get_db)):
    """
    Get the current exchange connection status.
    """
    try:
        # Get exchanges from database
        exchanges = db.get_all_exchanges()

        # Create a Redis service to check connectivity
        redis_service = RedisService()

        # Check connectivity status for each exchange
        exchange_status = []
        for exchange in exchanges:
            # This is a simplified check - in a real implementation,
            # you would check if the exchange connection is active
            # by checking the Redis or database status

            # For now, just create some mock data
            status = "Connected"
            if exchange.name.lower() == "kucoin":
                status = "Rate Limited"
            elif exchange.name.lower() == "crypto.com":
                status = "Disconnected"

            exchange_status.append(
                {
                    "name": exchange.name,
                    "status": status,
                    "last_update": datetime.now().isoformat(),
                    "active": exchange.is_active,
                }
            )

        # Close Redis service
        redis_service.close()

        return exchange_status
    except Exception as e:
        logger.error(f"Error getting exchange status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/monitor/logs")
async def get_recent_logs(count: int = 20):
    """
    Get recent log entries.
    """
    try:
        # In a real implementation, you would fetch from a log file or database
        # For now, generate some sample logs

        # Define log levels
        log_levels = ["INFO", "WARNING", "ERROR", "SUCCESS"]
        log_colors = {"INFO": "blue-400", "WARNING": "yellow-400", "ERROR": "loss-red", "SUCCESS": "profit-green"}

        # Generate sample logs
        logs = []
        now = datetime.now()

        for i in range(count):
            time_offset = timedelta(seconds=i * 15)
            log_time = (now - time_offset).strftime("%Y-%m-%d %H:%M:%S")

            # Select level based on index
            level_idx = i % len(log_levels)
            level = log_levels[level_idx]
            color = log_colors[level]

            # Generate message based on level
            if level == "INFO":
                message = f"Price update completed for {'BTC' if i % 2 == 0 else 'ETH'}/USDT"
            elif level == "WARNING":
                message = "Rate limit approaching for KuCoin API"
            elif level == "ERROR":
                message = "Failed to connect to Crypto.com API"
            else:  # SUCCESS
                if i % 2 == 0:
                    message = "Found arbitrage opportunity BTC/USDT (Binance->Bybit)"
                else:
                    message = f"Executed trade #{10000 + i}"

            logs.append({"timestamp": log_time, "level": level, "level_color": color, "message": message})

        return logs
    except Exception as e:
        logger.error(f"Error getting logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/monitor/trading-activity")
async def get_trading_activity():
    """
    Get data for the trading activity chart.
    """
    try:
        # In a real implementation, you would fetch from the database
        # For now, generate sample data

        # Get current time and generate data points for the past 30 minutes
        now = datetime.now()
        labels = []
        opportunities_data = []
        trades_data = []

        for i in range(30):
            time_offset = timedelta(minutes=29 - i)
            point_time = now - time_offset
            labels.append(point_time.strftime("%H:%M"))

            # Generate random data with some correlation
            opportunities = min(10, max(0, 5 + (i % 7) - 3))
            trades = min(opportunities, max(0, 2 + (i % 5) - 2))

            opportunities_data.append(opportunities)
            trades_data.append(trades)

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
                    "data": trades_data,
                    "borderColor": "#3abe78",
                    "backgroundColor": "rgba(58, 190, 120, 0.1)",
                },
            ],
        }
    except Exception as e:
        logger.error(f"Error getting trading activity: {e}")
        raise HTTPException(status_code=500, detail=str(e))
