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

from src.arbirich.core.trading_service import get_trading_service
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
        # Get trading service components from the status endpoint
        trading_service = get_trading_service()
        trading_status = await trading_service.get_status()

        # Format as a list of processes
        processes = []
        process_id = 12340  # Base PID for display purposes

        # Extract component statuses
        components = trading_status.get("components", {})
        for component_name, status in components.items():
            if component_name != "overall":
                is_active = status == "active"
                processes.append(
                    {
                        "name": component_name.capitalize(),
                        "status": "Running" if is_active else "Stopped",
                        "pid": str(process_id),
                        "memory_usage": f"{5 + len(processes) * 2}MB",  # Placeholder
                        "cpu_usage": f"{2 + len(processes)}%",  # Placeholder
                    }
                )
                process_id += 1

        # Check for strategies
        strategies = trading_status.get("strategies", {})
        for strategy_name, strategy_status in strategies.items():
            if strategy_status.get("active", False):
                processes.append(
                    {
                        "name": f"Strategy: {strategy_name}",
                        "status": "Running",
                        "pid": str(process_id),
                        "memory_usage": "12MB",  # Placeholder
                        "cpu_usage": "3%",  # Placeholder
                    }
                )
                process_id += 1

        # Always add the web server
        processes.append(
            {"name": "Web Server", "status": "Running", "pid": "10000", "memory_usage": "25MB", "cpu_usage": "3%"}
        )

        return processes
    except Exception as e:
        logger.error(f"Error getting processes: {e}")
        # Return fallback processes if the API call fails
        return [
            {"name": "Price Fetcher", "status": "Running", "pid": "12345", "memory_usage": "10MB", "cpu_usage": "2%"},
            {
                "name": "Opportunity Scanner",
                "status": "Running",
                "pid": "12346",
                "memory_usage": "15MB",
                "cpu_usage": "4%",
            },
            {"name": "Trade Executor", "status": "Running", "pid": "12347", "memory_usage": "12MB", "cpu_usage": "3%"},
            {"name": "Web Server", "status": "Running", "pid": "12348", "memory_usage": "25MB", "cpu_usage": "3%"},
        ]
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


async def get_logs():
    """
    Get recent system logs.

    Returns:
        List[Dict]: List of log entries with timestamp, level, level_color, and message.
    """
    try:
        # In a real implementation, this would fetch from your logging system
        # For now, return sample log data
        return [
            {
                "timestamp": "2023-05-16 10:15:32",
                "level": "INFO",
                "level_color": "blue-400",
                "message": "Price update completed for BTC/USDT",
            },
            {
                "timestamp": "2023-05-16 10:15:35",
                "level": "INFO",
                "level_color": "blue-400",
                "message": "Price update completed for ETH/USDT",
            },
            {
                "timestamp": "2023-05-16 10:15:38",
                "level": "SUCCESS",
                "level_color": "profit-green",
                "message": "Found arbitrage opportunity BTC/USDT (Binance->Bybit)",
            },
            {
                "timestamp": "2023-05-16 10:15:45",
                "level": "WARNING",
                "level_color": "yellow-400",
                "message": "Rate limit approaching for KuCoin API",
            },
            {
                "timestamp": "2023-05-16 10:15:50",
                "level": "SUCCESS",
                "level_color": "profit-green",
                "message": "Executed trade #12345",
            },
            {
                "timestamp": "2023-05-16 10:16:02",
                "level": "INFO",
                "level_color": "blue-400",
                "message": "Price update completed for BTC/USDT",
            },
            {
                "timestamp": "2023-05-16 10:16:15",
                "level": "ERROR",
                "level_color": "loss-red",
                "message": "Failed to connect to Crypto.com API",
            },
            {
                "timestamp": "2023-05-16 10:16:22",
                "level": "INFO",
                "level_color": "blue-400",
                "message": "Retry attempt 1 for Crypto.com connection",
            },
            {
                "timestamp": "2023-05-16 10:16:35",
                "level": "INFO",
                "level_color": "blue-400",
                "message": "Price update completed for ETH/USDT",
            },
            {
                "timestamp": "2023-05-16 10:16:38",
                "level": "ERROR",
                "level_color": "loss-red",
                "message": "Retry failed for Crypto.com API",
            },
        ]
    except Exception as e:
        logger.error(f"Error retrieving logs: {e}")
        return []


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
