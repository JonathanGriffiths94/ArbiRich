import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import redis
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, status
from fastapi.responses import JSONResponse

from src.arbirich.config.config import get_all_strategy_names, get_strategy_config
from src.arbirich.core.config.model_registry import ConfigModelRegistry
from src.arbirich.core.config.validator import BasicStrategyConfig
from src.arbirich.core.trading.trading_service import get_trading_service
from src.arbirich.models.models import Strategy
from src.arbirich.models.router_models import (
    ChartData,
    ChartDataset,
    DashboardStats,
    HealthResponse,
    StatusResponse,
    TradingStatusResponse,
    TradingStopRequest,
)
from src.arbirich.services.database.database_service import DatabaseService
from src.arbirich.services.metrics.strategy_metrics_service import StrategyMetricsService
from src.arbirich.services.redis.redis_service import RedisService
from src.arbirich.utils.channel_diagnostics import log_channel_diagnostics
from src.arbirich.web.dependencies import get_db_service, get_metrics_service
from src.arbirich.web.routes.strategy_routes import router as strategy_router

logger = logging.getLogger(__name__)

# Create main API router
main_router = APIRouter()

# Create sub-routers for different API sections
status_router = APIRouter(prefix="/status", tags=["status"])
trading_router = APIRouter(prefix="/trading", tags=["trading"])
strategies_router = APIRouter(prefix="/strategies", tags=["strategies"])
dashboard_router = APIRouter(prefix="/dashboard", tags=["dashboard"])
market_router = APIRouter(prefix="/market", tags=["market"])
trading_pairs_router = APIRouter(prefix="/trading-pairs", tags=["trading-pairs"])
exchanges_router = APIRouter(prefix="/exchanges", tags=["exchanges"])
monitor_api_router = APIRouter(prefix="/monitor", tags=["monitor"])


def get_db():
    """Database service dependency"""
    # Re-use the common implementation
    return get_db_service()


def get_services() -> Tuple[DatabaseService, RedisService]:
    """
    Dependency to get required services for trading operations

    Returns:
        Tuple of database service and Redis service
    """
    db_service = DatabaseService()
    redis_service = RedisService()

    try:
        yield (db_service, redis_service)
    finally:
        # Add error handling for closing services
        if db_service:
            try:
                db_service.close()
            except Exception as e:
                logger.error(f"Error closing database service: {e}")

        if redis_service:
            try:
                redis_service.close()
            except Exception as e:
                logger.error(f"Error closing Redis service: {e}")


# -------------------- STATUS ENDPOINTS --------------------


@status_router.get("/", response_model=StatusResponse)
async def get_status():
    """Get the current status of the ArbiRich API."""
    try:
        # Get trading system status
        trading_service = get_trading_service()
        trading_status = await trading_service.get_status()

        # Count active strategies and collect their names
        active_strategy_names = []
        for strategy_name, strategy_status in trading_status.get("strategies", {}).items():
            if strategy_status.get("active", False):
                active_strategy_names.append(strategy_name)

        # Get component statuses
        components = {
            "reporting": "active" if trading_status.get("reporting", False) else "inactive",
            "ingestion": "active" if trading_status.get("ingestion", False) else "inactive",
            "detection": "active" if trading_status.get("detection", False) else "inactive",
            "execution": "active" if trading_status.get("execution", False) else "inactive",
        }

        # Get active pairs and exchanges (if available in trading status)
        active_pairs = trading_status.get("active_pairs", [])
        active_exchanges = trading_status.get("active_exchanges", [])

        # Get trading timing information (if available)
        trading_start_time = trading_status.get("start_time")
        trading_stop_time = trading_status.get("stop_time")
        trading_stop_reason = trading_status.get("stop_reason")
        trading_stop_emergency = (
            trading_status.get("emergency_stop", False) if trading_status.get("stop_time") else None
        )

        # Include process information for each component
        processes = []
        process_id = 12340  # Base PID for display purposes

        for component_name, status in components.items():
            processes.append(
                {
                    "name": component_name.capitalize(),
                    "status": "Running" if status == "active" else "Stopped",
                    "pid": str(process_id),
                    "memory_usage": f"{5 + len(processes) * 2}MB",  # Placeholder
                    "cpu_usage": f"{2 + len(processes)}%",  # Placeholder
                }
            )
            process_id += 1

        # Add active strategies as processes
        for strategy_name, strategy_status in trading_status.get("strategies", {}).items():
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

        return {
            "status": "operational" if trading_status.get("overall", False) else "degraded",
            "version": os.getenv("VERSION", "0.1.0"),
            "timestamp": datetime.now().isoformat(),
            "environment": os.getenv("ENV", "development"),
            "components": components,
            "processes": processes,  # Include processes information
            "active_strategies": active_strategy_names,
            "active_pairs": active_pairs,
            "active_exchanges": active_exchanges,
            "active_trading": trading_status.get("overall", False),
            "trading_start_time": trading_start_time,
            "trading_stop_time": trading_stop_time,
            "trading_stop_reason": trading_stop_reason,
            "trading_stop_emergency": trading_stop_emergency,
        }
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        return {
            "status": "error",
            "version": os.getenv("VERSION", "0.1.0"),
            "timestamp": datetime.now().isoformat(),
            "environment": os.getenv("ENV", "development"),
            "components": {"error": str(e)},
            "processes": [],  # Empty processes list on error
            "active_strategies": [],
            "active_pairs": [],
            "active_exchanges": [],
            "active_trading": False,
            "trading_start_time": None,
            "trading_stop_time": None,
            "trading_stop_reason": "error",
            "trading_stop_emergency": None,
        }


@status_router.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint to verify API, Redis, and PostgreSQL connectivity
    """
    status = {"api": "healthy", "redis": "unknown", "database": "unknown"}

    # Try to connect to Redis
    try:
        redis_host = os.getenv("REDIS_HOST", "localhost")
        redis_port = int(os.getenv("REDIS_PORT", 6379))
        redis_client = redis.Redis(host=redis_host, port=redis_port, db=0)
        redis_client.ping()
        status["redis"] = "healthy"
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        status["redis"] = "unhealthy"

    # Try to connect to PostgreSQL
    try:
        db_host = os.getenv("POSTGRES_HOST", "localhost")
        db_port = os.getenv("POSTGRES_PORT", "5432")
        db_name = os.getenv("POSTGRES_DB", "arbirich_db")
        db_user = os.getenv("POSTGRES_USER", "arbiuser")
        db_password = os.getenv("POSTGRES_PASSWORD", "postgres")

        conn = psycopg2.connect(host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        status["database"] = "healthy"
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        status["database"] = "unhealthy"

    # If any component is unhealthy, return a 503 status
    if "unhealthy" in status.values():
        raise HTTPException(status_code=503, detail=status)

    return status


@status_router.get("/channels")
async def get_channel_status():
    """Get diagnostic information about Redis channels and subscribers."""
    try:
        # Run diagnostics and return the result
        return log_channel_diagnostics()
    except Exception as e:
        logger.error(f"Error getting channel diagnostics: {e}")
        raise HTTPException(status_code=500, detail="Error getting channel diagnostics")


# -------------------- TRADING ENDPOINTS --------------------


@trading_router.get("/status")
async def get_trading_status():
    """Get the current status of the trading system"""
    trading_service = get_trading_service()
    status = await trading_service.get_status()
    return status


@trading_router.post("/start", response_model=TradingStatusResponse)
async def start_trading():
    """Start all trading components with activation"""
    try:
        trading_service = get_trading_service()
        result = await trading_service.start_all(activate_strategies=False)
        return TradingStatusResponse(
            success=result,
            message="Trading system started successfully" if result else "Failed to start trading system",
        )
    except Exception as e:
        logger.error(f"Error starting trading system: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to start trading: {str(e)}"
        )


@trading_router.post("/stop", response_model=TradingStatusResponse)
async def stop_trading(request: Request, stop_request: TradingStopRequest = None):
    """Stop all trading activities."""
    logger.info("Stop trading requested")

    # Default to non-emergency if no request body is provided
    emergency = stop_request.emergency if stop_request else False

    # Check for emergency header
    if request.headers.get("X-Emergency-Shutdown") == "true":
        emergency = True
        logger.warning("EMERGENCY SHUTDOWN REQUESTED!")

    try:
        # Stop all components
        from src.arbirich.core.state.system_state import mark_system_shutdown
        from src.arbirich.core.trading.bytewax_flows.ingestion.ingestion_source import disable_processor_startup
        from src.arbirich.services.exchange_processors.registry import set_processors_shutting_down

        # Set shutdown flags
        set_processors_shutting_down(True)
        disable_processor_startup()

        mark_system_shutdown(True)
        if emergency:
            logger.warning("Emergency shutdown completed")

        # All components stopped successfully
        logger.info("Successfully stopped all trading components via router")

        return TradingStatusResponse(
            status="shutdown",
            message="Trading system shutdown successfully" + (" (EMERGENCY)" if emergency else ""),
            overall=False,
            success=True,
            components={
                "ingestion": "inactive",
                "detection": "inactive",
                "execution": "inactive",
                "reporting": "inactive",
            },
        )
    except Exception as e:
        logger.error(f"Error stopping trading system via router: {e}", exc_info=True)
        return TradingStatusResponse(
            status="error",
            message=f"Error stopping trading system: {str(e)}",
            overall=None,
            success=False,
            components=None,
        )


@trading_router.post("/restart", response_model=Dict[str, Any])
async def restart_trading():
    """Restart all trading components with clean state."""
    try:
        trading_service = get_trading_service()
        result = await trading_service.restart_all_components()
        return result
    except Exception as e:
        logger.error(f"Error restarting trading components: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to restart trading components: {str(e)}")


@trading_router.post("/configure/{component_name}")
async def configure_component(
    component_name: str,
    config: Dict[str, Any],
):
    """Update configuration for a specific component with validation."""
    try:
        # Validate the configuration
        errors = ConfigModelRegistry.validate_component_config(component_name, config)
        if errors:
            return JSONResponse(status_code=400, content={"status": "error", "errors": errors})

        # Get the trading service
        trading_service = get_trading_service()

        # Set the new configuration (assuming trading_service has a method to do this)
        if hasattr(trading_service, "set_component_config"):
            await trading_service.set_component_config(component_name, config)
            return {"status": "success", "message": f"Configuration updated for {component_name}"}
        else:
            raise HTTPException(status_code=501, detail="Configuration update not implemented")
    except Exception as e:
        logger.error(f"Error configuring component {component_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to configure component: {str(e)}")


# -------------------- STRATEGY ENDPOINTS --------------------


@strategies_router.get("/", response_model=List[Dict[str, Any]])
async def get_strategies(services: Tuple[DatabaseService, RedisService] = Depends(get_services)):
    """Get all available strategies with status"""
    db_service, _ = services

    # Get strategies from database
    strategy_names = get_all_strategy_names()
    strategies = []

    # Get strategy status from trading service
    trading_service = get_trading_service()
    trading_status = await trading_service.get_status()
    strategy_statuses = trading_status.get("strategies", {})

    for name in strategy_names:
        # Get full config with defaults
        config = get_strategy_config(name)

        # Get actual database strategy for active status
        db_strategy = db_service.get_strategy_by_name(name)

        if db_strategy:
            # Get status from trading service if available
            is_active = db_strategy.is_active
            arbitrage_running = False
            execution_running = False

            if name in strategy_statuses:
                is_active = strategy_statuses[name].get("active", is_active)
                arbitrage_running = strategy_statuses[name].get("arbitrage_running", False)
                execution_running = strategy_statuses[name].get("execution_running", False)

            # Create strategy info object with both config and DB data
            strategy_info = {
                "id": db_strategy.id,
                "name": db_strategy.name,
                "is_active": is_active,
                "arbitrage_running": arbitrage_running,
                "execution_running": execution_running,
                "starting_capital": db_strategy.starting_capital,
                "min_spread": db_strategy.min_spread,
                "total_profit": db_strategy.total_profit,
                "total_loss": db_strategy.total_loss,
                "net_profit": db_strategy.net_profit,
                "trade_count": db_strategy.trade_count,
                # Add fields from config
                "type": config.get("type", "unknown"),
                "exchanges": config.get("exchanges", []),
                "pairs": config.get("pairs", []),
                "additional_info": config.get("additional_info", {}),
            }
            strategies.append(strategy_info)

    return strategies


@strategies_router.get("/{strategy_name}", response_model=Strategy)
async def get_strategy(
    strategy_name: str = Path(..., description="The name of the strategy to retrieve"),
    db_gen: DatabaseService = Depends(get_db),
):
    """Get detailed information about a specific strategy."""
    try:
        # Extract the database service from the generator
        db = next(db_gen)

        strategy = db.get_strategy_by_name(strategy_name)
        if not strategy:
            raise HTTPException(status_code=404, detail=f"Strategy '{strategy_name}' not found")
        return strategy
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching strategy '{strategy_name}': {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve strategy")


@strategies_router.post("/", response_model=Strategy)
async def create_strategy(
    strategy: BasicStrategyConfig,
    db_gen: DatabaseService = Depends(get_db),
):
    """Create a new strategy with validated configuration."""
    try:
        # Extract the database service from the generator
        db = next(db_gen)

        # Ensure the strategy doesn't already exist
        existing = db.get_strategy_by_name(strategy.name)
        if existing:
            raise HTTPException(status_code=400, detail=f"Strategy '{strategy.name}' already exists")

        # Convert to Strategy model
        db_strategy = Strategy(
            name=strategy.name,
            starting_capital=float(strategy.get("starting_capital", 1000.0)),
            min_spread=float(strategy.threshold),  # Use the threshold from Pydantic model
            additional_info=strategy.model_dump(exclude={"name", "threshold"}),
            is_active=False,  # New strategies are inactive by default
        )

        # Create the strategy in the database
        created_strategy = db.create_strategy(db_strategy)
        return created_strategy
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating strategy: {e}")
        raise HTTPException(status_code=500, detail="Failed to create strategy")


@strategies_router.post("/{strategy_name}/start", response_model=TradingStatusResponse)
async def start_strategy(strategy_name: str, services: Tuple[DatabaseService, RedisService] = Depends(get_services)):
    """Start a specific strategy"""
    try:
        db_service, redis_service = services
        strategy = db_service.get_strategy_by_name(strategy_name)

        if not strategy:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Strategy '{strategy_name}' not found")

        # Update database first
        db_service.update_strategy_status_by_name(strategy_name, is_active=True)

        # Start the strategy in trading service
        trading_service = get_trading_service()
        result = await trading_service.start_strategy(strategy_name)

        # Record metric in Redis
        redis_service.record_performance_metric(
            f"strategy_{strategy_name}_status",
            1,  # 1 = active
        )

        return TradingStatusResponse(
            success=True, message=f"Strategy '{strategy_name}' started successfully", data=result
        )
    except HTTPException:
        raise  # Re-raise HTTP exceptions
    except Exception as e:
        logger.error(f"Error starting strategy '{strategy_name}': {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to start strategy: {str(e)}"
        )


@strategies_router.post("/{strategy_name}/stop", response_model=TradingStatusResponse)
async def stop_strategy(strategy_name: str, services: Tuple[DatabaseService, RedisService] = Depends(get_services)):
    """Stop a specific strategy"""
    try:
        db_service, redis_service = services
        strategy = db_service.get_strategy_by_name(strategy_name)

        if not strategy:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Strategy '{strategy_name}' not found")

        # Update database first
        db_service.update_strategy_status_by_name(strategy_name, is_active=False)

        # Stop the strategy in trading service
        trading_service = get_trading_service()
        result = await trading_service.stop_strategy(strategy_name)

        # Record metric in Redis
        redis_service.record_performance_metric(
            f"strategy_{strategy_name}_status",
            0,  # 0 = inactive
        )

        return TradingStatusResponse(
            success=True, message=f"Strategy '{strategy_name}' stopped successfully", data=result
        )
    except HTTPException:
        raise  # Re-raise HTTP exceptions
    except Exception as e:
        logger.error(f"Error stopping strategy '{strategy_name}': {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to stop strategy: {str(e)}"
        )


@strategies_router.put("/{strategy_name}/activate")
async def activate_strategy(
    strategy_name: str = Path(..., description="The name of the strategy to activate"),
    db_gen: DatabaseService = Depends(get_db),
):
    """Activate a strategy in the database and update relevant exchanges and pairs."""
    try:
        # Extract the database service from the generator
        db = next(db_gen)

        success = db.update_strategy_status_by_name(strategy_name, is_active=True)
        if not success:
            raise HTTPException(status_code=404, detail=f"Strategy '{strategy_name}' not found")

        # Update exchanges and pairs to active
        strategy_config = get_strategy_config(strategy_name)
        exchanges = strategy_config.get("exchanges", [])
        pairs = strategy_config.get("pairs", [])

        for exchange in exchanges:
            db.set_exchange_active(exchange, True)

        for pair in pairs:
            db.set_pair_active(pair, True)

        return {"status": "success", "message": f"Strategy '{strategy_name}' activated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error activating strategy '{strategy_name}': {e}")
        raise HTTPException(status_code=500, detail="Failed to activate strategy")


@strategies_router.put("/{strategy_name}/deactivate")
async def deactivate_strategy(
    strategy_name: str = Path(..., description="The name of the strategy to deactivate"),
    db_gen: DatabaseService = Depends(get_db),
):
    """Deactivate a strategy in the database and update relevant exchanges and pairs."""
    try:
        # Extract the database service from the generator
        db = next(db_gen)

        success = db.deactivate_strategy_by_name(strategy_name)
        if not success:
            raise HTTPException(status_code=404, detail=f"Strategy '{strategy_name}' not found")

        # Update exchanges and pairs to inactive if no other strategies are using them
        strategy_config = get_strategy_config(strategy_name)
        exchanges = strategy_config.get("exchanges", [])
        pairs = strategy_config.get("pairs", [])

        for exchange in exchanges:
            if not db.is_exchange_in_use(exchange):
                db.set_exchange_active(exchange, False)

        for pair in pairs:
            if not db.is_pair_in_use(pair):
                db.set_pair_active(pair, False)

        return {"status": "success", "message": f"Strategy '{strategy_name}' deactivated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deactivating strategy '{strategy_name}': {e}")
        raise HTTPException(status_code=500, detail="Failed to deactivate strategy")


@strategies_router.get("/{strategy_name}/recalculate-metrics", response_model=Dict[str, Any])
async def api_recalculate_metrics(strategy_name: str, period_days: int = 30, db_gen: DatabaseService = Depends(get_db)):
    """Recalculate metrics for a strategy via API."""
    try:
        # Extract the database service from the generator
        db = next(db_gen)

        strategy = db.get_strategy_by_name(strategy_name)
        if not strategy:
            raise HTTPException(status_code=404, detail=f"Strategy '{strategy_name}' not found")

        # Calculate metrics
        from arbirich.services.metrics.strategy_metrics_service import StrategyMetricsService

        metrics_service = StrategyMetricsService(db)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=period_days)

        metrics = metrics_service.calculate_strategy_metrics(db.session, strategy.id, start_date, end_date)

        return {
            "success": True,
            "message": f"Metrics calculated successfully for {strategy_name}",
            "period_days": period_days,
            "metrics": metrics,
        }

    except Exception as e:
        logger.error(f"Error recalculating metrics for strategy '{strategy_name}': {e}")
        raise HTTPException(status_code=500, detail=f"Failed to recalculate metrics: {str(e)}")


@strategies_router.get("/api/metrics/{strategy_id}")
async def get_strategy_metrics_api(
    strategy_id: int,
    period: str = Query("7d", description="Time period for metrics (1d, 7d, 30d, 90d, all)"),
    metrics_service: StrategyMetricsService = Depends(get_metrics_service),
):
    """API endpoint to get metrics for a strategy in JSON format."""
    try:
        # Use the shared helper function
        from arbirich.services.metrics.metrics_helper import calculate_period_dates, format_metrics_for_api

        # Get time period
        start_date, end_date, _ = calculate_period_dates(period)

        # Get metrics for the period
        metrics = metrics_service.get_metrics_for_period(strategy_id, start_date, end_date)

        if not metrics:
            return {"status": "success", "data": None, "message": "No metrics found for this period"}

        # Convert metrics to dictionary format for JSON response using the helper
        metrics_data = format_metrics_for_api(metrics)

        return {"status": "success", "data": metrics_data, "count": len(metrics_data)}
    except Exception as e:
        logger.error(f"Error in strategy metrics API: {e}", exc_info=True)
        return JSONResponse(
            status_code=500, content={"status": "error", "message": f"Error retrieving metrics: {str(e)}"}
        )


# -------------------- OPPORTUNITIES & EXECUTIONS ENDPOINTS --------------------


@trading_router.get("/opportunities", response_model=List[Dict[str, Any]])
async def get_recent_opportunities(
    count: int = 10,
    strategy_name: Optional[str] = None,
    services: Tuple[DatabaseService, RedisService] = Depends(get_services),
):
    """Get recent trade opportunities"""
    db_service, _ = services

    # Use the db_service instance to call the method
    opportunities = db_service.get_recent_opportunities(count, strategy_name)
    return opportunities


@trading_router.get("/executions", response_model=List[Dict[str, Any]])
async def get_recent_executions(
    count: int = 10,
    strategy_name: Optional[str] = None,
    services: Tuple[DatabaseService, RedisService] = Depends(get_services),
):
    """Get recent trade executions"""
    db_service, _ = services

    # Use the db_service to get executions from the database
    executions = db_service.get_recent_executions(count, strategy_name)
    return executions


# -------------------- DASHBOARD ENDPOINTS --------------------


@dashboard_router.get("/stats", response_model=DashboardStats)
async def get_dashboard_stats():
    """Get dashboard statistics."""
    try:
        with DatabaseService() as db_service:
            stats = DashboardStats()

            # Collect executions from all strategies
            logger.info("Collecting executions by strategy for dashboard stats")
            executions = []
            try:
                # Get all strategies
                strategies = db_service.get_all_strategies()
                # Get executions for each strategy
                for strategy in strategies:
                    strategy_executions = db_service.get_executions_by_strategy(strategy.name)
                    executions.extend(strategy_executions)
                logger.info(f"Collected {len(executions)} executions across all strategies")
            except Exception as e:
                logger.error(f"Error collecting executions: {e}")

            # Calculate total profit and trades
            if executions:
                for execution in executions:
                    # Convert Decimal values to float before adding
                    try:
                        buy_price = float(execution.executed_buy_price)
                        sell_price = float(execution.executed_sell_price)
                        volume = float(execution.volume)

                        profit = (sell_price - buy_price) * volume
                        stats.total_profit += profit
                        stats.total_trades += 1
                    except (TypeError, AttributeError) as e:
                        logger.warning(f"Skipping execution due to type error: {e}")
                        continue

            # Calculate win rate
            winning_trades = 0
            for e in executions:
                try:
                    if float(e.executed_sell_price) > float(e.executed_buy_price):
                        winning_trades += 1
                except (TypeError, AttributeError):
                    continue

            stats.win_rate = (winning_trades / stats.total_trades * 100) if stats.total_trades > 0 else 0

            # Get recent executions (last 24 hours)
            now = datetime.now()
            yesterday = now - timedelta(days=1)

            # Fixed comparison between timestamp and datetime
            stats.executions_24h = 0
            for e in executions:
                try:
                    # Check if execution_timestamp exists and is a datetime object
                    if hasattr(e, "execution_timestamp") and e.execution_timestamp:
                        # Handle different timestamp formats
                        if isinstance(e.execution_timestamp, (int, float)):
                            # Convert Unix timestamp to datetime
                            exec_time = datetime.fromtimestamp(e.execution_timestamp)
                        else:
                            # Already a datetime object
                            exec_time = e.execution_timestamp

                        # Now compare datetime objects
                        if exec_time > yesterday:
                            stats.executions_24h += 1
                except Exception as e:
                    logger.warning(f"Error processing execution timestamp: {e}")
                    continue

            # Get opportunities count
            from src.arbirich.models.schema import trade_opportunities

            with db_service.engine.begin() as conn:
                import sqlalchemy as sa

                result = conn.execute(sa.select(sa.func.count()).select_from(trade_opportunities))
                stats.opportunities_count = result.scalar() or 0

            # Get active strategies
            strategies = db_service.get_all_strategies()
            stats.active_strategies = sum(1 for s in strategies if getattr(s, "is_active", False))

            return stats
    except Exception as e:
        logger.error(f"Error getting dashboard stats: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving dashboard statistics")


@dashboard_router.get("/profit-chart", response_model=ChartData)
async def get_profit_chart():
    """Get profit chart data."""
    try:
        with DatabaseService() as db_service:
            # Collect executions from all strategies instead of using get_all_executions
            logger.info("Collecting executions for profit chart")
            executions = []
            try:
                # Get all strategies
                strategies = db_service.get_all_strategies()
                # Get executions for each strategy
                for strategy in strategies:
                    strategy_executions = db_service.get_executions_by_strategy(strategy.name)
                    executions.extend(strategy_executions)
                logger.info(f"Collected {len(executions)} executions for profit chart")
            except Exception as e:
                logger.error(f"Error collecting executions for profit chart: {e}")

            # Group executions by day and calculate daily profit
            daily_profits = {}
            for execution in executions:
                if not execution.execution_timestamp:
                    continue

                # Convert to datetime if needed
                timestamp = execution.execution_timestamp
                if isinstance(timestamp, (int, float)):
                    timestamp = datetime.fromtimestamp(timestamp)

                # Get the date part only
                date_key = timestamp.date()

                # Calculate profit
                try:
                    buy_price = float(execution.executed_buy_price)
                    sell_price = float(execution.executed_sell_price)
                    volume = float(execution.volume)
                    profit = (sell_price - buy_price) * volume
                except (TypeError, AttributeError):
                    logger.warning("Skipping execution with invalid price/volume data")
                    continue

                # Add to daily profits
                if date_key in daily_profits:
                    daily_profits[date_key] += profit
                else:
                    daily_profits[date_key] = profit

            # Sort dates
            sorted_dates = sorted(daily_profits.keys())

            # Fill in missing dates
            if sorted_dates:
                start_date = sorted_dates[0]
                end_date = sorted_dates[-1]
                current_date = start_date

                complete_profits = {}
                while current_date <= end_date:
                    if current_date in daily_profits:
                        complete_profits[current_date] = daily_profits[current_date]
                    else:
                        complete_profits[current_date] = 0
                    current_date += timedelta(days=1)

                # Update daily_profits
                daily_profits = complete_profits
                sorted_dates = sorted(daily_profits.keys())

            # Format for chart
            labels = [date.strftime("%Y-%m-%d") for date in sorted_dates]
            daily_values = [daily_profits[date] for date in sorted_dates]

            # Calculate cumulative profit
            cumulative_profit = []
            running_total = 0
            for profit in daily_values:
                running_total += profit
                cumulative_profit.append(running_total)

            # Create chart data
            chart_data = ChartData(
                labels=labels,
                datasets=[
                    ChartDataset(
                        label="Cumulative Profit",
                        data=cumulative_profit,
                        borderColor="#3abe78",
                        backgroundColor="rgba(58, 190, 120, 0.1)",
                        fill=True,
                    ),
                    ChartDataset(
                        label="Daily Profit",
                        data=daily_values,
                        borderColor="#85bb65",
                        backgroundColor="rgba(133, 187, 101, 0.1)",
                        fill=True,
                    ),
                ],
            )

            return chart_data
    except Exception as e:
        logger.error(f"Error getting profit chart data: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving profit chart data")


# -------------------- MARKET DATA ENDPOINTS --------------------


@market_router.get("/{exchange}/{symbol}")
async def get_market_data(
    exchange: str, symbol: str, services: Tuple[DatabaseService, RedisService] = Depends(get_services)
):
    """Get current market data for a trading pair on an exchange"""
    _, redis_service = services

    order_book = redis_service.get_order_book(exchange, symbol)
    if not order_book:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No data found for {exchange}:{symbol}")

    # Return formatted order book data
    return {
        "exchange": order_book.exchange,
        "symbol": order_book.symbol,
        "timestamp": order_book.timestamp,
        "best_bid": order_book.get_best_bid(),
        "best_ask": order_book.get_best_ask(),
        "mid_price": order_book.get_mid_price(),
        "spread": order_book.get_spread(),
        "spread_percentage": order_book.get_spread_percentage(),
    }


@trading_router.get("/metrics/{metric_name}", response_model=List[Dict[str, Any]])
async def get_metrics(
    metric_name: str, count: int = 60, services: Tuple[DatabaseService, RedisService] = Depends(get_services)
):
    """Get performance metrics"""
    _, redis_service = services

    metrics = redis_service.get_recent_metrics(metric_name, count)
    return metrics


# -------------------- TRADING PAIRS ENDPOINTS --------------------


@trading_pairs_router.get("/", response_model=List[Dict[str, Any]])
async def get_trading_pairs(db_gen: DatabaseService = Depends(get_db)):
    """Get all trading pairs."""
    try:
        # Extract the database service from the generator
        db = next(db_gen)

        pairs = db.get_all_pairs()
        return [pair.model_dump() for pair in pairs]
    except Exception as e:
        logger.error(f"Error fetching pairs: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve pairs")


@trading_pairs_router.get("/{symbol}", response_model=Dict[str, Any])
async def get_trading_pair(symbol: str, db_gen: DatabaseService = Depends(get_db)):
    """Get details of a specific trading pair."""
    try:
        # Extract the database service from the generator
        db = next(db_gen)

        pair = db.get_pair_by_symbol(symbol)
        if not pair:
            raise HTTPException(status_code=404, detail=f"Pair '{symbol}' not found")
        return pair.model_dump()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching pair '{symbol}': {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve pair")


# -------------------- EXCHANGES ENDPOINTS --------------------


@exchanges_router.get("/", response_model=List[Dict[str, Any]])
async def get_exchanges(db_gen: DatabaseService = Depends(get_db)):
    """Get all exchanges."""
    try:
        # Extract the database service from the generator
        db = next(db_gen)

        exchanges = db.get_all_exchanges()
        return [exchange.model_dump() for exchange in exchanges]
    except Exception as e:
        logger.error(f"Error fetching exchanges: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve exchanges")


@exchanges_router.get("/{name}", response_model=Dict[str, Any])
async def get_exchange(name: str, db_gen: DatabaseService = Depends(get_db)):
    """Get details of a specific exchange."""
    try:
        # Extract the database service from the generator
        db = next(db_gen)

        exchange = db.get_exchange_by_name(name)
        if not exchange:
            raise HTTPException(status_code=404, detail=f"Exchange '{name}' not found")
        return exchange.model_dump()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching exchange '{name}': {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve exchange")


# -------------------- MONITOR ENDPOINTS --------------------


@monitor_api_router.get("/system")
async def get_monitor_system():
    """Get system monitoring data."""
    from src.arbirich.web.controllers.monitor_controller import get_system_status

    return await get_system_status()


@monitor_api_router.get("/processes")
async def get_monitor_processes():
    """Get process monitoring data."""
    from src.arbirich.web.controllers.monitor_controller import get_processes

    try:
        return await get_processes()
    except Exception as e:
        logger.error(f"Error retrieving process data: {e}")
        # Return a useful error response
        return [{"name": "Error loading processes", "status": "Error", "pid": "N/A"}]


@monitor_api_router.get("/exchanges")
async def get_monitor_exchanges(db_gen: DatabaseService = Depends(get_db)):
    """Get exchange status monitoring data."""
    from src.arbirich.web.controllers.monitor_controller import get_exchange_status

    # Extract the database service from the generator
    db = next(db_gen)
    return await get_exchange_status(db)


@monitor_api_router.get("/activity")
async def get_monitor_activity():
    """Get trading activity monitoring data."""
    from src.arbirich.web.controllers.monitor_controller import get_trading_activity

    return await get_trading_activity()


@monitor_api_router.get("/trading-activity")
async def get_monitor_trading_activity():
    """Get trading activity monitoring data."""
    from src.arbirich.web.controllers.monitor_controller import get_trading_activity

    try:
        return await get_trading_activity()
    except Exception as e:
        logger.error(f"Error retrieving trading activity data: {e}")
        # Return a useful error response with empty chart data to prevent client-side errors
        return {
            "labels": [],
            "datasets": [
                {
                    "label": "Opportunities Found",
                    "data": [],
                    "borderColor": "#85bb65",
                    "backgroundColor": "rgba(133, 187, 101, 0.1)",
                },
                {
                    "label": "Trades Executed",
                    "data": [],
                    "borderColor": "#3abe78",
                    "backgroundColor": "rgba(58, 190, 120, 0.1)",
                },
            ],
        }


# -------------------- ROUTER REGISTRATION --------------------

# Register all sub-routers with the main router
main_router.include_router(status_router)
main_router.include_router(trading_router)
main_router.include_router(strategies_router)
main_router.include_router(dashboard_router)
main_router.include_router(market_router)
main_router.include_router(trading_pairs_router)
main_router.include_router(exchanges_router)
main_router.include_router(monitor_api_router)
main_router.include_router(strategy_router)


# Root endpoint
@main_router.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Welcome to ArbiRich API",
        "version": os.getenv("VERSION", "0.1.0"),
        "docs": "/docs",
    }
