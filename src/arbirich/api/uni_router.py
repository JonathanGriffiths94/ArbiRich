"""
Unified API Router for ArbiRich trading platform.

This module consolidates all API endpoints into a single organized structure,
including status endpoints, trading operations, dashboard data, and health monitoring.
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import redis
from fastapi import APIRouter, Depends, HTTPException, Path, status
from pydantic import BaseModel

from src.arbirich.config.config import get_all_strategy_names, get_strategy_config
from src.arbirich.models.models import Strategy
from src.arbirich.services.database.database_service import DatabaseService
from src.arbirich.services.redis.redis_service import RedisService
from src.arbirich.services.trading_service import trading_service

logger = logging.getLogger(__name__)

# Create main API router
main_router = APIRouter()

# Create sub-routers for different API sections
status_router = APIRouter(prefix="/status", tags=["status"])
trading_router = APIRouter(prefix="/trading", tags=["trading"])
strategies_router = APIRouter(prefix="/strategies", tags=["strategies"])
dashboard_router = APIRouter(prefix="/dashboard", tags=["dashboard"])
market_router = APIRouter(prefix="/market", tags=["market"])

# -------------------- MODEL DEFINITIONS --------------------


class StatusResponse(BaseModel):
    """Response model for the status endpoint."""

    status: str
    version: str
    timestamp: str
    environment: str
    components: Dict[str, str] = {}


class HealthResponse(BaseModel):
    """Response model for the health check endpoint."""

    api: str
    redis: str
    database: str


class TradingStatusResponse(BaseModel):
    """Standard response with status and message"""

    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None


class DashboardStats(BaseModel):
    """Dashboard statistics model."""

    total_profit: float = 0.0
    total_trades: int = 0
    win_rate: float = 0.0
    executions_24h: int = 0
    opportunities_count: int = 0
    active_strategies: int = 0


class ChartDataset(BaseModel):
    """Chart dataset model."""

    label: str
    data: List[float]
    borderColor: str
    backgroundColor: str
    fill: bool = False
    tension: float = 0.4


class ChartData(BaseModel):
    """Chart data model."""

    labels: List[str]
    datasets: List[ChartDataset]


# -------------------- DEPENDENCIES --------------------


def get_db():
    """Database service dependency"""
    db = DatabaseService()
    try:
        yield db
    finally:
        db.close()


async def get_services() -> Tuple[DatabaseService, RedisService]:
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
        db_service.close()
        redis_service.close()


# -------------------- STATUS ENDPOINTS --------------------


@status_router.get("/", response_model=StatusResponse)
async def get_status():
    """Get the current status of the ArbiRich API."""
    try:
        # Get trading system status
        trading_status = await trading_service.get_status()

        # Count active strategies
        active_strategies = sum(
            1
            for strategy_status in trading_status.get("strategies", {}).values()
            if strategy_status.get("active", False)
        )

        # Get component statuses
        components = {
            "database": "active" if trading_status.get("database", False) else "inactive",
            "ingestion": "active" if trading_status.get("ingestion", False) else "inactive",
            "arbitrage": "active" if trading_status.get("arbitrage", False) else "inactive",
            "execution": "active" if trading_status.get("execution", False) else "inactive",
        }

        return {
            "status": "operational" if trading_status.get("overall", False) else "degraded",
            "version": os.getenv("VERSION", "0.1.0"),
            "timestamp": datetime.now().isoformat(),
            "environment": os.getenv("ENV", "development"),
            "components": components,
        }
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        return {
            "status": "error",
            "version": os.getenv("VERSION", "0.1.0"),
            "timestamp": datetime.now().isoformat(),
            "environment": os.getenv("ENV", "development"),
            "components": {"error": str(e)},
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


# -------------------- TRADING ENDPOINTS --------------------


@trading_router.get("/status")
async def get_trading_status():
    """Get the current status of the trading system"""
    status = await trading_service.get_status()
    return status


@trading_router.post("/start", response_model=TradingStatusResponse)
async def start_trading():
    """Start all trading components with activation"""
    try:
        result = await trading_service.start_all(activate_entities=True)
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
async def stop_trading():
    """Stop all trading components"""
    try:
        await trading_service.stop_all()
        return TradingStatusResponse(success=True, message="Trading system stopped successfully")
    except Exception as e:
        logger.error(f"Error stopping trading system: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to stop trading: {str(e)}"
        )


@trading_router.post("/components/{component_name}/start", response_model=TradingStatusResponse)
async def start_component(component_name: str):
    """Start a specific component (ingestion, arbitrage, execution, database)"""
    try:
        result = await trading_service.start_component(component_name)
        return TradingStatusResponse(
            success=True, message=f"Component {component_name} started successfully", data=result
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        logger.error(f"Error starting component {component_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to start component: {str(e)}"
        )


@trading_router.post("/components/{component_name}/stop", response_model=TradingStatusResponse)
async def stop_component(component_name: str):
    """Stop a specific component (ingestion, arbitrage, execution, database)"""
    try:
        result = await trading_service.stop_component(component_name)
        return TradingStatusResponse(
            success=True, message=f"Component {component_name} stopped successfully", data=result
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        logger.error(f"Error stopping component {component_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to stop component: {str(e)}"
        )


# -------------------- STRATEGY ENDPOINTS --------------------


@strategies_router.get("/", response_model=List[Dict[str, Any]])
async def get_strategies(services: Tuple[DatabaseService, RedisService] = Depends(get_services)):
    """Get all available strategies with status"""
    db_service, _ = services

    # Get strategies from database
    strategy_names = get_all_strategy_names()
    strategies = []

    # Get strategy status from trading service
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


@strategies_router.get("/{strategy_id}", response_model=Strategy)
async def get_strategy(
    strategy_id: int = Path(..., description="The ID of the strategy to retrieve"),
    db: DatabaseService = Depends(get_db),
):
    """Get detailed information about a specific strategy."""
    try:
        strategy = db.get_strategy_by_id(strategy_id)
        if not strategy:
            raise HTTPException(status_code=404, detail=f"Strategy with ID {strategy_id} not found")
        return strategy
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching strategy {strategy_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve strategy")


@strategies_router.post("/{strategy_id}/start", response_model=TradingStatusResponse)
async def start_strategy(strategy_id: int, services: Tuple[DatabaseService, RedisService] = Depends(get_services)):
    """Start a specific strategy"""
    try:
        db_service, redis_service = services
        strategy = db_service.get_strategy_by_id(strategy_id)

        if not strategy:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Strategy with ID {strategy_id} not found"
            )

        # Update database first
        db_service.update_strategy_status(strategy_id, is_active=True)

        # Start the strategy in trading service
        result = await trading_service.start_strategy(strategy.name)

        # Record metric in Redis
        redis_service.record_performance_metric(
            f"strategy_{strategy.name}_status",
            1,  # 1 = active
        )

        return TradingStatusResponse(
            success=True, message=f"Strategy {strategy.name} started successfully", data=result
        )
    except HTTPException:
        raise  # Re-raise HTTP exceptions
    except Exception as e:
        logger.error(f"Error starting strategy {strategy_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to start strategy: {str(e)}"
        )


@strategies_router.post("/{strategy_id}/stop", response_model=TradingStatusResponse)
async def stop_strategy(strategy_id: int, services: Tuple[DatabaseService, RedisService] = Depends(get_services)):
    """Stop a specific strategy"""
    try:
        db_service, redis_service = services
        strategy = db_service.get_strategy_by_id(strategy_id)

        if not strategy:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Strategy with ID {strategy_id} not found"
            )

        # Update database first
        db_service.update_strategy_status(strategy_id, is_active=False)

        # Stop the strategy in trading service
        result = await trading_service.stop_strategy(strategy.name)

        # Record metric in Redis
        redis_service.record_performance_metric(
            f"strategy_{strategy.name}_status",
            0,  # 0 = inactive
        )

        return TradingStatusResponse(
            success=True, message=f"Strategy {strategy.name} stopped successfully", data=result
        )
    except HTTPException:
        raise  # Re-raise HTTP exceptions
    except Exception as e:
        logger.error(f"Error stopping strategy {strategy_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to stop strategy: {str(e)}"
        )


@strategies_router.put("/{strategy_id}/activate")
async def activate_strategy(
    strategy_id: int = Path(..., description="The ID of the strategy to activate"),
    db: DatabaseService = Depends(get_db),
):
    """Activate a strategy in the database."""
    try:
        success = db.activate_strategy(strategy_id)
        if not success:
            raise HTTPException(status_code=404, detail=f"Strategy with ID {strategy_id} not found")
        return {"status": "success", "message": f"Strategy with ID {strategy_id} activated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error activating strategy {strategy_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to activate strategy")


@strategies_router.put("/{strategy_id}/deactivate")
async def deactivate_strategy(
    strategy_id: int = Path(..., description="The ID of the strategy to deactivate"),
    db: DatabaseService = Depends(get_db),
):
    """Deactivate a strategy in the database."""
    try:
        success = db.deactivate_strategy(strategy_id)
        if not success:
            raise HTTPException(status_code=404, detail=f"Strategy with ID {strategy_id} not found")
        return {"status": "success", "message": f"Strategy with ID {strategy_id} deactivated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deactivating strategy {strategy_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to deactivate strategy")


# -------------------- OPPORTUNITIES & EXECUTIONS ENDPOINTS --------------------


@trading_router.get("/opportunities", response_model=List[Dict[str, Any]])
async def get_recent_opportunities(
    count: int = 10,
    strategy_name: Optional[str] = None,
    services: Tuple[DatabaseService, RedisService] = Depends(get_services),
):
    """Get recent trade opportunities"""
    _, redis_service = services

    opportunities = redis_service.get_recent_opportunities(count, strategy_name)
    return opportunities


@trading_router.get("/executions", response_model=List[Dict[str, Any]])
async def get_recent_executions(
    count: int = 10,
    strategy_name: Optional[str] = None,
    services: Tuple[DatabaseService, RedisService] = Depends(get_services),
):
    """Get recent trade executions"""
    _, redis_service = services

    executions = redis_service.get_recent_executions(count, strategy_name)
    return executions


# -------------------- DASHBOARD ENDPOINTS --------------------


@dashboard_router.get("/stats", response_model=DashboardStats)
async def get_dashboard_stats():
    """Get dashboard statistics."""
    try:
        with DatabaseService() as db:
            stats = DashboardStats()

            # Get all executions using a fallback method if get_all_executions doesn't exist
            try:
                executions = db.get_all_executions()
            except AttributeError:
                # Fallback: Collect executions from all strategies
                logger.warning("get_all_executions not available, using fallback")
                executions = []
                try:
                    # Get all strategies
                    strategies = db.get_all_strategies()
                    # Get executions for each strategy
                    for strategy in strategies:
                        strategy_executions = db.get_executions_by_strategy(strategy.name)
                        executions.extend(strategy_executions)
                except Exception as e:
                    logger.error(f"Error in fallback execution retrieval: {e}")

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
            stats.executions_24h = sum(
                1 for e in executions if e.execution_timestamp and e.execution_timestamp > yesterday
            )

            # Get opportunities count
            from src.arbirich.models.schema import trade_opportunities

            with db.engine.begin() as conn:
                import sqlalchemy as sa

                result = conn.execute(sa.select(sa.func.count()).select_from(trade_opportunities))
                stats.opportunities_count = result.scalar() or 0

            # Get active strategies
            strategies = db.get_all_strategies()
            stats.active_strategies = sum(1 for s in strategies if getattr(s, "is_active", False))

            return stats
    except Exception as e:
        logger.error(f"Error getting dashboard stats: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving dashboard statistics")


@dashboard_router.get("/profit-chart", response_model=ChartData)
async def get_profit_chart():
    """Get profit chart data."""
    try:
        with DatabaseService() as db:
            # Get all executions
            executions = db.get_all_executions()

            # Group executions by day and calculate daily profit
            daily_profits = {}
            for execution in executions:
                if not execution.execution_timestamp:
                    continue

                # Get the date part only
                date_key = execution.execution_timestamp.date()

                # Calculate profit
                profit = (execution.executed_sell_price - execution.executed_buy_price) * execution.volume

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


# -------------------- ROUTER REGISTRATION --------------------

# Register all sub-routers with the main router
main_router.include_router(status_router)
main_router.include_router(trading_router)
main_router.include_router(strategies_router)
main_router.include_router(dashboard_router)
main_router.include_router(market_router)


# Root endpoint
@main_router.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Welcome to ArbiRich API",
        "version": os.getenv("VERSION", "0.1.0"),
        "docs": "/docs",
    }
