import logging
from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from arbirich.services.strategy_metrics_service import StrategyMetricsService
from src.arbirich.models.models import StrategyExchangeMetrics, StrategyTradingPairMetrics
from src.arbirich.services.database.database_service import DatabaseService

logger = logging.getLogger(__name__)
router = APIRouter()

templates = Jinja2Templates(directory="src/arbirich/web/templates")


# Fix the dependency to return a database service instance instead of passing it directly
def get_db_service():
    """Get a database service instance."""
    db = DatabaseService()
    try:
        yield db
    finally:
        # Clean up resources if needed
        pass


@router.get("/strategies", response_class=HTMLResponse)
async def get_strategies(request: Request, db: DatabaseService = Depends(get_db_service)):
    """Get all strategies with their metrics."""
    try:
        strategies = db.get_all_strategies()

        # Create a metrics service to fetch metrics for each strategy
        metrics_service = StrategyMetricsService(db)

        # Store metrics separately for each strategy
        strategy_metrics = {}

        # Fetch metrics for each strategy
        for strategy in strategies:
            try:
                latest_metrics = metrics_service.get_latest_metrics_for_strategy(strategy.id)
                # Store metrics in the dictionary using strategy ID as key
                strategy_metrics[strategy.id] = latest_metrics
            except Exception as metrics_error:
                logger.error(f"Error fetching metrics for strategy {strategy.name}: {metrics_error}")
                # Continue with next strategy if metrics fetching fails

        # Sort strategies by profit (most profitable first)
        strategies = sorted(strategies, key=lambda s: s.net_profit, reverse=True)

        # Return the strategy list and metrics separately
        return templates.TemplateResponse(
            "strategies.html", {"request": request, "strategies": strategies, "strategy_metrics": strategy_metrics}
        )
    except Exception as e:
        logger.error(f"Error in get_strategies: {e}", exc_info=True)
        return templates.TemplateResponse(
            "error.html", {"request": request, "error_message": f"Error loading strategies: {str(e)}"}
        )


# Update the get_strategy endpoint to include metrics data
@router.get("/strategy/{strategy_name}", response_class=HTMLResponse)
async def get_strategy(request: Request, strategy_name: str, db: DatabaseService = Depends(get_db_service)):
    """Get details for a specific strategy."""
    try:
        # Get all strategies
        strategies = db.get_all_strategies()

        # Find the strategy by name
        strategy = next((s for s in strategies if s.name == strategy_name), None)
        if not strategy:
            logger.error(f"Strategy {strategy_name} not found")
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "error_message": f"Strategy '{strategy_name}' not found",
                    "back_url": "/strategies",
                },
            )

        # Get strategy metrics
        metrics_service = StrategyMetricsService(db)
        metrics = metrics_service.get_latest_metrics_for_strategy(strategy.id)

        # Get opportunities and executions
        opportunities = db.get_opportunities_by_strategy(strategy_name)
        executions = db.get_executions_by_strategy(strategy_name)

        # Sort by timestamp (newest first)
        opportunities.sort(key=lambda x: x.opportunity_timestamp, reverse=True)
        executions.sort(key=lambda x: x.execution_timestamp, reverse=True)

        # Calculate performance metrics
        total_opportunities = len(opportunities)
        total_executions = len(executions)
        execution_rate = (total_executions / total_opportunities * 100) if total_opportunities > 0 else 0

        # Group executions by pair for a basic chart
        pairs_data = {}
        for execution in executions:
            if execution.pair not in pairs_data:
                pairs_data[execution.pair] = 0
            profit = (execution.executed_sell_price - execution.executed_buy_price) * execution.volume
            pairs_data[execution.pair] += profit

        chart_labels = list(pairs_data.keys())
        chart_data = list(pairs_data.values())

        # Get any trading pair and exchange metrics if available
        pair_metrics = []
        exchange_metrics = []

        if metrics and hasattr(metrics, "trading_pair_metrics"):
            pair_metrics = metrics.trading_pair_metrics

        if metrics and hasattr(metrics, "exchange_metrics"):
            exchange_metrics = metrics.exchange_metrics

        return templates.TemplateResponse(
            "strategy.html",
            {
                "request": request,
                "strategy": strategy,
                "metrics": metrics,
                "pair_metrics": pair_metrics,
                "exchange_metrics": exchange_metrics,
                "opportunities": opportunities[:10],  # Show only the 10 most recent
                "executions": executions[:10],  # Show only the 10 most recent
                "total_opportunities": total_opportunities,
                "total_executions": total_executions,
                "execution_rate": execution_rate,
                "chart_labels": chart_labels,
                "chart_data": chart_data,
            },
        )
    except Exception as e:
        logger.error(f"Error getting strategy {strategy_name}: {e}", exc_info=True)
        return templates.TemplateResponse(
            "error.html",
            {"request": request, "error_message": f"Error getting strategy: {str(e)}", "back_url": "/strategies"},
        )


# Add a recalculate metrics endpoint that redirects back to the strategy page
@router.get("/strategy/{strategy_name}/recalculate-metrics")
async def recalculate_strategy_metrics(
    request: Request,
    strategy_name: str,
    period_days: int = Query(30, description="Number of days to include in calculation", ge=1, le=365),
    db: DatabaseService = Depends(get_db_service),
):
    """Recalculate metrics for a strategy."""
    try:
        # Get all strategies
        strategies = db.get_all_strategies()

        # Find the strategy by name
        strategy = next((s for s in strategies if s.name == strategy_name), None)
        if not strategy:
            logger.error(f"Strategy {strategy_name} not found")
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "error_message": f"Strategy '{strategy_name}' not found",
                    "back_url": "/strategies",
                },
            )

        # Calculate metrics
        metrics_service = StrategyMetricsService(db)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=period_days)

        metrics = metrics_service.calculate_strategy_metrics(db.session, strategy.id, start_date, end_date)

        if metrics:
            return templates.TemplateResponse(
                "success.html",
                {
                    "request": request,
                    "message": f"Metrics calculated successfully for {strategy.name}",
                    "redirect_url": f"/strategy/{strategy_name}",
                },
            )
        else:
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "error_message": f"No executions found for {strategy.name} in the last {period_days} days",
                    "back_url": f"/strategy/{strategy_name}",
                },
            )
    except Exception as e:
        logger.error(f"Error recalculating metrics: {e}", exc_info=True)
        return templates.TemplateResponse(
            "error.html",
            {
                "request": request,
                "error_message": f"Error recalculating metrics: {str(e)}",
                "back_url": f"/strategy/{strategy_name}",
            },
        )


# Add a route to manually trigger metrics calculation
@router.get("/admin/calculate-metrics/{strategy_id}", response_class=HTMLResponse)
async def trigger_metrics_calculation(
    request: Request, strategy_id: int, period_days: int = 7, db: DatabaseService = Depends(get_db_service)
):
    """Manually trigger metrics calculation for a strategy."""
    try:
        metrics_service = StrategyMetricsService(db)

        # Get current time for period end
        period_end = datetime.utcnow()
        period_start = period_end - timedelta(days=period_days)

        # Use the session
        session = db.session

        # Calculate metrics
        metrics = metrics_service.calculate_strategy_metrics(session, strategy_id, period_start, period_end)

        if metrics:
            return templates.TemplateResponse(
                "success.html",
                {
                    "request": request,
                    "message": f"Metrics calculated successfully for strategy ID {strategy_id}",
                    "redirect_url": f"/strategy/{metrics.strategy.name}",
                },
            )
        else:
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "error_message": f"No executions found for strategy ID {strategy_id} in the last {period_days} days",
                },
            )
    except Exception as e:
        logger.error(f"Error calculating metrics: {e}", exc_info=True)
        return templates.TemplateResponse(
            "error.html", {"request": request, "error_message": f"Error calculating metrics: {str(e)}"}
        )
