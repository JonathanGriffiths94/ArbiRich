import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from arbirich.services.metrics.strategy_metrics_service import StrategyMetricsService
from src.arbirich.services.database.database_service import DatabaseService
from src.arbirich.web.dependencies import get_db_service

logger = logging.getLogger(__name__)
router = APIRouter()

base_dir = Path(__file__).resolve().parent.parent
templates_dir = base_dir / "templates"
logger.info(f"Strategy routes templates directory: {templates_dir}")
templates = Jinja2Templates(directory=str(templates_dir))


@router.get("/strategies", response_class=HTMLResponse)
async def get_strategies(request: Request, db_gen: DatabaseService = Depends(get_db_service)):
    """Get all strategies with their metrics."""
    try:
        # Extract the database service from the generator
        db = next(db_gen)

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
            "pages/strategies.html",
            {"request": request, "strategies": strategies, "strategy_metrics": strategy_metrics},
        )
    except Exception as e:
        logger.error(f"Error in get_strategies: {e}", exc_info=True)
        return templates.TemplateResponse(
            "errors/error.html", {"request": request, "error_message": f"Error loading strategies: {str(e)}"}
        )


@router.get("/strategy/{strategy_name}", response_class=HTMLResponse)
async def get_strategy_detail(request: Request, strategy_name: str, db_gen: DatabaseService = Depends(get_db_service)):
    """Get detailed view for a specific strategy."""
    try:
        # Extract the database service from the generator
        db = next(db_gen)

        strategy = db.get_strategy_by_name(strategy_name)
        if not strategy:
            return templates.TemplateResponse(
                "errors/404.html", {"request": request, "message": f"Strategy '{strategy_name}' not found"}
            )

        # Get executions for this strategy
        executions = db.get_executions_by_strategy(strategy_name)

        # Sort executions by timestamp (newest first)
        executions = sorted(executions, key=lambda x: x.execution_timestamp, reverse=True)

        return templates.TemplateResponse(
            "pages/strategy_detail.html", {"request": request, "strategy": strategy, "executions": executions}
        )
    except Exception as e:
        logger.error(f"Error getting strategy detail: {e}")
        return templates.TemplateResponse(
            "errors/error.html", {"request": request, "error_message": f"Error loading strategy: {str(e)}"}
        )


@router.get("/strategy/{strategy_name}/metrics", response_class=HTMLResponse)
async def get_strategy_metrics(
    request: Request,
    strategy_name: str,
    period: Optional[str] = None,
    db_gen: DatabaseService = Depends(get_db_service),
):
    """Get metrics view for a specific strategy."""
    try:
        # Extract the database service from the generator
        db = next(db_gen)

        # Get all strategies
        strategies = db.get_all_strategies()

        # Find the strategy by name
        strategy = next((s for s in strategies if s.name == strategy_name), None)
        if not strategy:
            logger.error(f"Strategy {strategy_name} not found")
            return templates.TemplateResponse(
                "errors/error.html",
                {
                    "request": request,
                    "error_message": f"Strategy '{strategy_name}' not found",
                    "back_url": "/strategies",
                },
            )

        # Get strategy metrics
        metrics_service = StrategyMetricsService(db)
        metrics = metrics_service.get_latest_metrics_for_strategy(strategy.id)

        # Get trading pair and exchange metrics
        pair_metrics = []
        exchange_metrics = []
        if metrics:
            if hasattr(metrics, "trading_pair_metrics"):
                pair_metrics = metrics.trading_pair_metrics
            if hasattr(metrics, "exchange_metrics"):
                exchange_metrics = metrics.exchange_metrics

        # Get executions
        executions = db.get_executions_by_strategy(strategy_name)
        executions.sort(key=lambda x: x.execution_timestamp, reverse=True)

        # Parse period parameter for metrics view
        metrics_period = period or "30d"

        context = {
            "request": request,
            "strategy": strategy,
            "metrics": metrics,
            "pair_metrics": pair_metrics,
            "exchange_metrics": exchange_metrics,
            "executions": executions[:10],  # Show only the 10 most recent
            "period": metrics_period,
        }

        logger.info(f"Rendering strategy metrics template for: {strategy_name}")
        return templates.TemplateResponse("pages/strategy_metrics.html", context)

    except Exception as e:
        logger.error(f"Error getting strategy metrics for {strategy_name}: {e}", exc_info=True)
        return templates.TemplateResponse(
            "errors/error.html",
            {
                "request": request,
                "error_message": f"Error getting strategy metrics: {str(e)}",
                "back_url": "/strategies",
            },
        )


@router.get("/strategy/{strategy_name}/recalculate-metrics")
async def recalculate_strategy_metrics(
    request: Request,
    strategy_name: str,
    period_days: int = Query(30, description="Number of days to include in calculation", ge=1, le=365),
    db_gen: DatabaseService = Depends(get_db_service),
):
    """Recalculate metrics for a strategy."""
    try:
        # Extract the database service from the generator
        db = next(db_gen)

        # Get all strategies
        strategies = db.get_all_strategies()

        # Find the strategy by name
        strategy = next((s for s in strategies if s.name == strategy_name), None)
        if not strategy:
            logger.error(f"Strategy {strategy_name} not found")
            return templates.TemplateResponse(
                "errors/error.html",
                {
                    "request": request,
                    "error_message": f"Strategy '{strategy_name}' not found",
                    "back_url": "/strategies",
                },
            )

        # Reuse API endpoint functionality for calculation
        metrics_service = StrategyMetricsService(db)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=period_days)

        try:
            metrics = metrics_service.calculate_strategy_metrics(db.session, strategy.id, start_date, end_date)
            logger.info(f"Metrics recalculated for {strategy_name}, period: {period_days} days")

            # Redirect to metrics page
            return RedirectResponse(url=f"/strategy/{strategy_name}/metrics")

        except Exception as e:
            logger.error(f"Error calculating metrics: {e}")
            return templates.TemplateResponse(
                "errors/error.html",
                {
                    "request": request,
                    "error_message": f"Error calculating metrics for {strategy.name}: {str(e)}",
                    "back_url": f"/strategy/{strategy_name}",
                },
            )
    except Exception as e:
        logger.error(f"Error recalculating metrics: {e}", exc_info=True)
        return templates.TemplateResponse(
            "errors/error.html",
            {
                "request": request,
                "error_message": f"Error recalculating metrics: {str(e)}",
                "back_url": f"/strategy/{strategy_name}",
            },
        )


@router.get("/admin/calculate-metrics/{strategy_name}", response_class=HTMLResponse)
async def trigger_metrics_calculation(
    request: Request, strategy_name: str, period_days: int = 7, db_gen: DatabaseService = Depends(get_db_service)
):
    """Manually trigger metrics calculation for a strategy."""
    try:
        # Extract the database service from the generator
        db = next(db_gen)

        # Find strategy by name
        strategy = next((s for s in db.get_all_strategies() if s.name == strategy_name), None)
        if not strategy:
            return templates.TemplateResponse(
                "errors/error.html",
                {
                    "request": request,
                    "error_message": f"Strategy '{strategy_name}' not found",
                    "back_url": "/strategies",
                },
            )

        metrics_service = StrategyMetricsService(db)

        # Get current time for period end
        period_end = datetime.utcnow()
        period_start = period_end - timedelta(days=period_days)

        # Use the session and strategy ID from the found strategy
        session = db.session
        metrics = metrics_service.calculate_strategy_metrics(session, strategy.id, period_start, period_end)

        if metrics:
            # Redirect to strategy page using the strategy name
            return RedirectResponse(url=f"/strategy/{strategy_name}/metrics")
        else:
            return templates.TemplateResponse(
                "errors/error.html",
                {
                    "request": request,
                    "error_message": f"No executions found for strategy '{strategy_name}' in the last {period_days} days",
                    "back_url": "/strategies",
                },
            )
    except Exception as e:
        logger.error(f"Error calculating metrics: {e}", exc_info=True)
        return templates.TemplateResponse(
            "errors/error.html", {"request": request, "error_message": f"Error calculating metrics: {str(e)}"}
        )
