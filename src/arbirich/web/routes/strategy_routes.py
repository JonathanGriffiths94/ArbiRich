import logging
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from arbirich.services.metrics.strategy_metrics_service import StrategyMetricsService
from src.arbirich.services.database.database_service import DatabaseService

logger = logging.getLogger(__name__)
router = APIRouter()

templates = Jinja2Templates(directory="src/arbirich/web/templates")


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
            "errors/error.html", {"request": request, "error_message": f"Error loading strategies: {str(e)}"}
        )


@router.get("/strategy/{strategy_name}", response_class=HTMLResponse)
async def get_strategy_overview(
    request: Request,
    strategy_name: str,
    action: Optional[str] = None,
    period: Optional[str] = None,
    db: DatabaseService = Depends(get_db_service),
):
    """Get overview details for a specific strategy."""
    try:
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

        # Handle 'action' parameter - for example, calculating metrics
        if action == "calculate":
            # Convert period parameter to days
            days = 30  # Default
            if period == "all":
                days = 365  # Use a large number for "all time"
            elif period and period.isdigit():
                days = int(period)

            # Calculate metrics
            metrics_service = StrategyMetricsService(db)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            try:
                metrics = metrics_service.calculate_strategy_metrics(db.session, strategy.id, start_date, end_date)
                logger.info(f"Metrics calculated for {strategy_name}, period: {period}")
            except Exception as calc_error:
                logger.error(f"Error calculating metrics: {calc_error}")
                # Continue despite calculation error

            # Redirect back to the strategy page
            return RedirectResponse(url=f"/strategy/{strategy_name}")

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

        # Get executions and opportunities
        executions = db.get_executions_by_strategy(strategy_name)
        executions.sort(key=lambda x: x.execution_timestamp, reverse=True)

        # Try to get opportunities if available
        opportunities = []
        try:
            opportunities = db.get_opportunities_by_strategy(strategy_name)
            opportunities.sort(key=lambda x: x.opportunity_timestamp, reverse=True)
        except Exception as opp_error:
            logger.warning(f"Could not load opportunities: {opp_error}")

        # Common template context - add the view parameter
        context = {
            "request": request,
            "strategy": strategy,
            "metrics": metrics,
            "pair_metrics": pair_metrics,
            "exchange_metrics": exchange_metrics,
            "executions": executions[:10],  # Show only the 10 most recent
            "opportunities": opportunities[:10] if opportunities else [],
        }

        logger.info(f"Rendering strategy overview template for: {strategy_name}")
        return templates.TemplateResponse("pages/strategy.html", context)

    except Exception as e:
        logger.error(f"Error getting strategy {strategy_name}: {e}", exc_info=True)
        return templates.TemplateResponse(
            "errors/error.html",
            {"request": request, "error_message": f"Error getting strategy: {str(e)}", "back_url": "/strategies"},
        )


@router.get("/strategy/{strategy_name}/detail", response_class=HTMLResponse)
async def get_strategy_detail(request: Request, strategy_name: str, db: DatabaseService = Depends(get_db_service)):
    """Get detailed view for a specific strategy."""
    try:
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

        context = {
            "request": request,
            "strategy": strategy,
            "metrics": metrics,
            "pair_metrics": pair_metrics,
            "exchange_metrics": exchange_metrics,
            "executions": executions[:10],  # Show only the 10 most recent
        }

        logger.info(f"Rendering strategy detail template for: {strategy_name}")
        return templates.TemplateResponse("strategy_detail.html", context)

    except Exception as e:
        logger.error(f"Error getting strategy detail for {strategy_name}: {e}", exc_info=True)
        return templates.TemplateResponse(
            "errors/error.html",
            {
                "request": request,
                "error_message": f"Error getting strategy detail: {str(e)}",
                "back_url": "/strategies",
            },
        )


@router.get("/strategy/{strategy_name}/metrics", response_class=HTMLResponse)
async def get_strategy_metrics(
    request: Request, strategy_name: str, period: Optional[str] = None, db: DatabaseService = Depends(get_db_service)
):
    """Get metrics view for a specific strategy."""
    try:
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
        return templates.TemplateResponse("strategy_metrics.html", context)

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
                "errors/error.html",
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

        try:
            metrics = metrics_service.calculate_strategy_metrics(db.session, strategy.id, start_date, end_date)
            logger.info(f"Metrics recalculated for {strategy_name}, period: {period_days} days")

            # Redirect to metrics page, no need for query params anymore
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
    request: Request, strategy_name: str, period_days: int = 7, db: DatabaseService = Depends(get_db_service)
):
    """Manually trigger metrics calculation for a strategy."""
    try:
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
