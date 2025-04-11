import logging
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from src.arbirich.services.database.database_service import DatabaseService
from src.arbirich.services.metrics.strategy_metrics_service import StrategyMetricsService
from src.arbirich.utils.metrics_helper import calculate_period_dates
from src.arbirich.web.dependencies import get_db_service, get_metrics_service

logger = logging.getLogger(__name__)
router = APIRouter()

templates = Jinja2Templates(directory="src/arbirich/web/templates")


@router.get("/metrics", response_class=HTMLResponse)
async def get_metrics_dashboard(request: Request, db: DatabaseService = Depends(get_db_service)):
    """Display a dashboard of metrics for all strategies."""
    try:
        # Get all strategies
        strategies = db.get_all_strategies()

        # Sort by net profit (descending)
        strategies = sorted(strategies, key=lambda s: s.net_profit, reverse=True)

        # Create a metrics service to access the metrics
        metrics_service = StrategyMetricsService(db)

        # Get latest metrics for each strategy
        strategy_metrics = {}
        for strategy in strategies:
            metrics = metrics_service.get_latest_metrics_for_strategy(strategy.id)
            if metrics:
                strategy_metrics[strategy.id] = metrics

        # Calculate platform-wide statistics
        total_trades = sum(s.trade_count for s in strategies)
        total_profit = sum(s.net_profit for s in strategies)

        # Calculate win rate across all strategies
        win_count = sum(m.win_count for m in strategy_metrics.values() if m)
        loss_count = sum(m.loss_count for m in strategy_metrics.values() if m)
        overall_win_rate = (win_count / (win_count + loss_count) * 100) if (win_count + loss_count) > 0 else 0

        return templates.TemplateResponse(
            "pages/metrics.html",
            {
                "request": request,
                "strategies": strategies,
                "strategy_metrics": strategy_metrics,
                "total_trades": total_trades,
                "total_profit": total_profit,
                "overall_win_rate": overall_win_rate,
            },
        )
    except Exception as e:
        logger.error(f"Error in metrics dashboard: {e}", exc_info=True)
        return templates.TemplateResponse(
            "errors/error.html", {"request": request, "error_message": f"Error loading metrics dashboard: {str(e)}"}
        )


@router.get("/metrics/{strategy_id}", response_class=HTMLResponse)
async def get_strategy_metrics(
    request: Request,
    strategy_id: int,
    period: str = Query("7d", description="Time period for metrics (1d, 7d, 30d, 90d, all)"),
    db: DatabaseService = Depends(get_db_service),
    metrics_service: StrategyMetricsService = Depends(get_metrics_service),
):
    """Display detailed metrics for a specific strategy."""
    try:
        # Get the strategy
        strategy = db.get_strategy(strategy_id)
        if not strategy:
            return templates.TemplateResponse(
                "errors/error.html", {"request": request, "error_message": f"Strategy with ID {strategy_id} not found"}
            )

        # Get time period
        start_date, end_date, period_name = calculate_period_dates(period)

        # Get metrics for the period
        metrics = metrics_service.get_metrics_for_period(strategy_id, start_date, end_date)

        # Get latest metrics
        latest_metrics = metrics_service.get_latest_metrics_for_strategy(strategy_id)

        # Get executions for this strategy
        executions = db.get_executions_by_strategy(strategy.name)

        # Sort by timestamp (newest first)
        recent_executions = sorted(
            executions,
            key=lambda e: (
                e.execution_timestamp
                if isinstance(e.execution_timestamp, (int, float))
                else datetime.timestamp(e.execution_timestamp)
            ),
            reverse=True,
        )[:10]  # Get only the 10 most recent

        return templates.TemplateResponse(
            "pages/strategy_metrics.html",
            {
                "request": request,
                "strategy": strategy,
                "metrics": latest_metrics,
                "metrics_history": metrics,
                "period": period,
                "period_name": period_name,
                "executions": recent_executions,
            },
        )
    except Exception as e:
        logger.error(f"Error in strategy metrics: {e}", exc_info=True)
        return templates.TemplateResponse(
            "errors/error.html", {"request": request, "error_message": f"Error loading strategy metrics: {str(e)}"}
        )


@router.get("/api/metrics/{strategy_id}")
async def get_strategy_metrics_api(
    strategy_id: int,
    period: str = Query("7d", description="Time period for metrics (1d, 7d, 30d, 90d, all)"),
    metrics_service: StrategyMetricsService = Depends(get_metrics_service),
):
    """API endpoint to get metrics for a strategy in JSON format."""
    try:
        # Get time period
        start_date, end_date, _ = calculate_period_dates(period)

        # Get metrics for the period
        metrics = metrics_service.get_metrics_for_period(strategy_id, start_date, end_date)

        if not metrics:
            return {"status": "success", "data": None, "message": "No metrics found for this period"}

        # Convert metrics to dictionary format for JSON response
        metrics_data = []
        for metric in metrics:
            metrics_data.append(
                {
                    "id": metric.id,
                    "strategy_id": metric.strategy_id,
                    "period_start": metric.period_start.isoformat() if metric.period_start else None,
                    "period_end": metric.period_end.isoformat() if metric.period_end else None,
                    "win_count": metric.win_count,
                    "loss_count": metric.loss_count,
                    "win_rate": float(metric.win_rate) if metric.win_rate else 0,
                    "gross_profit": float(metric.gross_profit) if metric.gross_profit else 0,
                    "gross_loss": float(metric.gross_loss) if metric.gross_loss else 0,
                    "net_profit": float(metric.net_profit) if metric.net_profit else 0,
                    "profit_factor": float(metric.profit_factor) if metric.profit_factor else 0,
                    "max_drawdown": float(metric.max_drawdown) if metric.max_drawdown else 0,
                    "max_drawdown_percentage": (
                        float(metric.max_drawdown_percentage) if metric.max_drawdown_percentage else 0
                    ),
                    "avg_profit_per_trade": float(metric.avg_profit_per_trade) if metric.avg_profit_per_trade else 0,
                    "avg_loss_per_trade": float(metric.avg_loss_per_trade) if metric.avg_loss_per_trade else 0,
                    "risk_reward_ratio": float(metric.risk_reward_ratio) if metric.risk_reward_ratio else 0,
                    "total_volume": float(metric.total_volume) if metric.total_volume else 0,
                    "avg_volume_per_trade": float(metric.avg_volume_per_trade) if metric.avg_volume_per_trade else 0,
                    "avg_hold_time_seconds": metric.avg_hold_time_seconds,
                    "created_at": metric.created_at.isoformat() if metric.created_at else None,
                }
            )

        return {"status": "success", "data": metrics_data, "count": len(metrics_data)}
    except Exception as e:
        logger.error(f"Error in strategy metrics API: {e}", exc_info=True)
        return JSONResponse(
            status_code=500, content={"status": "error", "message": f"Error retrieving metrics: {str(e)}"}
        )


# Add route to manually trigger metrics calculation
@router.get("/recalculate-metrics/{strategy_id}")
async def recalculate_metrics_endpoint(
    request: Request,
    strategy_id: int,
    period_days: int = Query(7, description="Number of days to include in calculation", ge=1, le=365),
    db: DatabaseService = Depends(get_db_service),
    metrics_service: StrategyMetricsService = Depends(get_metrics_service),
):
    """Manually trigger metrics calculation for a strategy."""
    try:
        # Get the strategy
        strategy = db.get_strategy(strategy_id)
        if not strategy:
            return templates.TemplateResponse(
                "errors/error.html", {"request": request, "error_message": f"Strategy with ID {strategy_id} not found"}
            )

        # Calculate time period
        end_date = datetime.now()
        start_date = end_date - timedelta(days=period_days)

        # Calculate metrics
        metrics = metrics_service.calculate_strategy_metrics(db.session, strategy_id, start_date, end_date)

        if metrics:
            return templates.TemplateResponse(
                "success.html",
                {
                    "request": request,
                    "message": f"Metrics calculated successfully for {strategy.name}",
                    "redirect_url": f"/metrics/{strategy_id}",
                },
            )
        else:
            return templates.TemplateResponse(
                "errors/error.html",
                {
                    "request": request,
                    "error_message": f"No executions found for {strategy.name} in the last {period_days} days",
                },
            )
    except Exception as e:
        logger.error(f"Error recalculating metrics: {e}", exc_info=True)
        return templates.TemplateResponse(
            "errors/error.html", {"request": request, "error_message": f"Error recalculating metrics: {str(e)}"}
        )
