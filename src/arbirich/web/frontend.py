import json
import logging
import os
from datetime import datetime
from typing import List

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from arbirich.services.metrics.strategy_metrics_service import StrategyMetricsService
from src.arbirich.services.database.database_service import DatabaseService

logger = logging.getLogger(__name__)

# Define the directory containing the templates
templates_dir = os.path.join(os.path.dirname(__file__), "templates")
templates = Jinja2Templates(directory=templates_dir)

# Create a router for frontend routes - don't use empty prefix
router = APIRouter(tags=["frontend"])


# Add get_db function for dependency injection
def get_db():
    db = DatabaseService()
    try:
        yield db
    finally:
        pass


@router.get("/", response_class=HTMLResponse)
async def get_index(request: Request):
    """Render the index page with setup, dashboard, and monitor buttons."""
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/{path:path}", response_class=HTMLResponse)
async def catch_all(request: Request, path: str):
    """Catch all other routes and display 404 page."""
    try:
        # First check if path exists in templates directory
        template_path = f"src/arbirich/web/templates/pages/{path}.html"

        if os.path.exists(template_path):
            # For dashboard route, we need to pass the stats
            if path == "dashboard":
                with DatabaseService() as db_service:
                    # Get strategies for the dashboard
                    strategies = db_service.get_all_strategies()

                    # Sort strategies by profit (most profitable first)
                    strategies = sorted(strategies, key=lambda s: s.net_profit, reverse=True)

                    # Get metrics for each strategy
                    metrics_service = StrategyMetricsService(db_service)
                    for strategy in strategies:
                        metrics = metrics_service.get_latest_metrics_for_strategy(strategy.id)
                        strategy.latest_metrics = metrics

                    # Get recent executions
                    executions = []
                    for strategy in strategies:
                        strategy_executions = db_service.get_executions_by_strategy(strategy.name)
                        executions.extend(strategy_executions)

                    # Sort executions by timestamp (newest first) and take top 10
                    executions.sort(key=lambda x: x.execution_timestamp, reverse=True)
                    recent_executions = executions[:10]

                    # Calculate total stats
                    total_profit = sum(s.net_profit for s in strategies)
                    total_trades = sum(s.trade_count for s in strategies)

                    # Calculate overall win rate
                    win_count = sum(m.win_count for s in strategies if (m := getattr(s, "latest_metrics", None)))
                    loss_count = sum(m.loss_count for s in strategies if (m := getattr(s, "latest_metrics", None)))
                    overall_win_rate = (
                        (win_count / (win_count + loss_count) * 100) if (win_count + loss_count) > 0 else 0
                    )

                    # Get statistics
                    stats = get_dashboard_stats(db_service)

                return templates.TemplateResponse(
                    f"pages/{path}.html",
                    {
                        "request": request,
                        "strategies": strategies,
                        "recent_executions": recent_executions,
                        "total_profit": total_profit,
                        "total_trades": total_trades,
                        "overall_win_rate": overall_win_rate,
                        "stats": stats,
                    },
                )
            # For executions route, we need to pass pagination info
            elif path == "executions":
                with DatabaseService() as db_service:
                    executions = get_recent_executions(db_service, limit=20)
                    return templates.TemplateResponse(
                        f"pages/{path}.html",
                        {
                            "request": request,
                            "executions": executions,
                            "total_pages": 1,
                            "page": 1,
                            "per_page": 20,
                            "total_executions": len(executions),
                        },
                    )
            else:
                # For other pages, just render the template
                return templates.TemplateResponse(f"pages/{path}.html", {"request": request})
        else:
            return templates.TemplateResponse(
                "errors/404.html",
                {"request": request, "path": path, "message": f"The page '{path}' you're looking for was not found."},
                status_code=404,
            )
    except Exception as e:
        logger.error(f"Error handling unknown path '{path}': {e}")
        return templates.TemplateResponse(
            "errors/error.html", {"request": request, "error_message": f"Error: {str(e)}"}
        )


from src.arbirich.web.controllers.mock_data_provider import mock_provider


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Display the main dashboard with trading data."""
    # Use the DatabaseService to get data
    try:
        with DatabaseService() as db:
            try:
                # Get recent trade opportunities from the database - limit to 5 for dashboard
                opportunities = get_recent_opportunities(db, limit=5)
            except Exception as e:
                logger.warning(f"Error getting opportunities, using mock data: {e}")
                opportunities = mock_provider.get_opportunities(5)

            try:
                # Get recent executions - limit to 5 for dashboard
                executions = get_recent_executions(db, limit=5)
            except Exception as e:
                logger.warning(f"Error getting executions, using mock data: {e}")
                executions = mock_provider.get_executions(5)

            try:
                # Get strategy leaderboard
                strategies = get_strategy_leaderboard(db)
            except Exception as e:
                logger.warning(f"Error getting strategies, using mock data: {e}")
                strategies = mock_provider.get_strategies()

            try:
                # Calculate statistics
                stats = get_dashboard_stats(db)
            except Exception as e:
                logger.warning(f"Error getting stats, using mock data: {e}")
                stats = mock_provider.get_dashboard_stats()

        return templates.TemplateResponse(
            "pages/dashboard.html",
            {
                "request": request,
                "opportunities": opportunities,
                "executions": executions,
                "strategies": strategies,
                "stats": stats,
            },
        )
    except Exception as e:
        logger.error(f"Error rendering dashboard: {e}")
        # Fall back to using all mock data
        return templates.TemplateResponse(
            "pages/dashboard.html",
            {
                "request": request,
                "opportunities": mock_provider.get_opportunities(5),
                "executions": mock_provider.get_executions(5),
                "strategies": mock_provider.get_strategies(),
                "stats": mock_provider.get_dashboard_stats(),
            },
        )


@router.get("/opportunities", response_class=HTMLResponse)
async def opportunities_list(request: Request):
    """Display a list of trade opportunities."""
    with DatabaseService() as db_service:
        # Show more on the dedicated page
        opportunities = get_recent_opportunities(db_service, limit=50)

    return templates.TemplateResponse("pages/opportunities.html", {"request": request, "opportunities": opportunities})


@router.get("/executions", response_class=HTMLResponse)
async def executions_list(request: Request):
    """Display a list of trade executions."""
    with DatabaseService() as db_service:
        # Show more on the dedicated page
        executions = get_recent_executions(db_service, limit=50)

    return templates.TemplateResponse("pages/executions.html", {"request": request, "executions": executions})


@router.get("/strategies", response_class=HTMLResponse)
async def strategies_list(request: Request):
    """Display a list of all strategies."""
    with DatabaseService() as db_service:
        strategies = get_strategy_leaderboard(db_service)

    return templates.TemplateResponse("pages/strategies.html", {"request": request, "strategies": strategies})


@router.get("/strategy/{strategy_name}", response_class=HTMLResponse)
async def strategy_detail(request: Request, strategy_name: str, db: DatabaseService = Depends(get_db)):
    """Strategy detail page."""
    try:
        logger.info(f"Looking up strategy: {strategy_name}")

        # Get strategy by name
        strategies = db.get_all_strategies()
        strategy = next((s for s in strategies if s.name == strategy_name), None)

        if not strategy:
            logger.warning(f"Strategy not found: {strategy_name}")
            return templates.TemplateResponse(
                "errors/error.html", {"request": request, "error_message": f"Strategy {strategy_name} not found"}
            )

        # Process additional_info if it's a string
        if hasattr(strategy, "additional_info") and isinstance(strategy.additional_info, str):
            try:
                strategy.additional_info = json.loads(strategy.additional_info)
            except (json.JSONDecodeError, TypeError):
                # If it can't be parsed as JSON, set to empty dict
                strategy.additional_info = {}
        elif not hasattr(strategy, "additional_info") or strategy.additional_info is None:
            strategy.additional_info = {}

        # Get recent executions
        raw_executions = db.get_executions_by_strategy(strategy_name)

        # Process executions to include calculated fields and correct format
        executions = []
        for execution in raw_executions:
            # Convert timestamp to datetime
            if isinstance(execution.execution_timestamp, (int, float)):
                execution_time = datetime.fromtimestamp(execution.execution_timestamp)
            else:
                execution_time = execution.execution_timestamp

            # Calculate profit here instead of relying on an actual_profit attribute
            profit = (execution.executed_sell_price - execution.executed_buy_price) * execution.volume

            # Create a dictionary with all needed attributes
            exec_dict = {
                "id": execution.id,
                "pair": execution.pair,
                "buy_exchange": execution.buy_exchange,
                "sell_exchange": execution.sell_exchange,
                "executed_buy_price": execution.executed_buy_price,
                "executed_sell_price": execution.executed_sell_price,
                "volume": execution.volume,
                "spread": execution.spread,
                "execution_timestamp": execution_time,
                "profit": profit,  # Add calculated profit
            }

            executions.append(exec_dict)

        # Sort and limit
        executions = sorted(executions, key=lambda e: e["execution_timestamp"], reverse=True)[:10]

        # Get metrics if available - handle possible errors with relationships
        metrics = None
        try:
            from arbirich.services.metrics.strategy_metrics_service import StrategyMetricsService

            metrics_service = StrategyMetricsService(db)
            metrics = metrics_service.get_latest_metrics_for_strategy(strategy.id)

            # Safely access related metrics - avoid loading relationships that might error
            if metrics:
                # Don't access metrics.trading_pair_metrics or metrics.exchange_metrics here
                # We'll pass just the basic metrics to the template
                pass
        except Exception as metrics_error:
            logger.error(f"Error loading metrics: {str(metrics_error)}", exc_info=True)
            # Continue without metrics

        return templates.TemplateResponse(
            "pages/strategy.html",
            {"request": request, "strategy": strategy, "executions": executions, "metrics": metrics},
        )
    except Exception as e:
        logger.error(f"Error in strategy_detail: {str(e)}", exc_info=True)
        return templates.TemplateResponse(
            "errors/error.html", {"request": request, "error_message": f"Error loading strategy: {str(e)}"}
        )


@router.get("/setup", response_class=HTMLResponse)
async def setup(request: Request):
    """Display the setup page for configuration."""
    return templates.TemplateResponse("pages/setup.html", {"request": request})


@router.get("/monitor", response_class=HTMLResponse)
async def monitor(request: Request):
    """Display the monitoring page for real-time system status."""
    return templates.TemplateResponse("pages/monitor.html", {"request": request})


# Helper functions to work with the DatabaseService


def get_recent_opportunities(db_service: DatabaseService, limit: int = 25) -> List[dict]:
    """Get recent trade opportunities from the database."""
    with db_service.engine.begin() as conn:
        import sqlalchemy as sa

        from src.arbirich.models.schema import trade_opportunities

        # Query the opportunities with proper ordering
        result = conn.execute(
            sa.select(trade_opportunities).order_by(trade_opportunities.c.opportunity_timestamp.desc()).limit(limit)
        )

        opportunities = []
        for row in result:
            # Create the opportunity dict with proper field mapping
            opp_dict = {
                "id": str(row.id),
                "strategy": row.strategy,
                "pair": row.pair,  # This is used instead of 'symbol' in the template
                "symbol": row.pair,  # Add this as well for compatibility
                "buy_exchange": row.buy_exchange,
                "sell_exchange": row.sell_exchange,
                "buy_price": float(row.buy_price),
                "sell_price": float(row.sell_price),
                "spread": float(row.spread),
                "volume": float(row.volume),
            }

            # Calculate profit percent
            if opp_dict["buy_price"] > 0:
                profit_percent = ((opp_dict["sell_price"] - opp_dict["buy_price"]) / opp_dict["buy_price"]) * 100
                opp_dict["profit_percent"] = profit_percent
            else:
                opp_dict["profit_percent"] = 0

            # Format the timestamp for the template
            if row.opportunity_timestamp:
                opp_dict["created_at"] = row.opportunity_timestamp

            opportunities.append(opp_dict)

        return opportunities


def get_recent_executions(db_service: DatabaseService, limit: int = 10) -> List[dict]:
    """Get recent trade executions from the database."""
    with db_service.engine.begin() as conn:
        import sqlalchemy as sa

        from src.arbirich.models.schema import trade_executions

        # Query the executions with proper ordering
        result = conn.execute(
            sa.select(trade_executions).order_by(trade_executions.c.execution_timestamp.desc()).limit(limit)
        )

        executions = []
        for row in result:
            # Create the execution dict with proper field mapping
            exec_dict = {
                "id": str(row.id),
                "opportunity_id": str(row.opportunity_id) if row.opportunity_id else None,
                "strategy": row.strategy,
                "pair": row.pair,
                "symbol": row.pair,  # Add for compatibility
                "buy_exchange": row.buy_exchange,
                "sell_exchange": row.sell_exchange,
                "executed_buy_price": float(row.executed_buy_price),
                "executed_sell_price": float(row.executed_sell_price),
                "spread": float(row.spread),
                "volume": float(row.volume),
                "execution_id": row.execution_id,
            }

            # Calculate actual profit
            exec_dict["actual_profit"] = exec_dict["executed_sell_price"] - exec_dict["executed_buy_price"]

            # Add expected profit (same as actual for now)
            exec_dict["expected_profit"] = exec_dict["actual_profit"]

            # Format the timestamp for the template
            if row.execution_timestamp:
                exec_dict["created_at"] = row.execution_timestamp

            # Add a default status based on data
            if exec_dict["actual_profit"] > 0:
                exec_dict["status"] = "completed"
            elif exec_dict["actual_profit"] < 0:
                exec_dict["status"] = "failed"
            else:
                exec_dict["status"] = "pending"

            # Ensure opportunity_id is properly formatted for the UI
            if exec_dict["opportunity_id"] == "None" or exec_dict["opportunity_id"] is None:
                exec_dict["opportunity_id"] = None

            executions.append(exec_dict)

        return executions


def get_dashboard_stats(db_service: DatabaseService) -> dict:
    """Get dashboard statistics."""
    with db_service.engine.begin() as conn:
        import sqlalchemy as sa

        from src.arbirich.models.schema import trade_executions, trade_opportunities

        # Count opportunities
        opp_result = conn.execute(sa.select(sa.func.count()).select_from(trade_opportunities))
        total_opportunities = opp_result.scalar() or 0

        # Count executions
        exec_result = conn.execute(sa.select(sa.func.count()).select_from(trade_executions))
        total_executions = exec_result.scalar() or 0

        return {
            "total_opportunities": total_opportunities,
            "total_executions": total_executions,
        }


def get_strategy_leaderboard(db_service: DatabaseService) -> List[dict]:
    """Get performance stats for all strategies."""
    with db_service.engine.begin() as conn:
        import sqlalchemy as sa

        from src.arbirich.models.schema import strategies

        # Get all strategies ordered by net profit
        result = conn.execute(sa.select(strategies).order_by(strategies.c.net_profit.desc()))

        strategies_list = []
        for row in result:
            # Create the strategy dict
            strategy_dict = {
                "id": row.id,
                "name": row.name,
                "starting_capital": float(row.starting_capital),
                "min_spread": float(row.min_spread),
                "total_profit": float(row.total_profit),
                "total_loss": float(row.total_loss),
                "net_profit": float(row.net_profit),
                "trade_count": row.trade_count,
                "start_timestamp": row.start_timestamp,
                "last_updated": row.last_updated,
            }

            strategies_list.append(strategy_dict)

        return strategies_list


def get_strategy_opportunities(db_service: DatabaseService, strategy_name: str, limit: int = 10) -> List[dict]:
    """Get opportunities for a specific strategy."""
    with db_service.engine.begin() as conn:
        import sqlalchemy as sa

        from src.arbirich.models.schema import trade_opportunities

        # Query opportunities for this strategy with proper ordering
        result = conn.execute(
            sa.select(trade_opportunities)
            .where(trade_opportunities.c.strategy == strategy_name)
            .order_by(trade_opportunities.c.opportunity_timestamp.desc())
            .limit(limit)
        )

        opportunities = []
        for row in result:
            # Create the opportunity dict with proper field mapping
            opp_dict = {
                "id": str(row.id),
                "strategy": row.strategy,
                "pair": row.pair,
                "buy_exchange": row.buy_exchange,
                "sell_exchange": row.sell_exchange,
                "buy_price": float(row.buy_price),
                "sell_price": float(row.sell_price),
                "spread": float(row.spread),
                "volume": float(row.volume),
            }

            # Calculate profit percent
            if opp_dict["buy_price"] > 0:
                profit_percent = ((opp_dict["sell_price"] - opp_dict["buy_price"]) / opp_dict["buy_price"]) * 100
                opp_dict["profit_percent"] = profit_percent
            else:
                opp_dict["profit_percent"] = 0

            # Format the timestamp for the template
            if row.opportunity_timestamp:
                opp_dict["created_at"] = row.opportunity_timestamp

            opportunities.append(opp_dict)

        return opportunities


def get_strategy_executions(db_service: DatabaseService, strategy_name: str, limit: int = 10) -> List[dict]:
    """Get executions for a specific strategy."""
    with db_service.engine.begin() as conn:
        import sqlalchemy as sa

        from src.arbirich.models.schema import trade_executions

        # Query executions for this strategy with proper ordering
        result = conn.execute(
            sa.select(trade_executions)
            .where(trade_executions.c.strategy == strategy_name)
            .order_by(trade_executions.c.execution_timestamp.desc())
            .limit(limit)
        )

        executions = []
        for row in result:
            # Create the execution dict with proper field mapping
            exec_dict = {
                "id": str(row.id),
                "opportunity_id": str(row.opportunity_id) if row.opportunity_id else None,
                "strategy": row.strategy,
                "pair": row.pair,
                "buy_exchange": row.buy_exchange,
                "sell_exchange": row.sell_exchange,
                "executed_buy_price": float(row.executed_buy_price),
                "executed_sell_price": float(row.executed_sell_price),
                "spread": float(row.spread),
                "volume": float(row.volume),
                "execution_id": row.execution_id,
            }

            # Calculate actual profit
            exec_dict["actual_profit"] = exec_dict["executed_sell_price"] - exec_dict["executed_buy_price"]

            # Format the timestamp for the template
            if row.execution_timestamp:
                exec_dict["created_at"] = row.execution_timestamp

            # Add a default status based on data
            if exec_dict["actual_profit"] > 0:
                exec_dict["status"] = "completed"
            elif exec_dict["actual_profit"] < 0:
                exec_dict["status"] = "failed"
            else:
                exec_dict["status"] = "pending"

            # Ensure opportunity_id is properly formatted for the UI
            if exec_dict["opportunity_id"] == "None" or exec_dict["opportunity_id"] is None:
                exec_dict["opportunity_id"] = None

            executions.append(exec_dict)

        return executions
