import logging
import os
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from arbirich.services.strategy_metrics_service import StrategyMetricsService
from src.arbirich.services.database.database_service import DatabaseService

logger = logging.getLogger(__name__)

router = APIRouter()

# Update templates_dir to point to the parent directory of "pages"
templates_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates")
templates = Jinja2Templates(directory=templates_dir)


# Add get_db_service dependency
def get_db_service():
    """Get a database service instance."""
    db = DatabaseService()
    try:
        yield db
    finally:
        # Clean up any resources if needed
        pass


@router.get("/dashboard", response_class=HTMLResponse)
async def get_dashboard(request: Request, db: DatabaseService = Depends(get_db_service)):
    """Get the dashboard view."""
    try:
        # Get strategies for the dashboard
        strategies = db.get_all_strategies()

        # Sort strategies by profit (most profitable first)
        strategies = sorted(strategies, key=lambda s: s.net_profit, reverse=True)

        # Get metrics for each strategy
        metrics_service = StrategyMetricsService(db)
        for strategy in strategies:
            metrics = metrics_service.get_latest_metrics_for_strategy(strategy.id)
            strategy.latest_metrics = metrics

        # Get recent executions
        executions = []
        for strategy in strategies:
            strategy_executions = db.get_executions_by_strategy(strategy.name)
            executions.extend(strategy_executions)

        # Sort executions by timestamp (newest first) and take top 10
        executions.sort(key=lambda x: x.execution_timestamp, reverse=True)
        recent_executions = executions[:10]

        # Calculate total stats
        total_profit = sum(s.net_profit for s in strategies)
        total_trades = sum(s.trade_count for s in strategies)

        # Calculate overall win rate
        win_count = sum(s.latest_metrics.win_count for s in strategies if s.latest_metrics)
        loss_count = sum(s.latest_metrics.loss_count for s in strategies if s.latest_metrics)
        overall_win_rate = (win_count / (win_count + loss_count) * 100) if (win_count + loss_count) > 0 else 0

        return templates.TemplateResponse(
            "pages/dashboard.html",  # Updated path
            {
                "request": request,
                "strategies": strategies,
                "recent_executions": recent_executions,
                "total_profit": total_profit,
                "total_trades": total_trades,
                "overall_win_rate": overall_win_rate,
            },
        )
    except Exception as e:
        logger.error(f"Error in dashboard: {e}", exc_info=True)
        return templates.TemplateResponse(
            "pages/error.html",  # Updated path
            {"request": request, "error_message": f"Error loading dashboard: {str(e)}"},
        )


@router.get("/opportunities", response_class=HTMLResponse)
async def get_opportunities(
    request: Request,
    db: DatabaseService = Depends(get_db_service),
    strategy: str = "",
    pair: str = "",
    days: str = "7",
    page: int = 1,
    per_page: int = 20,
):
    """Get all opportunities with filtering options."""
    try:
        logger.info(
            f"Loading opportunities page with filters: strategy={strategy}, pair={pair}, days={days}, page={page}"
        )

        # Get strategies and pairs for filtering options
        strategies = db.get_all_strategies()
        pairs = db.get_all_pairs()

        # Filter by time period
        end_date = datetime.utcnow()
        if days != "all":
            days_int = int(days)
            start_date = end_date - timedelta(days=days_int)
        else:
            start_date = datetime.fromtimestamp(0)  # Beginning of time

        # Initialize all opportunities and filters for query
        all_opportunities = []

        # Add opportunities from each strategy that matches the filters
        if strategy:
            # Filter by specific strategy
            strategy_opportunities = db.get_opportunities_by_strategy(strategy)
            all_opportunities.extend(strategy_opportunities)
        else:
            # Get opportunities for all strategies
            for strat in strategies:
                strategy_opportunities = db.get_opportunities_by_strategy(strat.name)
                all_opportunities.extend(strategy_opportunities)

        # Filter by date and pair
        filtered_opportunities = []
        for opportunity in all_opportunities:
            # Convert timestamp to datetime if needed
            if isinstance(opportunity.opportunity_timestamp, (int, float)):
                opp_time = datetime.fromtimestamp(opportunity.opportunity_timestamp)
            else:
                opp_time = opportunity.opportunity_timestamp

            # Check date filter
            if opp_time >= start_date and opp_time <= end_date:
                # Check pair filter if specified
                if not pair or opportunity.pair == pair:
                    # Add datetime object for the template
                    opportunity.opportunity_timestamp = opp_time
                    filtered_opportunities.append(opportunity)

        # Sort opportunities by timestamp (newest first)
        sorted_opportunities = sorted(filtered_opportunities, key=lambda o: o.opportunity_timestamp, reverse=True)

        # Pagination
        total_opportunities = len(sorted_opportunities)
        total_pages = (total_opportunities + per_page - 1) // per_page
        page = min(max(1, page), total_pages) if total_pages > 0 else 1

        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_opportunities = sorted_opportunities[start_idx:end_idx]

        # Pre-calculate potential profits for each opportunity
        profit_map = {}
        total_potential_profit = 0

        for opportunity in paginated_opportunities:
            potential_profit = (opportunity.target_sell_price - opportunity.target_buy_price) * opportunity.volume
            profit_map[opportunity.id] = potential_profit
            total_potential_profit += potential_profit

        # Calculate average potential profit
        avg_potential_profit = total_potential_profit / len(paginated_opportunities) if paginated_opportunities else 0

        return templates.TemplateResponse(
            "pages/opportunities.html",  # Updated path
            {
                "request": request,
                "opportunities": paginated_opportunities,
                "strategies": strategies,
                "pairs": pairs,
                "selected_strategy": strategy,
                "selected_pair": pair,
                "days": days,
                "page": page,
                "per_page": per_page,
                "total_pages": total_pages,
                "total_opportunities": total_opportunities,
                "profit_map": profit_map,
                "total_potential_profit": total_potential_profit,
                "avg_potential_profit": avg_potential_profit,
            },
        )
    except Exception as e:
        logger.error(f"Error in opportunities: {e}", exc_info=True)
        return templates.TemplateResponse(
            "pages/error.html",  # Updated path
            {"request": request, "error_message": f"Error loading opportunities: {str(e)}"},
        )


@router.get("/executions", response_class=HTMLResponse)
async def get_executions(
    request: Request,
    db: DatabaseService = Depends(get_db_service),
    strategy: str = "",
    pair: str = "",
    days: str = "7",
    page: int = 1,
    per_page: int = 20,
):
    """Get all executions with filtering options."""
    try:
        logger.info(f"Loading executions page with filters: strategy={strategy}, pair={pair}, days={days}, page={page}")

        # Get strategies and pairs for filtering options
        strategies = db.get_all_strategies()
        pairs = db.get_all_pairs()

        # Filter by time period
        end_date = datetime.utcnow()
        if days != "all":
            days_int = int(days)
            start_date = end_date - timedelta(days=days_int)
        else:
            start_date = datetime.fromtimestamp(0)  # Beginning of time

        # Initialize all executions and filters for query
        all_executions = []

        # Add executions from each strategy that matches the filters
        if strategy:
            # Filter by specific strategy
            strategy_executions = db.get_executions_by_strategy(strategy)
            all_executions.extend(strategy_executions)
        else:
            # Get executions for all strategies
            for strat in strategies:
                strategy_executions = db.get_executions_by_strategy(strat.name)
                all_executions.extend(strategy_executions)

        # Filter by date and pair
        filtered_executions = []
        for execution in all_executions:
            # Convert timestamp to datetime if needed
            if isinstance(execution.execution_timestamp, (int, float)):
                exec_time = datetime.fromtimestamp(execution.execution_timestamp)
            else:
                exec_time = execution.execution_timestamp

            # Check date filter
            if exec_time >= start_date and exec_time <= end_date:
                # Check pair filter if specified
                if not pair or execution.pair == pair:
                    # Add datetime object for the template
                    execution.execution_timestamp = exec_time
                    filtered_executions.append(execution)

        # Sort executions by timestamp (newest first)
        sorted_executions = sorted(filtered_executions, key=lambda e: e.execution_timestamp, reverse=True)

        # Pagination
        total_executions = len(sorted_executions)
        total_pages = (total_executions + per_page - 1) // per_page
        page = min(max(1, page), total_pages) if total_pages > 0 else 1

        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_executions = sorted_executions[start_idx:end_idx]

        # Pre-calculate profits for each execution
        profit_map = {}
        total_profit = 0

        for execution in paginated_executions:
            profit = (execution.executed_sell_price - execution.executed_buy_price) * execution.volume
            profit_map[execution.id] = profit
            total_profit += profit

        # Calculate average profit
        avg_profit = total_profit / len(paginated_executions) if paginated_executions else 0

        return templates.TemplateResponse(
            "pages/executions.html",  # Updated path
            {
                "request": request,
                "executions": paginated_executions,
                "strategies": strategies,
                "pairs": pairs,
                "selected_strategy": strategy,
                "selected_pair": pair,
                "days": days,
                "page": page,
                "per_page": per_page,
                "total_pages": total_pages,
                "total_executions": total_executions,
                "profit_map": profit_map,
                "total_profit": total_profit,
                "avg_profit": avg_profit,
            },
        )
    except Exception as e:
        logger.error(f"Error in executions: {e}", exc_info=True)
        return templates.TemplateResponse(
            "pages/error.html",  # Updated path
            {"request": request, "error_message": f"Error loading executions: {str(e)}"},
        )


@router.get("/api", response_class=HTMLResponse)
async def get_api_docs(request: Request):
    """Get API documentation."""
    return templates.TemplateResponse("pages/api.html", {"request": request})  # Updated path


# Update the strategies endpoint to include metrics data


@router.get("/strategies", response_class=HTMLResponse)
async def get_strategies(request: Request, db: DatabaseService = Depends(get_db_service)):
    """Get all strategies."""
    try:
        strategies = db.get_all_strategies()

        # Get metrics for each strategy
        metrics_service = StrategyMetricsService(db)

        # Add metrics to each strategy
        for strategy in strategies:
            metrics = metrics_service.get_latest_metrics_for_strategy(strategy.id)
            strategy.latest_metrics = metrics

        return templates.TemplateResponse(
            "pages/strategies.html", {"request": request, "strategies": strategies}
        )  # Updated path
    except Exception as e:
        logger.error(f"Error getting strategies: {e}")
        return templates.TemplateResponse(
            "pages/error.html",  # Updated path
            {"request": request, "error_message": f"Error getting strategies: {str(e)}"},
        )
