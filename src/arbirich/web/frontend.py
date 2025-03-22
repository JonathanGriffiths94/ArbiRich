import os
from typing import List

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from src.arbirich.services.database.database_service import DatabaseService

# Define the directory containing the templates
templates_dir = os.path.join(os.path.dirname(__file__), "templates")
templates = Jinja2Templates(directory=templates_dir)

# Create a router for frontend routes
router = APIRouter(prefix="", tags=["frontend"])


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Render the dashboard homepage."""
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Display the main dashboard with trading data."""
    # Use the DatabaseService to get data
    with DatabaseService() as db_service:
        # Get recent trade opportunities from the database - limit to 5 for dashboard
        opportunities = get_recent_opportunities(db_service, limit=5)

        # Get recent executions - limit to 5 for dashboard
        executions = get_recent_executions(db_service, limit=5)

        # Get strategy leaderboard
        strategies = get_strategy_leaderboard(db_service)

        # Calculate statistics
        stats = get_dashboard_stats(db_service)

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "opportunities": opportunities,
            "executions": executions,
            "strategies": strategies,
            "stats": stats,
        },
    )


@router.get("/opportunities", response_class=HTMLResponse)
async def opportunities_list(request: Request):
    """Display a list of trade opportunities."""
    with DatabaseService() as db_service:
        # Show more on the dedicated page
        opportunities = get_recent_opportunities(db_service, limit=50)

    return templates.TemplateResponse("opportunities.html", {"request": request, "opportunities": opportunities})


@router.get("/executions", response_class=HTMLResponse)
async def executions_list(request: Request):
    """Display a list of trade executions."""
    with DatabaseService() as db_service:
        # Show more on the dedicated page
        executions = get_recent_executions(db_service, limit=50)

    return templates.TemplateResponse("executions.html", {"request": request, "executions": executions})


@router.get("/strategies", response_class=HTMLResponse)
async def strategies_list(request: Request):
    """Display a list of all strategies."""
    with DatabaseService() as db_service:
        strategies = get_strategy_leaderboard(db_service)

    return templates.TemplateResponse("strategies.html", {"request": request, "strategies": strategies})


@router.get("/strategy/{strategy_name}", response_class=HTMLResponse)
async def strategy_detail(request: Request, strategy_name: str):
    """Display detailed information for a specific strategy."""
    with DatabaseService() as db_service:
        # Get the strategy details
        strategies = get_strategy_leaderboard(db_service)
        strategy = next((s for s in strategies if s["name"] == strategy_name), None)

        if not strategy:
            # Handle case where strategy doesn't exist
            return templates.TemplateResponse(
                "error.html", {"request": request, "error": f"Strategy '{strategy_name}' not found"}
            )

        # Get strategy-specific opportunities
        opportunities = get_strategy_opportunities(db_service, strategy_name, limit=10)

        # Get strategy-specific executions
        executions = get_strategy_executions(db_service, strategy_name, limit=10)

    return templates.TemplateResponse(
        "strategy.html",
        {"request": request, "strategy": strategy, "opportunities": opportunities, "executions": executions},
    )


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
