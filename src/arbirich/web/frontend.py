import os
from typing import List

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from src.arbirich.models.models import TradeExecution, TradeOpportunity

# Import the database service instead of direct ORM usage
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
        # Get recent trade opportunities from the database
        # Note: DatabaseService doesn't have a direct method for this query,
        # so we'll need to implement a custom method
        opportunities = get_recent_opportunities(db_service, limit=25)

        # Get recent executions
        # Similar custom method needed
        executions = get_recent_executions(db_service, limit=10)

        # Calculate statistics
        stats = get_dashboard_stats(db_service)

    return templates.TemplateResponse(
        "dashboard.html", {"request": request, "opportunities": opportunities, "executions": executions, "stats": stats}
    )


@router.get("/opportunities", response_class=HTMLResponse)
async def opportunities_list(request: Request):
    """Display a list of trade opportunities."""
    with DatabaseService() as db_service:
        opportunities = get_recent_opportunities(db_service, limit=100)

    return templates.TemplateResponse("opportunities.html", {"request": request, "opportunities": opportunities})


@router.get("/executions", response_class=HTMLResponse)
async def executions_list(request: Request):
    """Display a list of trade executions."""
    with DatabaseService() as db_service:
        executions = get_recent_executions(db_service, limit=100)

    return templates.TemplateResponse("executions.html", {"request": request, "executions": executions})


# Helper functions to work with the DatabaseService


def get_recent_opportunities(db_service: DatabaseService, limit: int = 25) -> List[dict]:
    """Get recent trade opportunities from the database."""
    # Add a direct SQLAlchemy query since the service doesn't have this method
    with db_service.engine.begin() as conn:
        import sqlalchemy as sa

        from src.arbirich.models.schema import trade_opportunities

        # Check the schema to verify which timestamp column exists
        result = conn.execute(
            sa.select(trade_opportunities).order_by(trade_opportunities.c.opportunity_timestamp.desc()).limit(limit)
        )

        opportunities = []
        for row in result:
            # Create the TradeOpportunity model
            opp = TradeOpportunity.model_validate(
                {
                    **row._asdict(),
                    "opportunity_timestamp": row.opportunity_timestamp.timestamp()
                    if row.opportunity_timestamp
                    else None,
                    "id": str(row.id),
                }
            )

            # Convert to dict and add the profit_percent calculation
            opp_dict = opp.model_dump()

            # Calculate profit percent
            if opp.buy_price > 0:
                profit_percent = ((opp.sell_price - opp.buy_price) / opp.buy_price) * 100
                opp_dict["profit_percent"] = profit_percent
            else:
                opp_dict["profit_percent"] = 0

            # Format the timestamp for the template
            if opp.opportunity_timestamp:
                from datetime import datetime

                opp_dict["created_at"] = datetime.fromtimestamp(opp.opportunity_timestamp)

            opportunities.append(opp_dict)

        return opportunities


def get_recent_executions(db_service: DatabaseService, limit: int = 10) -> List[dict]:
    """Get recent trade executions from the database."""
    # Add a direct SQLAlchemy query since the service doesn't have this method
    with db_service.engine.begin() as conn:
        import sqlalchemy as sa

        from src.arbirich.models.schema import trade_executions

        # Also update this to use the correct timestamp column
        result = conn.execute(
            sa.select(trade_executions).order_by(trade_executions.c.execution_timestamp.desc()).limit(limit)
        )

        executions = []
        for row in result:
            # Create the TradeExecution model
            exec = TradeExecution.model_validate(
                {
                    **row._asdict(),
                    "execution_timestamp": row.execution_timestamp.timestamp() if row.execution_timestamp else None,
                    "id": str(row.id),
                    "opportunity_id": str(row.opportunity_id) if row.opportunity_id else None,
                }
            )

            # Convert to dict and add additional fields
            exec_dict = exec.model_dump()

            # Calculate actual profit
            exec_dict["actual_profit"] = exec.executed_sell_price - exec.executed_buy_price

            # Format the timestamp for the template
            if exec.execution_timestamp:
                from datetime import datetime

                exec_dict["created_at"] = datetime.fromtimestamp(exec.execution_timestamp)

            # Add a default status
            exec_dict["status"] = "completed"  # Or get this from your database if it's stored

            # Add expected_profit if needed
            exec_dict["expected_profit"] = exec_dict["actual_profit"]  # Or calculate based on your logic

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
