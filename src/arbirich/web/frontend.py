import json
import logging
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import List

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from src.arbirich.models import enums
from src.arbirich.models.models import Exchange, TradeExecution, TradeOpportunity
from src.arbirich.services.database.database_service import DatabaseService
from src.arbirich.services.metrics.strategy_metrics_service import StrategyMetricsService
from src.arbirich.web.controllers.mock_data_provider import mock_provider
from src.arbirich.web.dependencies import get_db_service, get_template_context

logger = logging.getLogger(__name__)

base_dir = Path(__file__).resolve().parent
templates_dir = base_dir / "templates"
logger.info(f"Templates directory: {templates_dir}")
templates = Jinja2Templates(directory=str(templates_dir))

# Add global template context variables
templates.env.globals["enums"] = enums

router = APIRouter(tags=["frontend"])


def get_db():
    """Get database service instance using generator"""
    db_gen = get_db_service()
    db = next(db_gen)
    try:
        yield db
    finally:
        try:
            db.close()
        except Exception as e:
            logger.error(f"Error closing database service: {e}")


# Error handling decorator
def handle_errors(template="errors/error.html"):
    def decorator(func):
        @wraps(func)
        async def wrapper(request: Request, *args, **kwargs):
            try:
                return await func(request, *args, **kwargs)
            except Exception as e:
                logger.error(f"Error in {func.__name__}: {str(e)}", exc_info=True)
                return templates.TemplateResponse(template, {"request": request, "error_message": f"Error: {str(e)}"})

        return wrapper

    return decorator


# Database query helpers with fallback to mock data
def safe_query(db_func, mock_func, *args, **kwargs):
    """Execute database query with fallback to mock data"""
    try:
        return db_func(*args, **kwargs)
    except Exception as e:
        logger.warning(f"Error executing {db_func.__name__}, using mock data: {e}")
        return mock_func(*args, **kwargs)


# Routes
@router.get("/", response_class=HTMLResponse)
async def get_index(request: Request):
    """Render the index page"""
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/dashboard", response_class=HTMLResponse)
@handle_errors()
async def dashboard(
    request: Request, db: DatabaseService = Depends(get_db), template_context: dict = Depends(get_template_context)
):
    """Display the main dashboard with trading data."""
    logger.info("Loading dashboard data...")
    try:
        # Get recent opportunities directly - don't use safe_query to catch specific errors
        opportunities = get_recent_opportunities(db, limit=5)
        logger.info(f"Loaded {len(opportunities)} opportunities")
    except Exception as e:
        logger.error(f"Error loading opportunities: {e}", exc_info=True)
        opportunities = mock_provider.get_opportunities(5)
        logger.info(f"Loaded {len(opportunities)} opportunities")

    try:
        # Get recent executions
        executions = get_recent_executions(db, limit=5)
        logger.info(f"Loaded {len(executions)} executions")
    except Exception as e:
        logger.error(f"Error loading executions: {e}", exc_info=True)
        executions = mock_provider.get_executions(5)
        logger.info(f"Loaded {len(executions)} executions")

    try:
        # Get strategies with enhanced debug
        strategies = get_strategy_leaderboard(db)
        logger.info(f"Loaded {len(strategies)} strategies")
        # Log first strategy for debugging
        if strategies:
            logger.info(f"First strategy details: {strategies[0]}")
    except Exception as e:
        logger.error(f"Error loading strategies: {e}", exc_info=True)
        strategies = mock_provider.get_strategies()
        logger.info(f"Loaded {len(strategies)} strategies")

    try:
        # Get enhanced dashboard stats
        stats = get_enhanced_dashboard_stats(db)
        logger.info(f"Loaded dashboard stats: {stats}")
    except Exception as e:
        logger.error(f"Error loading stats: {e}", exc_info=True)
        stats = mock_provider.get_dashboard_stats()
        logger.info(f"Loaded dashboard stats: {stats}")

    try:
        # Get all exchanges
        exchanges = get_all_exchanges(db)
        logger.info(f"Loaded {len(exchanges)} exchanges")
    except Exception as e:
        logger.error(f"Error loading exchanges: {e}", exc_info=True)
        exchanges = mock_provider.get_exchanges() if hasattr(mock_provider, "get_exchanges") else []
        logger.info(f"Loaded {len(exchanges)} exchanges")

    return templates.TemplateResponse(
        "pages/dashboard.html",
        {
            "request": request,
            "opportunities": opportunities,
            "executions": executions,
            "strategies": strategies,
            "stats": stats,
            "exchanges": exchanges,
            **template_context,  # Add template context
        },
    )


@router.get("/opportunities", response_class=HTMLResponse)
@handle_errors()
async def opportunities_list(request: Request, template_context: dict = Depends(get_template_context)):
    """Display a list of trade opportunities."""
    with DatabaseService() as db_service:
        opportunities = get_recent_opportunities(db_service, limit=50)
    return templates.TemplateResponse(
        "pages/opportunities.html", {"request": request, "opportunities": opportunities, **template_context}
    )


@router.get("/executions", response_class=HTMLResponse)
@handle_errors()
async def executions_list(request: Request, template_context: dict = Depends(get_template_context)):
    """Display a list of trade executions."""
    with DatabaseService() as db_service:
        executions = get_recent_executions(db_service, limit=50)
    return templates.TemplateResponse(
        "pages/executions.html", {"request": request, "executions": executions, **template_context}
    )


@router.get("/strategies", response_class=HTMLResponse)
@handle_errors()
async def strategies_list(request: Request, template_context: dict = Depends(get_template_context)):
    """Display a list of all strategies."""
    with DatabaseService() as db_service:
        strategies = get_strategy_leaderboard(db_service)
    return templates.TemplateResponse(
        "pages/strategies.html", {"request": request, "strategies": strategies, **template_context}
    )


@router.get("/strategy/{strategy_name}", response_class=HTMLResponse)
@handle_errors()
async def strategy_detail(
    request: Request,
    strategy_name: str,
    db: DatabaseService = Depends(get_db),
    template_context: dict = Depends(get_template_context),
):
    """Strategy detail page."""
    # Get strategy by name
    strategies = db.get_all_strategies()
    strategy = next((s for s in strategies if s.name == strategy_name), None)

    if not strategy:
        return templates.TemplateResponse(
            "errors/error.html", {"request": request, "error_message": f"Strategy {strategy_name} not found"}
        )

    # Process additional_info field
    if hasattr(strategy, "additional_info") and isinstance(strategy.additional_info, str):
        try:
            strategy.additional_info = json.loads(strategy.additional_info)
        except (json.JSONDecodeError, TypeError):
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

    # Get metrics if available
    metrics = None
    try:
        metrics_service = StrategyMetricsService(db)
        metrics = metrics_service.get_latest_metrics_for_strategy(strategy.id)
    except Exception:
        logger.exception("Error loading metrics")

    return templates.TemplateResponse(
        "pages/strategy.html",
        {
            "request": request,
            "strategy": strategy,
            "executions": executions,
            "metrics": metrics,
            **template_context,  # Add template context
        },
    )


@router.get("/setup", response_class=HTMLResponse)
async def setup(request: Request):
    """Redirect setup page to strategies page."""
    return RedirectResponse(url="/strategies", status_code=307)


@router.get("/monitor", response_class=HTMLResponse)
async def monitor(request: Request, template_context: dict = Depends(get_template_context)):
    """Display the monitoring page for real-time system status."""
    return templates.TemplateResponse("pages/monitor.html", {"request": request, **template_context})


@router.get("/exchanges", response_class=HTMLResponse)
@handle_errors()
async def exchanges_list(request: Request, template_context: dict = Depends(get_template_context)):
    """Display a list of all exchanges."""
    with DatabaseService() as db_service:
        exchanges = get_all_exchanges(db_service)  # Renamed function call
    return templates.TemplateResponse(
        "pages/exchanges.html", {"request": request, "exchanges": exchanges, **template_context}
    )


@router.get("/about", response_class=HTMLResponse)
async def get_about(request: Request, template_context: dict = Depends(get_template_context)):
    """
    Render the about page.
    """
    return templates.TemplateResponse("about.html", {"request": request, **template_context})


@router.get("/{path:path}", response_class=HTMLResponse)
@handle_errors()
async def catch_all(request: Request, path: str, template_context: dict = Depends(get_template_context)):
    """Catch all other routes and display 404 page if template doesn't exist."""
    template_path = templates_dir / "pages" / f"{path}.html"

    if not template_path.exists():
        return templates.TemplateResponse(
            "errors/404.html",
            {"request": request, "path": path, "message": f"The page '{path}' you're looking for was not found."},
            status_code=404,
        )

    # For other pages, just render the template
    return templates.TemplateResponse(f"pages/{path}.html", {"request": request, **template_context})


# Helper functions for database queries - keeping all original functions


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
            # Create a Pydantic model instance
            opportunity = TradeOpportunity(
                id=str(row.id),
                strategy=row.strategy,
                pair=row.pair,
                buy_exchange=row.buy_exchange,
                sell_exchange=row.sell_exchange,
                buy_price=float(row.buy_price),
                sell_price=float(row.sell_price),
                spread=float(row.spread),
                volume=float(row.volume),
                opportunity_timestamp=row.opportunity_timestamp.timestamp() if row.opportunity_timestamp else None,
            )

            # Convert to dict and add calculated fields for template usage
            opp_dict = opportunity.model_dump()

            # Add additional fields needed for templates
            opp_dict["symbol"] = opp_dict["pair"]  # For compatibility template
            if opportunity.opportunity_timestamp:
                opp_dict["created_at"] = datetime.fromtimestamp(opportunity.opportunity_timestamp)
            if opp_dict["buy_price"] > 0:
                spread_percent = ((opp_dict["sell_price"] - opp_dict["buy_price"]) / opp_dict["buy_price"]) * 100
                opp_dict["spread_percent"] = round(spread_percent, 2)
            else:
                opp_dict["spread_percent"] = 0

            # Calculate potential profit (as $ value)
            opp_dict["profit"] = round(float(opportunity.spread * opportunity.volume), 2)

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
            # Create a Pydantic model instance
            execution = TradeExecution(
                id=str(row.id),
                strategy=row.strategy,
                pair=row.pair,
                buy_exchange=row.buy_exchange,
                sell_exchange=row.sell_exchange,
                executed_buy_price=float(row.executed_buy_price),
                executed_sell_price=float(row.executed_sell_price),
                spread=float(row.spread),
                volume=float(row.volume),
                execution_timestamp=row.execution_timestamp.timestamp() if row.execution_timestamp else None,
                execution_id=row.execution_id,
                opportunity_id=str(row.opportunity_id) if row.opportunity_id else None,
            )

            # Convert to dict and add calculated fields for template usage
            exec_dict = execution.model_dump()

            # Add additional fields needed for templates
            exec_dict["symbol"] = exec_dict["pair"]  # Add for compatibility

            # Calculate actual profit including volume (price difference * volume)
            price_diff = execution.executed_sell_price - execution.executed_buy_price
            actual_profit = price_diff * execution.volume
            exec_dict["actual_profit"] = actual_profit
            exec_dict["profit"] = round(actual_profit, 2)  # For template compatibility

            # Add expected profit (same as actual for now)
            exec_dict["expected_profit"] = actual_profit

            # Format the timestamp for the template
            if execution.execution_timestamp:
                exec_dict["created_at"] = datetime.fromtimestamp(execution.execution_timestamp)

            # Add a default status based on data
            if actual_profit > 0:
                exec_dict["status"] = "completed"
            elif actual_profit < 0:
                exec_dict["status"] = "failed"
            else:
                exec_dict["status"] = "pending"

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


def get_strategy_leaderboard(db_service):
    """Fetch strategy performance data for the leaderboard"""
    try:
        # Use the repository pattern instead of direct SQL queries
        strategies = db_service.get_all_strategies()

        # Format the data for display
        result = []
        for strategy in strategies:
            # Handle additional_info properly (might be JSON string or already parsed)
            additional_info = {}
            if hasattr(strategy, "additional_info") and strategy.additional_info:
                if isinstance(strategy.additional_info, dict):
                    additional_info = strategy.additional_info
                elif isinstance(strategy.additional_info, str):
                    try:
                        additional_info = json.loads(strategy.additional_info)
                    except json.JSONDecodeError:
                        pass

            # Extract strategy parameters
            min_spread = getattr(strategy, "min_spread", None)
            min_volume = getattr(strategy, "min_volume", None)
            threshold = getattr(strategy, "threshold", None)

            # Calculate profit percentage
            profit_percentage = 0
            if hasattr(strategy, "starting_capital") and strategy.starting_capital > 0:
                profit_percentage = (strategy.net_profit / strategy.starting_capital) * 100

            # Format the strategy data
            strategy_data = {
                "id": strategy.id,
                "name": strategy.name,
                "description": strategy.description or "",
                "starting_capital": float(strategy.starting_capital) if hasattr(strategy, "starting_capital") else 0,
                "total_profit": float(strategy.total_profit) if hasattr(strategy, "total_profit") else 0,
                "total_loss": float(strategy.total_loss) if hasattr(strategy, "total_loss") else 0,
                "net_profit": float(strategy.net_profit) if hasattr(strategy, "net_profit") else 0,
                "profit": float(strategy.net_profit)
                if hasattr(strategy, "net_profit")
                else 0,  # Add profit alias for template
                "profit_percentage": float(profit_percentage),
                "trade_count": strategy.trade_count if hasattr(strategy, "trade_count") else 0,
                "is_active": strategy.is_active if hasattr(strategy, "is_active") else False,
                "is_running": False,  # Default to false, update if needed
                "exchanges": additional_info.get("exchanges", []),
                "pairs": additional_info.get("pairs", []),
                "parameters": {
                    "min_spread": float(min_spread) if min_spread is not None else 0.0001,
                    "min_volume": float(min_volume) if min_volume is not None else 0.001,
                    "threshold": float(threshold) if threshold is not None else 0.0001,
                },
                "last_updated": strategy.last_updated.isoformat()
                if hasattr(strategy, "last_updated") and strategy.last_updated
                else None,
                "win_rate": 0,  # Default win rate
            }
            result.append(strategy_data)

        # Sort by net profit in descending order
        result.sort(key=lambda s: s["net_profit"], reverse=True)
        return result

    except Exception as e:
        logger.error(f"Error fetching strategy leaderboard: {e}", exc_info=True)
        return []


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
            # Create a Pydantic model instance
            opportunity = TradeOpportunity(
                id=str(row.id),
                strategy=row.strategy,
                pair=row.pair,
                buy_exchange=row.buy_exchange,
                sell_exchange=row.sell_exchange,
                buy_price=float(row.buy_price),
                sell_price=float(row.sell_price),
                spread=float(row.spread),
                volume=float(row.volume),
                opportunity_timestamp=row.opportunity_timestamp.timestamp() if row.opportunity_timestamp else None,
            )

            # Convert to dict and add calculated fields for template usage
            opp_dict = opportunity.model_dump()

            # Add additional fields needed for template
            opp_dict["symbol"] = opp_dict["pair"]  # For compatibility
            if opportunity.opportunity_timestamp:
                opp_dict["created_at"] = datetime.fromtimestamp(opportunity.opportunity_timestamp)
            # Calculate profit percent
            if opp_dict["buy_price"] > 0:
                profit_percent = ((opp_dict["sell_price"] - opp_dict["buy_price"]) / opp_dict["buy_price"]) * 100
                opp_dict["profit_percent"] = profit_percent
            else:
                opp_dict["profit_percent"] = 0

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
            # Create a Pydantic model instance
            execution = TradeExecution(
                id=str(row.id),
                strategy=row.strategy,
                pair=row.pair,
                buy_exchange=row.buy_exchange,
                sell_exchange=row.sell_exchange,
                executed_buy_price=float(row.executed_buy_price),
                executed_sell_price=float(row.executed_sell_price),
                spread=float(row.spread),
                volume=float(row.volume),
                execution_timestamp=row.execution_timestamp.timestamp() if row.execution_timestamp else None,
                execution_id=row.execution_id,
                opportunity_id=str(row.opportunity_id) if row.opportunity_id else None,
            )

            # Convert to dict and add calculated fields for template usage
            exec_dict = execution.model_dump()

            # Add additional fields needed for template
            exec_dict["symbol"] = exec_dict["pair"]  # Add for compatibility

            # Calculate actual profit
            actual_profit = execution.executed_sell_price - execution.executed_buy_price
            exec_dict["actual_profit"] = actual_profit
            exec_dict["profit"] = round(actual_profit, 2)  # For template compatibility

            # Format the timestamp for the template
            if execution.execution_timestamp:
                exec_dict["created_at"] = datetime.fromtimestamp(execution.execution_timestamp)

            # Add a default status based on data
            if actual_profit > 0:
                exec_dict["status"] = "completed"
            elif actual_profit < 0:
                exec_dict["status"] = "failed"
            else:
                exec_dict["status"] = "pending"

            executions.append(exec_dict)

        return executions


def get_all_exchanges(db):
    """Get all exchanges from the database."""
    try:
        exchanges = []
        with db.engine.begin() as conn:
            import sqlalchemy as sa

            from src.arbirich.models.schema import exchanges as exchanges_table

            result = conn.execute(sa.select(exchanges_table))
            for row in result:
                # Fix: Use _mapping instead of mappings
                mapping = row._mapping

                # Handle additional_info properly - check if it's already a dict before parsing
                additional_info = mapping.additional_info
                if additional_info and not isinstance(additional_info, dict):
                    try:
                        additional_info = json.loads(additional_info)
                    except (json.JSONDecodeError, TypeError):
                        additional_info = {}

                # Handle withdrawal_fee properly
                withdrawal_fee = mapping.withdrawal_fee
                if withdrawal_fee and not isinstance(withdrawal_fee, dict):
                    try:
                        withdrawal_fee = json.loads(withdrawal_fee)
                    except (json.JSONDecodeError, TypeError):
                        withdrawal_fee = {}

                # Handle mapping properly
                mapping_data = mapping.mappings
                if mapping_data and not isinstance(mapping_data, dict):
                    try:
                        mapping_data = json.loads(mapping_data)
                    except (json.JSONDecodeError, TypeError):
                        mapping_data = {}

                # Create a Pydantic model instance
                exchange = Exchange(
                    id=mapping.id,
                    name=mapping.name,
                    api_rate_limit=mapping.api_rate_limit,
                    trade_fees=float(mapping.trade_fees) if mapping.trade_fees is not None else None,
                    rest_url=mapping.rest_url,
                    ws_url=mapping.ws_url,
                    delimiter=mapping.delimiter,
                    withdrawal_fee=withdrawal_fee,
                    api_response_time=mapping.api_response_time,
                    mapping=mapping_data,
                    additional_info=additional_info,
                    is_active=mapping.is_active,
                    created_at=mapping.created_at,
                )

                # Convert to dict for template usage
                exchanges.append(exchange.model_dump())

        return exchanges
    except Exception as e:
        logger.error(f"Error loading exchanges: {e}")
        return []


def get_enhanced_dashboard_stats(db_service: DatabaseService) -> dict:
    """Get enhanced dashboard statistics with more KPIs."""
    with db_service.engine.begin() as conn:
        from datetime import datetime, timedelta

        import sqlalchemy as sa

        from src.arbirich.models.schema import strategies, trade_executions, trade_opportunities

        # Count opportunities
        opp_result = conn.execute(sa.select(sa.func.count()).select_from(trade_opportunities))
        total_opportunities = opp_result.scalar() or 0

        # Count executions
        exec_result = conn.execute(sa.select(sa.func.count()).select_from(trade_executions))
        total_executions = exec_result.scalar() or 0

        # Count executions in the last 24 hours
        one_day_ago = datetime.now() - timedelta(days=1)
        exec_24h_result = conn.execute(
            sa.select(sa.func.count())
            .select_from(trade_executions)
            .where(trade_executions.c.execution_timestamp >= one_day_ago)
        )
        executions_24h = exec_24h_result.scalar() or 0

        # Get total profit across all strategies
        profit_result = conn.execute(sa.select(sa.func.sum(strategies.c.net_profit)))
        total_profit = float(profit_result.scalar() or 0)

        # Get highest individual strategy profit
        best_profit_result = conn.execute(sa.select(sa.func.max(strategies.c.net_profit)))
        best_strategy_profit = float(best_profit_result.scalar() or 0)

        # Count active strategies
        active_result = conn.execute(sa.select(sa.func.count()).where(strategies.c.is_active).select_from(strategies))
        active_strategies = active_result.scalar() or 0

        # Calculate win rate from executions if possible
        win_count = (
            conn.execute(
                sa.select(sa.func.count())
                .where(trade_executions.c.executed_sell_price > trade_executions.c.executed_buy_price)
                .select_from(trade_executions)
            ).scalar()
            or 0
        )

        if total_executions > 0:
            win_rate = (win_count / total_executions) * 100
        else:
            win_rate = 0

        # Summing total trades across all strategies
        total_trades_result = conn.execute(sa.select(sa.func.sum(strategies.c.trade_count)))
        total_trades = total_trades_result.scalar() or 0

        return {
            "total_opportunities": total_opportunities,
            "total_executions": total_executions,
            "executions_24h": executions_24h,
            "total_profit": total_profit,
            "best_strategy_profit": best_strategy_profit,
            "win_rate": win_rate,
            "active_strategies": active_strategies,
            "total_trades": int(total_trades),  # Make sure it's an integer
        }
