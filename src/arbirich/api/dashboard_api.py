"""API endpoints for dashboard data."""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from src.arbirich.services.database.database_service import DatabaseService

logger = logging.getLogger(__name__)

router = APIRouter(tags=["dashboard"])


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


@router.get("/stats", response_model=DashboardStats)
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


@router.get("/profit-chart", response_model=ChartData)
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
