"""
Helper functions for metrics calculations and processing.
This module centralizes common metrics-related functions used across the application.
"""

from datetime import datetime, timedelta
from typing import Tuple


def calculate_period_dates(period: str) -> Tuple[datetime, datetime, str]:
    """
    Calculate start date, end date, and period name based on period string.

    Args:
        period: Period string (1d, 7d, 30d, 90d, all)

    Returns:
        Tuple of (start_date, end_date, period_name)
    """
    end_date = datetime.now()

    if period == "1d":
        start_date = end_date - timedelta(days=1)
        period_name = "Last 24 hours"
    elif period == "7d":
        start_date = end_date - timedelta(days=7)
        period_name = "Last 7 days"
    elif period == "30d":
        start_date = end_date - timedelta(days=30)
        period_name = "Last 30 days"
    elif period == "90d":
        start_date = end_date - timedelta(days=90)
        period_name = "Last 90 days"
    else:  # "all"
        start_date = datetime(2000, 1, 1)
        period_name = "All time"

    return start_date, end_date, period_name


def format_metrics_for_api(metrics_list: list) -> list:
    """
    Format metrics objects into dictionaries suitable for API responses.

    Args:
        metrics_list: List of StrategyMetrics objects

    Returns:
        List of dictionaries with metrics data
    """
    metrics_data = []

    for metric in metrics_list:
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

    return metrics_data


def calculate_strategy_summary_metrics(strategies: list) -> dict:
    """
    Calculate summary metrics across all strategies.

    Args:
        strategies: List of Strategy objects

    Returns:
        Dictionary with summary metrics
    """
    total_profit = sum(s.net_profit for s in strategies)
    total_trades = sum(s.trade_count for s in strategies)

    # Get metrics for each strategy to calculate overall win rate
    win_count = 0
    loss_count = 0

    for strategy in strategies:
        if hasattr(strategy, "latest_metrics") and strategy.latest_metrics:
            win_count += strategy.latest_metrics.win_count
            loss_count += strategy.latest_metrics.loss_count

    overall_win_rate = (win_count / (win_count + loss_count) * 100) if (win_count + loss_count) > 0 else 0

    return {
        "total_profit": total_profit,
        "total_trades": total_trades,
        "win_count": win_count,
        "loss_count": loss_count,
        "overall_win_rate": overall_win_rate,
    }
