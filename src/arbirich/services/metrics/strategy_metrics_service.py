import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from src.arbirich.models.models import StrategyDB as Strategy
from src.arbirich.models.models import (
    StrategyExchangeMetrics,
    StrategyMetrics,
    StrategyTradingPairMetrics,
    TradeExecution,
)
from src.arbirich.services.database.database_service import DatabaseService

logger = logging.getLogger(__name__)


class StrategyMetricsService:
    """Service for calculating and managing strategy performance metrics."""

    def __init__(self, db_service: Optional[DatabaseService] = None):
        self.db = db_service or DatabaseService()
        self.StrategyMetrics = StrategyMetrics
        self.StrategyTradingPairMetrics = StrategyTradingPairMetrics
        self.StrategyExchangeMetrics = StrategyExchangeMetrics
        self.logger = logger

    def calculate_metrics_for_all_strategies(self, period_days: int = 7) -> List[StrategyMetrics]:
        """Calculate metrics for all strategies for the specified period.

        Args:
            period_days: Number of days to include in the period (default: 7)

        Returns:
            List of created StrategyMetrics objects
        """
        # Get all active strategies
        strategies = self.db.session.query(Strategy).filter(Strategy.is_active).all()

        period_end = datetime.utcnow()
        period_start = period_end - timedelta(days=period_days)

        result = []
        for strategy in strategies:
            metrics = self.calculate_strategy_metrics(self.db.session, strategy.id, period_start, period_end)
            if metrics:
                result.append(metrics)

        return result

    def calculate_strategy_metrics(
        self, session: Session, strategy_id: int, period_start: datetime, period_end: datetime
    ) -> Optional[StrategyMetrics]:
        """Calculate metrics for a specific strategy for the given period.

        Args:
            session: SQLAlchemy session
            strategy_id: ID of the strategy
            period_start: Start of the period
            period_end: End of the period

        Returns:
            Created StrategyMetrics object or None if no trades found
        """
        try:
            # Get executions for this strategy in the time period
            from src.arbirich.models.schema import strategies

            # Get the strategy first
            strategy_query = select(strategies).where(strategies.c.id == strategy_id)
            strategy_result = session.execute(strategy_query).first()

            if not strategy_result:
                logger.error(f"Strategy with ID {strategy_id} not found")
                return None

            strategy_name = strategy_result.name

            # Convert datetime to string format for query
            start_str = period_start.strftime("%Y-%m-%d %H:%M:%S")
            end_str = period_end.strftime("%Y-%m-%d %H:%M:%S")

            # Update query to join with trading_pairs and exchanges tables to get the necessary columns
            query = text(
                """
                SELECT 
                    e.*,
                    tp.symbol as pair,
                    be.name as buy_exchange,
                    se.name as sell_exchange
                FROM 
                    trade_executions e
                JOIN 
                    trading_pairs tp ON e.trading_pair_id = tp.id
                JOIN 
                    exchanges be ON e.buy_exchange_id = be.id
                JOIN 
                    exchanges se ON e.sell_exchange_id = se.id
                WHERE 
                    e.strategy_id = :strategy_id 
                    AND e.execution_timestamp BETWEEN :start_date AND :end_date
            """
            )

            # Execute with parameters
            executions_result = session.execute(
                query, {"strategy_id": strategy_id, "start_date": start_str, "end_date": end_str}
            )

            executions = list(executions_result)

            if not executions:
                logger.warning(f"No executions found for strategy {strategy_name} in the specified period")
                return None

            # Initialize metrics with consistent Decimal type
            win_count = 0
            loss_count = 0
            gross_profit = Decimal("0.0")
            gross_loss = Decimal("0.0")
            total_volume = Decimal("0.0")

            # Track metrics by pair and exchange
            pair_metrics = {}
            exchange_metrics = {}

            for execution in executions:
                # Convert numeric values to Decimal for consistent calculations
                executed_buy_price = Decimal(str(execution.executed_buy_price))
                executed_sell_price = Decimal(str(execution.executed_sell_price))
                volume = Decimal(str(execution.volume))

                # Calculate profit for this execution using Decimal arithmetic
                profit = (executed_sell_price - executed_buy_price) * volume

                # Update win/loss counts
                if profit > Decimal("0"):
                    win_count += 1
                    gross_profit += profit
                else:
                    loss_count += 1
                    gross_loss += abs(profit)

                total_volume += volume

                # Track metrics by pair
                pair = execution.pair
                if pair not in pair_metrics:
                    pair_metrics[pair] = {"trade_count": 0, "wins": 0, "losses": 0, "net_profit": Decimal("0.0")}

                pair_metrics[pair]["trade_count"] += 1
                if profit > Decimal("0"):
                    pair_metrics[pair]["wins"] += 1
                else:
                    pair_metrics[pair]["losses"] += 1
                pair_metrics[pair]["net_profit"] += profit

                # Track metrics by exchange (both buy and sell)
                for exchange in [execution.buy_exchange, execution.sell_exchange]:
                    if exchange not in exchange_metrics:
                        exchange_metrics[exchange] = {
                            "trade_count": 0,
                            "wins": 0,
                            "losses": 0,
                            "net_profit": Decimal("0.0"),
                        }

                    exchange_metrics[exchange]["trade_count"] += 1
                    if profit > Decimal("0"):
                        exchange_metrics[exchange]["wins"] += 1
                    else:
                        exchange_metrics[exchange]["losses"] += 1
                    # Split profit evenly between buy and sell exchanges
                    exchange_metrics[exchange]["net_profit"] += profit / Decimal("2")

            # Calculate derived metrics
            total_trades = win_count + loss_count
            win_rate = (
                (Decimal(str(win_count)) / Decimal(str(total_trades)) * Decimal("100"))
                if total_trades > 0
                else Decimal("0")
            )
            net_profit = gross_profit - gross_loss
            profit_factor = (gross_profit / gross_loss) if gross_loss > Decimal("0") else Decimal("0")
            avg_profit_per_trade = gross_profit / Decimal(str(win_count)) if win_count > 0 else Decimal("0")
            avg_loss_per_trade = gross_loss / Decimal(str(loss_count)) if loss_count > 0 else Decimal("0")
            risk_reward_ratio = (
                (avg_profit_per_trade / avg_loss_per_trade) if avg_loss_per_trade > Decimal("0") else Decimal("0")
            )

            # Calculate average volume per trade (save for use in metrics object)
            avg_volume_per_trade = total_volume / Decimal(str(total_trades)) if total_trades > 0 else Decimal("0")

            # Calculate max drawdown (simplified)
            max_drawdown = gross_loss
            max_drawdown_percentage = (
                (max_drawdown / (gross_profit + gross_loss) * Decimal("100"))
                if (gross_profit + gross_loss) > Decimal("0")
                else Decimal("0")
            )

            # Create metrics record
            metrics = StrategyMetrics(
                strategy_id=strategy_id,
                period_start=period_start,
                period_end=period_end,
                win_count=win_count,
                loss_count=loss_count,
                win_rate=win_rate,
                gross_profit=gross_profit,
                gross_loss=gross_loss,
                net_profit=net_profit,
                profit_factor=profit_factor,
                max_drawdown=max_drawdown,
                max_drawdown_percentage=max_drawdown_percentage,
                avg_profit_per_trade=avg_profit_per_trade,
                avg_loss_per_trade=avg_loss_per_trade,
                risk_reward_ratio=risk_reward_ratio,
                total_volume=total_volume,
                avg_volume_per_trade=avg_volume_per_trade,
                avg_hold_time_seconds=0,  # We don't track this yet
            )

            # Save the metrics to the database
            session.add(metrics)
            session.flush()  # This gives us an ID for the metrics record

            # Now create and save the pair metrics - using parameterized queries for safety
            for pair_name, pair_data in pair_metrics.items():
                # Get the pair ID - using safer parameterized query
                # Fix the table name from 'pairs' to 'trading_pairs'
                pair_query = text("SELECT id FROM trading_pairs WHERE symbol = :symbol")
                pair_result = session.execute(pair_query, {"symbol": pair_name}).first()

                if pair_result:
                    pair_id = pair_result.id
                    pair_win_rate = (
                        (Decimal(str(pair_data["wins"])) / Decimal(str(pair_data["trade_count"])) * Decimal("100"))
                        if pair_data["trade_count"] > 0
                        else Decimal("0")
                    )

                    pair_metric = StrategyTradingPairMetrics(
                        strategy_metrics_id=metrics.id,
                        trading_pair_id=pair_id,
                        trade_count=pair_data["trade_count"],
                        net_profit=pair_data["net_profit"],
                        win_rate=pair_win_rate,
                    )

                    session.add(pair_metric)

            # Create and save the exchange metrics - using parameterized queries for safety
            for exchange_name, exchange_data in exchange_metrics.items():
                # Get the exchange ID - using safer parameterized query
                exchange_query = text("SELECT id FROM exchanges WHERE name = :name")
                exchange_result = session.execute(exchange_query, {"name": exchange_name}).first()

                if exchange_result:
                    exchange_id = exchange_result.id
                    exchange_win_rate = (
                        (
                            Decimal(str(exchange_data["wins"]))
                            / Decimal(str(exchange_data["trade_count"]))
                            * Decimal("100")
                        )
                        if exchange_data["trade_count"] > 0
                        else Decimal("0")
                    )

                    exchange_metric = StrategyExchangeMetrics(
                        strategy_metrics_id=metrics.id,
                        exchange_id=exchange_id,
                        trade_count=exchange_data["trade_count"],
                        net_profit=exchange_data["net_profit"],
                        win_rate=exchange_win_rate,
                    )

                    session.add(exchange_metric)

            # Commit everything
            session.commit()

            return metrics
        except Exception as e:
            logger.error(f"Error calculating metrics for strategy {strategy_id}: {e}", exc_info=True)
            session.rollback()
            return None

    def _analyze_trades(self, executions: List[TradeExecution]) -> Dict:
        """Analyze trade executions and calculate various metrics."""
        if not executions:
            return self._get_empty_trades_data()

        total_trades = len(executions)

        # Calculate profit for each execution
        for execution in executions:
            # If net_profit is not already calculated, calculate it
            if not hasattr(execution, "net_profit") or execution.net_profit is None:
                execution.net_profit = (execution.executed_sell_price - execution.executed_buy_price) * execution.volume

        winning_trades = [e for e in executions if e.net_profit > 0]
        losing_trades = [e for e in executions if e.net_profit <= 0]

        win_count = len(winning_trades)
        loss_count = len(losing_trades)

        # Avoid division by zero
        win_rate = (win_count / total_trades * 100) if total_trades > 0 else 0

        # Financial metrics
        gross_profit = sum(e.net_profit for e in winning_trades) if winning_trades else 0
        gross_loss = abs(sum(e.net_profit for e in losing_trades)) if losing_trades else 0
        net_profit = gross_profit - gross_loss

        # Avoid division by zero
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else 0 if gross_profit == 0 else 999

        # Average metrics
        avg_profit = (gross_profit / win_count) if win_count > 0 else 0
        avg_loss = (gross_loss / loss_count) if loss_count > 0 else 0

        # Avoid division by zero
        risk_reward = (avg_profit / avg_loss) if avg_loss > 0 else 0 if avg_profit == 0 else 999

        # Volume metrics
        total_volume = sum(e.volume for e in executions)
        avg_volume = (total_volume / total_trades) if total_trades > 0 else 0

        # Time metrics - handling for executions without closed_at attribute
        total_hold_time = 0
        hold_time_count = 0

        for execution in executions:
            # Check if execution has required attributes
            if hasattr(execution, "executed_at") and hasattr(execution, "closed_at") and execution.closed_at:
                # Calculate hold time
                if isinstance(execution.executed_at, float):
                    executed_time = datetime.fromtimestamp(execution.executed_at)
                else:
                    executed_time = execution.executed_at

                if isinstance(execution.closed_at, float):
                    closed_time = datetime.fromtimestamp(execution.closed_at)
                else:
                    closed_time = execution.closed_at

                hold_time = (closed_time - executed_time).total_seconds()
                total_hold_time += hold_time
                hold_time_count += 1

        avg_hold_time = (total_hold_time / hold_time_count) if hold_time_count > 0 else 0

        return {
            "win_count": win_count,
            "loss_count": loss_count,
            "win_rate": win_rate,
            "gross_profit": gross_profit,
            "gross_loss": gross_loss,
            "net_profit": net_profit,
            "profit_factor": profit_factor,
            "avg_profit_per_trade": avg_profit,
            "avg_loss_per_trade": avg_loss,
            "risk_reward_ratio": risk_reward,
            "total_volume": total_volume,
            "avg_volume_per_trade": avg_volume,
            "avg_hold_time_seconds": avg_hold_time,
        }

    def _get_empty_trades_data(self) -> Dict:
        """Return a dict with zero values for all trade metrics."""
        return {
            "win_count": 0,
            "loss_count": 0,
            "win_rate": 0,
            "gross_profit": 0,
            "gross_loss": 0,
            "net_profit": 0,
            "profit_factor": 0,
            "avg_profit_per_trade": 0,
            "avg_loss_per_trade": 0,
            "risk_reward_ratio": 0,
            "total_volume": 0,
            "avg_volume_per_trade": 0,
            "avg_hold_time_seconds": 0,
        }

    def _calculate_drawdown(self, executions: List[TradeExecution]) -> Tuple[Decimal, Decimal]:
        """Calculate maximum drawdown for a list of executions.

        Returns:
            Tuple of (max_drawdown_amount, max_drawdown_percentage)
        """
        if not executions:
            return Decimal("0"), Decimal("0")

        # Sort executions by date
        sorted_execs = sorted(executions, key=lambda x: x.executed_at)

        peak = Decimal("0")
        max_drawdown = Decimal("0")
        max_drawdown_pct = Decimal("0")
        running_pnl = Decimal("0")

        for exec in sorted_execs:
            running_pnl += Decimal(str(exec.net_profit))

            if running_pnl > peak:
                peak = running_pnl

            current_drawdown = peak - running_pnl
            current_drawdown_pct = (current_drawdown / peak * 100) if peak > 0 else Decimal("0")

            if current_drawdown > max_drawdown:
                max_drawdown = current_drawdown
                max_drawdown_pct = current_drawdown_pct

        return max_drawdown, max_drawdown_pct

    def _calculate_trading_pair_metrics(
        self, session: Session, metrics_id: int, executions: List[TradeExecution]
    ) -> None:
        """Calculate and save trading pair metrics."""
        # Group executions by trading pair
        pair_executions = {}
        for execution in executions:
            if execution.trading_pair_id not in pair_executions:
                pair_executions[execution.trading_pair_id] = []
            pair_executions[execution.trading_pair_id].append(execution)

        # Create metrics for each trading pair
        for pair_id, pair_execs in pair_executions.items():
            total_trades = len(pair_execs)
            winning_trades = [e for e in pair_execs if e.net_profit > 0]

            win_count = len(winning_trades)
            win_rate = (win_count / total_trades * 100) if total_trades > 0 else 0

            net_profit = sum(e.net_profit for e in pair_execs)

            # Create and save the metrics
            pair_metrics = StrategyTradingPairMetrics(
                strategy_metrics_id=metrics_id,
                trading_pair_id=pair_id,
                trade_count=total_trades,
                net_profit=net_profit,
                win_rate=win_rate,
            )

            session.add(pair_metrics)

    def _calculate_exchange_metrics(self, session: Session, metrics_id: int, executions: List[TradeExecution]) -> None:
        """Calculate and save exchange metrics."""
        # Group executions by exchange
        exchange_executions = {}
        for execution in executions:
            # Assuming execution has exchange_id
            if execution.exchange_id not in exchange_executions:
                exchange_executions[execution.exchange_id] = []
            exchange_executions[execution.exchange_id].append(execution)

        # Create metrics for each exchange
        for exchange_id, exchange_execs in exchange_executions.items():
            total_trades = len(exchange_execs)
            winning_trades = [e for e in exchange_execs if e.net_profit > 0]

            win_count = len(winning_trades)
            win_rate = (win_count / total_trades * 100) if total_trades > 0 else 0

            net_profit = sum(e.net_profit for e in exchange_execs)

            # Create and save the metrics
            exchange_metrics = StrategyExchangeMetrics(
                strategy_metrics_id=metrics_id,
                exchange_id=exchange_id,
                trade_count=total_trades,
                net_profit=net_profit,
                win_rate=win_rate,
            )

            session.add(exchange_metrics)

    def get_latest_metrics_for_strategy(self, strategy_id: int) -> Optional[StrategyMetrics]:
        """Get the most recent metrics for a specific strategy."""
        try:
            # Use the database service's session
            metrics = self.db.get_latest_strategy_metrics(strategy_id)
            return metrics
        except Exception as e:
            logger.error(f"Error getting latest metrics for strategy {strategy_id}: {e}")
            return None

    def get_metrics_history(self, strategy_id: int, start_date: datetime, end_date: datetime) -> List[StrategyMetrics]:
        """Get metrics history for a strategy within a date range."""
        with self.db as db:
            return (
                db.session.query(StrategyMetrics)
                .filter(
                    StrategyMetrics.strategy_id == strategy_id,
                    StrategyMetrics.period_end >= start_date,
                    StrategyMetrics.period_end <= end_date,
                )
                .order_by(StrategyMetrics.period_end.asc())
                .all()
            )

    def get_metrics_for_period(
        self, strategy_id: int, start_date: datetime, end_date: datetime
    ) -> List[StrategyMetrics]:
        """Get metrics for a strategy within a specific time period."""
        try:
            # Use the database service's method
            return self.db.get_strategy_metrics_for_period(strategy_id, start_date, end_date)
        except Exception as e:
            logger.error(f"Error getting metrics for strategy {strategy_id} for period: {e}")
            return []
