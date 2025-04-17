import statistics
import time
from datetime import date
from typing import Dict, Optional

from src.arbirich.models.models import TradeExecution


class PerformanceMetrics:
    """Performance tracking for arbitrage strategies"""

    def __init__(self):
        """Initialize performance metrics tracker"""
        # Overall metrics
        self.total_profit = 0.0
        self.trade_count = 0
        self.win_count = 0
        self.loss_count = 0
        self.execution_times = []
        self.profits = []
        self.consecutive_losses = 0
        self.max_consecutive_losses = 0

        # Time-based metrics
        self.start_time = time.time()
        self.last_reset_time = self.start_time
        self.trades_by_day = {}  # Date string -> list of trade results

        # Tracking by exchange and pair
        self.exchange_profits = {}  # Exchange -> profit
        self.pair_profits = {}  # Pair -> profit

        # Recent trades
        self.recent_trades = []  # Last 100 trades
        self.max_recent_trades = 100

    def update(self, result: TradeExecution) -> None:
        """
        Update metrics with a new trade result

        Args:
            result: Trade execution result
        """
        # Update overall metrics
        self.trade_count += 1
        self.total_profit += result.profit
        self.profits.append(result.profit)
        self.execution_times.append(result.execution_time)

        # Update win/loss counts
        if result.profit > 0:
            self.win_count += 1
            self.consecutive_losses = 0
        else:
            self.loss_count += 1
            self.consecutive_losses += 1
            self.max_consecutive_losses = max(self.max_consecutive_losses, self.consecutive_losses)

        # Update time-based metrics
        trade_date = date.fromtimestamp(result.timestamp).isoformat()
        if trade_date not in self.trades_by_day:
            self.trades_by_day[trade_date] = []
        self.trades_by_day[trade_date].append(result)

        # Update exchange metrics
        if result.buy_exchange not in self.exchange_profits:
            self.exchange_profits[result.buy_exchange] = 0.0
        if result.sell_exchange not in self.exchange_profits:
            self.exchange_profits[result.sell_exchange] = 0.0

        # Split profit between exchanges (simplified)
        exchange_profit = result.profit / 2
        self.exchange_profits[result.buy_exchange] += exchange_profit
        self.exchange_profits[result.sell_exchange] += exchange_profit

        # Update pair metrics
        if result.pair not in self.pair_profits:
            self.pair_profits[result.pair] = 0.0
        self.pair_profits[result.pair] += result.profit

        # Update recent trades
        self.recent_trades.append(result)
        if len(self.recent_trades) > self.max_recent_trades:
            self.recent_trades.pop(0)

    def reset_daily(self) -> None:
        """Reset daily metrics"""
        today = date.today().isoformat()
        # Keep historical data but reset today's data
        if today in self.trades_by_day:
            self.trades_by_day[today] = []
        self.last_reset_time = time.time()

    @property
    def win_rate(self) -> float:
        """
        Calculate win rate as a percentage

        Returns:
            Win rate (0-100)
        """
        if self.trade_count == 0:
            return 0.0
        return (self.win_count / self.trade_count) * 100

    @property
    def avg_profit_per_trade(self) -> float:
        """
        Calculate average profit per trade

        Returns:
            Average profit amount
        """
        if self.trade_count == 0:
            return 0.0
        return self.total_profit / self.trade_count

    @property
    def avg_execution_time(self) -> float:
        """
        Calculate average execution time in milliseconds

        Returns:
            Average execution time
        """
        if not self.execution_times:
            return 0.0
        return sum(self.execution_times) / len(self.execution_times)

    @property
    def daily_profit(self) -> float:
        """
        Calculate profit for the current day

        Returns:
            Today's profit
        """
        today = date.today().isoformat()
        if today not in self.trades_by_day:
            return 0.0

        return sum(trade.profit for trade in self.trades_by_day[today])

    @property
    def profit_std_dev(self) -> float:
        """
        Calculate standard deviation of profit

        Returns:
            Standard deviation of profit
        """
        if len(self.profits) < 2:
            return 0.0
        return statistics.stdev(self.profits)

    def get_best_pair(self) -> Optional[str]:
        """
        Get the trading pair with the highest profit

        Returns:
            Best performing trading pair or None
        """
        if not self.pair_profits:
            return None
        return max(self.pair_profits.items(), key=lambda x: x[1])[0]

    def get_best_exchange(self) -> Optional[str]:
        """
        Get the exchange with the highest profit

        Returns:
            Best performing exchange or None
        """
        if not self.exchange_profits:
            return None
        return max(self.exchange_profits.items(), key=lambda x: x[1])[0]

    def to_dict(self) -> Dict:
        """
        Convert metrics to dictionary format

        Returns:
            Dictionary of metrics
        """
        return {
            "total_profit": self.total_profit,
            "trade_count": self.trade_count,
            "win_count": self.win_count,
            "loss_count": self.loss_count,
            "win_rate": self.win_rate,
            "avg_profit": self.avg_profit_per_trade,
            "avg_execution_time": self.avg_execution_time,
            "consecutive_losses": self.consecutive_losses,
            "max_consecutive_losses": self.max_consecutive_losses,
            "daily_profit": self.daily_profit,
            "uptime": time.time() - self.start_time,
            "best_pair": self.get_best_pair(),
            "best_exchange": self.get_best_exchange(),
        }
