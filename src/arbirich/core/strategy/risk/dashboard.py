import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict

from ..parameters.performance import PerformanceMetrics
from .management import RiskManagement

logger = logging.getLogger(__name__)


class RiskDashboard:
    """
    Comprehensive risk monitoring dashboard

    Aggregates and monitors risk metrics across exchanges, strategies, and execution
    """

    def __init__(self):
        self.strategies = {}  # strategy_id -> metrics
        self.exchanges = {}  # exchange -> metrics
        self.pairs = {}  # trading_pair -> metrics

        # Time-based metrics
        self.hourly_metrics = {}
        self.daily_metrics = {}

        # Risk alerts
        self.alerts = []
        self.max_alerts = 100

        # Risk state
        self.risk_level = "normal"  # normal, elevated, high, critical
        self.last_update = time.time()

    def register_strategy(
        self, strategy_id: str, risk_management: RiskManagement, performance_metrics: PerformanceMetrics
    ):
        """Register a strategy for monitoring"""
        self.strategies[strategy_id] = {
            "risk_management": risk_management,
            "performance": performance_metrics,
            "last_update": time.time(),
        }
        logger.info(f"Registered strategy {strategy_id} with risk dashboard")

    def update_exchange_metrics(self, exchange: str, metrics: Dict[str, Any]):
        """Update metrics for an exchange"""
        if exchange not in self.exchanges:
            self.exchanges[exchange] = {
                "success_rate": 1.0,
                "avg_execution_time": 0,
                "profit": 0.0,
                "trade_count": 0,
                "failures": 0,
                "last_failure": None,
                "risk_factor": 1.0,
            }

        # Update with new metrics
        self.exchanges[exchange].update(metrics)
        self.exchanges[exchange]["last_update"] = time.time()

    def process_execution_result(self, result):
        """Process an execution result to update risk metrics"""
        # Update exchange metrics
        for exchange in [result.buy_exchange, result.sell_exchange]:
            if exchange not in self.exchanges:
                self.exchanges[exchange] = {
                    "success_rate": 1.0,
                    "avg_execution_time": 0,
                    "profit": 0.0,
                    "trade_count": 0,
                    "failures": 0,
                    "last_failure": None,
                    "risk_factor": 1.0,
                }

            self.exchanges[exchange]["trade_count"] += 1
            self.exchanges[exchange]["profit"] += result.profit / 2  # Split profit between exchanges

            if not result.success:
                self.exchanges[exchange]["failures"] += 1
                self.exchanges[exchange]["last_failure"] = time.time()

                # Update success rate
                total = self.exchanges[exchange]["trade_count"]
                failures = self.exchanges[exchange]["failures"]
                self.exchanges[exchange]["success_rate"] = 1.0 - (failures / total if total > 0 else 0)

                # Generate alert for failure
                self.add_alert(
                    "execution_failure",
                    f"Execution failed on {exchange}",
                    {"exchange": exchange, "pair": result.pair, "error": result.error, "timestamp": result.timestamp},
                )

        # Update pair metrics
        pair = result.pair
        if pair not in self.pairs:
            self.pairs[pair] = {"success_rate": 1.0, "avg_profit": 0.0, "trade_count": 0, "failures": 0, "volume": 0.0}

        self.pairs[pair]["trade_count"] += 1
        self.pairs[pair]["volume"] += result.volume

        if result.success:
            # Update average profit
            old_total = self.pairs[pair]["avg_profit"] * (self.pairs[pair]["trade_count"] - 1)
            new_avg = (old_total + result.profit) / self.pairs[pair]["trade_count"]
            self.pairs[pair]["avg_profit"] = new_avg
        else:
            self.pairs[pair]["failures"] += 1

        # Update time-based metrics
        self._update_time_metrics(result)

        # Check for risk level changes
        self.evaluate_risk_level()

    def _update_time_metrics(self, result):
        """Update hourly and daily metrics"""
        hour_key = datetime.now().strftime("%Y-%m-%d-%H")
        day_key = datetime.now().strftime("%Y-%m-%d")

        # Initialize hourly metrics if needed
        if hour_key not in self.hourly_metrics:
            self.hourly_metrics[hour_key] = {
                "profit": 0.0,
                "volume": 0.0,
                "trade_count": 0,
                "success_count": 0,
                "exchanges": {},
                "pairs": {},
            }

        # Initialize daily metrics if needed
        if day_key not in self.daily_metrics:
            self.daily_metrics[day_key] = {
                "profit": 0.0,
                "volume": 0.0,
                "trade_count": 0,
                "success_count": 0,
                "exchanges": {},
                "pairs": {},
            }

        # Update hourly metrics
        hourly = self.hourly_metrics[hour_key]
        hourly["profit"] += result.profit
        hourly["volume"] += result.volume
        hourly["trade_count"] += 1
        if result.success:
            hourly["success_count"] += 1

        # Update exchange metrics in hourly data
        for exchange in [result.buy_exchange, result.sell_exchange]:
            if exchange not in hourly["exchanges"]:
                hourly["exchanges"][exchange] = {"count": 0, "profit": 0.0, "failures": 0}
            hourly["exchanges"][exchange]["count"] += 1
            hourly["exchanges"][exchange]["profit"] += result.profit / 2
            if not result.success:
                hourly["exchanges"][exchange]["failures"] += 1

        # Update pair metrics in hourly data
        if result.pair not in hourly["pairs"]:
            hourly["pairs"][result.pair] = {"count": 0, "profit": 0.0, "volume": 0.0}
        hourly["pairs"][result.pair]["count"] += 1
        hourly["pairs"][result.pair]["profit"] += result.profit
        hourly["pairs"][result.pair]["volume"] += result.volume

        # Update daily metrics (similar to hourly)
        daily = self.daily_metrics[day_key]
        daily["profit"] += result.profit
        daily["volume"] += result.volume
        daily["trade_count"] += 1
        if result.success:
            daily["success_count"] += 1

        # Update exchange metrics in daily data
        for exchange in [result.buy_exchange, result.sell_exchange]:
            if exchange not in daily["exchanges"]:
                daily["exchanges"][exchange] = {"count": 0, "profit": 0.0, "failures": 0}
            daily["exchanges"][exchange]["count"] += 1
            daily["exchanges"][exchange]["profit"] += result.profit / 2
            if not result.success:
                daily["exchanges"][exchange]["failures"] += 1

        # Update pair metrics in daily data
        if result.pair not in daily["pairs"]:
            daily["pairs"][result.pair] = {"count": 0, "profit": 0.0, "volume": 0.0}
        daily["pairs"][result.pair]["count"] += 1
        daily["pairs"][result.pair]["profit"] += result.profit
        daily["pairs"][result.pair]["volume"] += result.volume

        # Clean up old data
        self._clean_time_metrics()

    def _clean_time_metrics(self):
        """Clean up old time-based metrics"""
        now = datetime.now()

        # Keep only last 48 hours of hourly data
        cutoff = (now - timedelta(hours=48)).strftime("%Y-%m-%d-%H")
        self.hourly_metrics = {k: v for k, v in self.hourly_metrics.items() if k >= cutoff}

        # Keep only last 90 days of daily data
        cutoff = (now - timedelta(days=90)).strftime("%Y-%m-%d")
        self.daily_metrics = {k: v for k, v in self.daily_metrics.items() if k >= cutoff}

    def add_alert(self, alert_type: str, message: str, data: Dict[str, Any] = None):
        """Add a risk alert"""
        alert = {"type": alert_type, "message": message, "timestamp": time.time(), "data": data or {}}

        self.alerts.append(alert)

        # Log the alert
        logger.warning(f"RISK ALERT: {message}")

        # Trim alerts if we have too many
        if len(self.alerts) > self.max_alerts:
            self.alerts = self.alerts[-self.max_alerts :]

        return alert

    def evaluate_risk_level(self):
        """Evaluate and update the current risk level"""
        # Count recent failures
        recent_failures = 0
        for alert in self.alerts:
            if alert["type"] == "execution_failure" and time.time() - alert["timestamp"] < 3600:  # Last hour
                recent_failures += 1

        # Check strategy performance
        negative_profit_strategies = 0
        for strategy_id, data in self.strategies.items():
            if data["performance"].daily_profit < 0:
                negative_profit_strategies += 1

        # Calculate exchange health
        unhealthy_exchanges = 0
        for exchange, data in self.exchanges.items():
            if data["success_rate"] < 0.9 or data.get("failures", 0) > 5:
                unhealthy_exchanges += 1

        # Determine risk level
        old_level = self.risk_level

        if recent_failures >= 10 or unhealthy_exchanges >= 3:
            self.risk_level = "critical"
        elif recent_failures >= 5 or negative_profit_strategies >= 2:
            self.risk_level = "high"
        elif recent_failures >= 3 or negative_profit_strategies >= 1:
            self.risk_level = "elevated"
        else:
            self.risk_level = "normal"

        # Log risk level change
        if old_level != self.risk_level:
            logger.warning(f"Risk level changed: {old_level} -> {self.risk_level}")
            self.add_alert(
                "risk_level_change",
                f"Risk level changed from {old_level} to {self.risk_level}",
                {"old": old_level, "new": self.risk_level},
            )

        self.last_update = time.time()
        return self.risk_level

    def get_dashboard_data(self):
        """Get a complete snapshot of risk dashboard data"""
        return {
            "risk_level": self.risk_level,
            "last_update": self.last_update,
            "exchanges": self.exchanges,
            "pairs": self.pairs,
            "strategies": {
                id: {
                    "metrics": s["performance"].to_dict(),
                    "risk_config": {
                        k: v
                        for k, v in s["risk_management"].__dict__.items()
                        if not k.startswith("_") and not callable(v)
                    },
                }
                for id, s in self.strategies.items()
            },
            "alerts": self.alerts[-10:],  # Last 10 alerts
            "hourly": {
                k: v
                for k, v in self.hourly_metrics.items()
                if datetime.strptime(k, "%Y-%m-%d-%H") > datetime.now() - timedelta(hours=24)
            },
            "daily": {
                k: v
                for k, v in self.daily_metrics.items()
                if datetime.strptime(k, "%Y-%m-%d") > datetime.now() - timedelta(days=7)
            },
        }

    def save_snapshot(self, filepath: str):
        """Save a snapshot of the dashboard to a file"""
        data = self.get_dashboard_data()

        try:
            with open(filepath, "w") as f:
                json.dump(data, f, indent=2)
            logger.info(f"Saved risk dashboard snapshot to {filepath}")
            return True
        except Exception as e:
            logger.error(f"Failed to save risk dashboard snapshot: {e}")
            return False
