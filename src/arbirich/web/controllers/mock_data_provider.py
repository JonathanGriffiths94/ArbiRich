"""
Provides mock data for dashboard and other components when real data is unavailable.
This helps the UI work even when the backend is not fully implemented.
"""

import logging
import random
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class MockDataProvider:
    """Provides mock data for dashboard and other UI components."""

    @staticmethod
    def get_dashboard_stats():
        """Get mock dashboard statistics."""
        return {
            "total_profit": 1250.75,
            "total_trades": 42,
            "win_rate": 78.5,
            "executions_24h": 8,
            "opportunities_count": 156,
            "active_strategies": 3,
        }

    @staticmethod
    def get_profit_chart_data():
        """Get mock profit chart data."""
        return {
            "labels": ["2025-03-18", "2025-03-19", "2025-03-20", "2025-03-21", "2025-03-22", "2025-03-23"],
            "datasets": [
                {
                    "label": "Cumulative Profit",
                    "data": [250, 450, 700, 950, 1100, 1250.75],
                    "borderColor": "#3abe78",
                    "backgroundColor": "rgba(58, 190, 120, 0.1)",
                    "fill": True,
                    "tension": 0.4,
                },
                {
                    "label": "Daily Profit",
                    "data": [250, 200, 250, 250, 150, 150.75],
                    "borderColor": "#85bb65",
                    "backgroundColor": "rgba(133, 187, 101, 0.1)",
                    "fill": True,
                    "tension": 0.4,
                },
            ],
        }

    @staticmethod
    def get_strategies():
        """Get mock strategy data."""
        return [
            {
                "id": 1,
                "name": "Basic Arbitrage",
                "starting_capital": 10000.0,
                "min_spread": 0.5,
                "is_active": True,
                "total_profit": 850.75,
                "total_loss": 120.25,
                "net_profit": 730.50,
                "trade_count": 28,
                "win_rate": 82.14,
                "min_spread_percent": 0.5,
            },
            {
                "id": 2,
                "name": "Conservative",
                "starting_capital": 5000.0,
                "min_spread": 1.0,
                "is_active": True,
                "total_profit": 320.50,
                "total_loss": 80.25,
                "net_profit": 240.25,
                "trade_count": 12,
                "win_rate": 75.0,
                "min_spread_percent": 1.0,
            },
            {
                "id": 3,
                "name": "Aggressive",
                "starting_capital": 2000.0,
                "min_spread": 0.2,
                "is_active": False,
                "total_profit": 195.25,
                "total_loss": 205.50,
                "net_profit": -10.25,
                "trade_count": 18,
                "win_rate": 44.44,
                "min_spread_percent": 0.2,
            },
        ]

    @staticmethod
    def get_opportunities(limit=5):
        """Get mock opportunity data."""
        opportunities = []
        pairs = ["BTC-USDT", "ETH-USDT", "LTC-USDT", "XRP-USDT"]
        exchanges = ["Binance", "Kraken", "Coinbase", "Bybit"]

        for i in range(limit):
            pair = pairs[i % len(pairs)]
            buy_exchange = exchanges[i % len(exchanges)]
            sell_exchange = exchanges[(i + 1) % len(exchanges)]
            spread_percent = round(random.uniform(0.2, 1.5), 2)

            opportunity = {
                "pair": pair,
                "buy_exchange": buy_exchange,
                "sell_exchange": sell_exchange,
                "spread_percent": spread_percent,
                "profit": round(random.uniform(5, 25), 2),
            }
            opportunities.append(opportunity)

        return opportunities

    @staticmethod
    def get_executions(limit=5):
        """Get mock execution data."""
        executions = []
        pairs = ["BTC-USDT", "ETH-USDT", "LTC-USDT", "XRP-USDT"]
        exchanges = ["Binance", "Kraken", "Coinbase", "Bybit"]

        for i in range(limit):
            pair = pairs[i % len(pairs)]
            buy_exchange = exchanges[i % len(exchanges)]
            sell_exchange = exchanges[(i + 1) % len(exchanges)]
            profit = round(random.uniform(-5, 20), 2) if random.random() < 0.8 else round(random.uniform(-20, -5), 2)

            execution = {
                "pair": pair,
                "buy_exchange": buy_exchange,
                "sell_exchange": sell_exchange,
                "profit": profit,
                "created_at": datetime.now() - timedelta(hours=i),
            }
            executions.append(execution)

        return executions


# Initialize the mock data provider
mock_provider = MockDataProvider()
