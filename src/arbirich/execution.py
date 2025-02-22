import logging
import threading
import time

from arbirich.exchange_clients.binance_client import BinanceClient
from arbirich.exchange_clients.bybit_client import BybitClient
from arbirich.exchange_clients.kucoin_client import KuCoinClient
from arbirich.market_data_service import MarketDataService

logger = logging.getLogger(__name__)


class TradeExecutor:
    def __init__(self):
        """Initialize exchange clients and use MarketDataService for all Redis operations."""
        self.market_data_service = (
            MarketDataService()
        )  # Shared instance for Redis operations
        self.clients = {
            "Binance": BinanceClient(),
            "Bybit": BybitClient(),
            "KuCoin": KuCoinClient(),
        }
        self.trade_history = []  # Stores executed trades for monitoring
        self._stop_event = threading.Event()

    def execute_trade(self, trade_signal):
        """Executes a buy and sell order across different exchanges for arbitrage."""
        try:
            buy_exchange = trade_signal["buy_exchange"]
            sell_exchange = trade_signal["sell_exchange"]
            pair = trade_signal["pair"]
            buy_price = trade_signal["buy_price"]
            sell_price = trade_signal["sell_price"]
            quantity = trade_signal.get("quantity", 0.1)  # Default to 0.1 BTC

            # Ensure valid exchange clients exist
            if buy_exchange not in self.clients or sell_exchange not in self.clients:
                logger.error(f"❌ Invalid exchanges: {buy_exchange}, {sell_exchange}")
                return {"error": "Invalid exchanges"}

            buy_client = self.clients[buy_exchange]
            sell_client = self.clients[sell_exchange]

            # Execute buy and sell orders
            logger.info(f"🔵 Buying {quantity} {pair} on {buy_exchange} at {buy_price}")
            buy_order = buy_client.place_order(pair, "buy", buy_price, quantity)
            logger.info(
                f"🔴 Selling {quantity} {pair} on {sell_exchange} at {sell_price}"
            )
            sell_order = sell_client.place_order(pair, "sell", sell_price, quantity)

            # Log successful trades
            trade_data = {
                "buy_order": buy_order,
                "sell_order": sell_order,
                "profit": (sell_price - buy_price) * quantity,
                "timestamp": time.time(),
            }
            # Store trade execution using MarketDataService
            self.market_data_service.store_trade_execution(trade_data)
            self.trade_history.append(trade_data)
            # TODO: Update persistent storage (e.g., database)

            logger.info(f"✅ Arbitrage Trade Executed: {trade_data}")
            return trade_data

        except Exception as e:
            logger.error(f"❌ Trade Execution Failed: {str(e)}")
            return {"error": str(e)}
