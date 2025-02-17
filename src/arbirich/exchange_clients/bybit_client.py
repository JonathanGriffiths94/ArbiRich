import asyncio
import logging

from pybit.unified_trading import HTTP, WebSocket

from src.arbirich.exchange_clients.exchange_client import ExchangeClient

logger = logging.getLogger(__name__)


class BybitClient(ExchangeClient):
    def __init__(self, symbol="BTCUSDT"):
        self.name = "bybit"
        self.symbol = symbol
        self.ws_client = WebSocket(testnet=False, channel_type="spot")
        self.rest_client = HTTP()

    async def connect_websocket(self, callback):
        """
        Connect to Bybit's WebSocket and listen for price updates.
        Calls the provided callback function with the latest price.
        """
        loop = asyncio.get_event_loop()

        def handle_msg(msg):
            logger.debug(f"Received message: {msg}")
            if "data" in msg and "lastPrice" in msg["data"]:
                price = float(msg["data"]["lastPrice"])
                logger.debug(f"Received {self.symbol} price via WebSocket: {price}")

                future = asyncio.run_coroutine_threadsafe(callback(price), loop)
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error executing callback: {e}")

        self.ws_client.ticker_stream(symbol=self.symbol, callback=handle_msg)
        await asyncio.Future()  # Keeps the connection open

    def fetch_rest(self):
        """
        Fetch the latest price using Bybit's REST API.
        Returns:
            float: the latest price.
        """
        try:
            response = self.client.get_tickers(category="spot", symbol=self.symbol)
            if response and "result" in response and "list" in response["result"]:
                price = float(response["result"]["list"][0]["lastPrice"])
                logger.info(f"Fetched {self.symbol} price via REST: {price}")
                return price
            else:
                logger.error(f"Error fetching price via REST: {response}")
                return None
        except Exception as e:
            logger.error(f"Exception fetching price via REST: {e}")
            return None
