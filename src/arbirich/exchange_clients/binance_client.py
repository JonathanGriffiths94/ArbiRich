import logging

from binance import AsyncClient, BinanceSocketManager

from src.arbirich.exchange_clients.exchange_client import ExchangeClient

logger = logging.getLogger(__name__)


class BinanceClient(ExchangeClient):
    def __init__(self, symbol="BTCUSDT"):
        self.name = "binance"
        self.symbol = symbol.lower()
        self.rest_client = None

    async def connect_rest(self):
        """
        Initialize Binance REST client.
        """
        self.rest_client = await AsyncClient.create()

    async def connect_websocket(self, callback):
        """
        Connect to Binance's WebSocket and listen for price updates.
        Calls the provided callback function with the latest price.
        """
        if not self.rest_client:
            await self.connect_rest()

        bm = BinanceSocketManager(self.rest_client)
        ws = bm.trade_socket(self.symbol)

        async with ws as stream:
            while True:
                msg = await stream.recv()
                if msg and "p" in msg:
                    price = float(msg["p"])
                    await callback(price)

    async def fetch_rest(self):
        """
        Fetch the latest price using Binance's REST API.
        Returns:
            float: the latest price.
        """
        try:
            if not self.rest_client:
                await self.connect_rest()

            response = await self.rest_client.get_symbol_ticker(
                symbol=self.symbol.upper()
            )
            price = float(response["price"])
            logger.info(f"Fetched {self.symbol.upper()} price via REST: {price}")
            return price
        except Exception as e:
            logger.error(f"Exception fetching price via REST: {e}")
            return None

    async def close(self):
        """
        Close the Binance REST client session.
        """
        if self.rest_client:
            await self.rest_client.close()
