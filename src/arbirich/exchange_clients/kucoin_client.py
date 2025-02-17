import asyncio
import logging

from kucoin.client import Market, WsToken
from kucoin.ws_client import KucoinWsClient

from src.arbirich.exchange_clients.exchange_client import ExchangeClient

logger = logging.getLogger(__name__)


class KuCoinClient(ExchangeClient):
    def __init__(self, symbol="BTC-USDT"):
        self.name = "kucoin"
        self.symbol = symbol
        self.ws_client = WsToken()
        self.rest_client = Market(url="https://api.kucoin.com")

    async def connect_websocket(self, callback):
        """
        Connect to KuCoin's WebSocket and listen for price updates.
        Calls the provided callback function with the latest price.
        """

        async def handle_msg(msg):
            if msg["topic"] == f"/market/ticker:{self.symbol}":
                price = float(msg["data"]["price"])
                if callback:
                    await callback(price)

        ws_client = await KucoinWsClient.create(
            None, self.ws_client, handle_msg, private=False
        )

        await ws_client.subscribe(f"/market/ticker:{self.symbol}")
        while True:
            await asyncio.sleep(10)

    async def fetch_rest(self):
        """
        Fetch the latest price using KuCoin's REST API.
        Returns:
            float: the latest price.
        """
        try:
            response = self.rest_client.get_ticker(self.symbol)
            price = float(response["price"])
            logger.info(f"Fetched {self.symbol} price via REST: {price}")
            return price
        except Exception as e:
            logger.error(f"Exception fetching price via REST: {e}")
            return None
