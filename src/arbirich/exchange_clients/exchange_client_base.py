from abc import ABC, abstractmethod


class ExchangeClient(ABC):
    @abstractmethod
    async def connect_websocket(self, callback):
        """
        Connect to the exchange's WebSocket endpoint.
        The `callback` function will be called with the latest price data.
        """
        pass

    @abstractmethod
    def fetch_rest(self):
        """
        Fetch the current price using the exchange's REST API.
        Returns:
            float: the latest price.
        """
        pass
