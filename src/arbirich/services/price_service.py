import asyncio
import logging
from contextlib import suppress

logger = logging.getLogger(__name__)


class PriceService:
    def __init__(self, exchange_clients: list):
        self.exchange_clients = exchange_clients
        self.prices = {}
        self.websocket_tasks = []

    async def _update_price(self, client, price):
        self.prices[client.name] = price
        logger.info(f"Updated {client.name} price: {price}")

    async def start_websocket_clients(self):
        for client in self.exchange_clients:
            task = asyncio.create_task(self._run_client_websocket(client))
            self.websocket_tasks.append(task)
        await asyncio.gather(*self.websocket_tasks)

    async def _run_client_websocket(self, client):
        async def price_callback(price):
            await self._update_price(client, price)

        while True:
            try:
                await client.connect_websocket(price_callback)
            except Exception as e:
                logger.error(f"Error in {client.name} WebSocket connection: {e}")
                await asyncio.sleep(5)  # Reconnect after a delay

    async def stop_websocket_clients(self):
        for task in self.websocket_tasks:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
