import asyncio
import logging
from contextlib import suppress

from src.arbirich.services.arbitrage_service import ArbitrageService
from src.arbirich.services.price_service import PriceService

logger = logging.getLogger(__name__)


class WebSocketManager:
    def __init__(
        self,
        exchange_clients: list,
        price_service: PriceService,
        arbitrage_service: ArbitrageService,
    ):
        self.exchange_clients = exchange_clients
        self.price_service = price_service
        self.arbitrage_service = arbitrage_service
        self.websocket_tasks = []

    async def start(self):
        """
        Start WebSocket connections for all exchange clients.
        """
        logger.info("Starting WebSocket connections...")
        for client in self.exchange_clients:
            task = asyncio.create_task(self._run_client(client))
            self.websocket_tasks.append(task)

    async def _run_client(self, client):
        """
        Runs WebSocket connection for a single exchange with reconnection handling.
        """

        async def price_callback(price):
            await self.price_service.update_price(
                client.name, price
            )  # Store price update
            self.arbitrage_service.detect_opportunity()  # Check for arbitrage opportunities

        delay = 1  # Initial delay for backoff

        while True:
            try:
                logger.info(f"Connecting {client.name} WebSocket...")
                await client.connect_websocket(price_callback)
                logger.info(f"WebSocket for {client.name} connected successfully.")
                delay = 1  # Reset delay on successful connection
            except Exception as e:
                logger.error(f"Error in {client.name} WebSocket: {e}")

                # Exponential backoff for reconnecting
                wait_time = min(delay * 2, 60) + (asyncio.random() * 2)
                logger.info(
                    f"Retrying {client.name} WebSocket in {wait_time:.2f} seconds..."
                )
                await asyncio.sleep(wait_time)

    async def stop(self):
        """
        Stop all WebSocket connections.
        """
        logger.info("Stopping WebSocket connections...")
        for task in self.websocket_tasks:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        logger.info("All WebSocket connections stopped.")
