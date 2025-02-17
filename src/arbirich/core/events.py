import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.arbirich.exchange_clients.binance_client import BinanceClient
from src.arbirich.exchange_clients.bybit_client import BybitClient
from src.arbirich.exchange_clients.kucoin_client import KuCoinClient
from src.arbirich.services.price_service import PriceService
from src.arbirich.services.websocket_manager import WebSocketManager

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Handles startup and shutdown logic.
    """
    # Initialize exchange clients
    bybit_client = BybitClient("XRPUSDT")
    binance_client = BinanceClient("XRPUSDT")
    kucoin_client = KuCoinClient("XRP-USDT")

    # Initialize PriceService
    price_service = PriceService()
    app.state.price_service = price_service

    # Initialize WebSocket Manager
    ws_manager = WebSocketManager(
        [bybit_client, binance_client, kucoin_client], price_service
    )
    app.state.ws_manager = ws_manager

    # Start WebSocket connections
    await ws_manager.start()

    yield

    # Shutdown WebSocket connections
    await ws_manager.stop()
