import asyncio
import logging
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI

from src.arbirich.exchange_clients.binance_client import BinanceClient
from src.arbirich.exchange_clients.bybit_client import BybitClient
from src.arbirich.exchange_clients.kucoin_client import KuCoinClient
from src.arbirich.routers import health
from src.arbirich.services.price_service import PriceService

logger = logging.getLogger(__name__)
API_VERSION: str = "v1"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize exchange clients
    bybit_client = BybitClient("ETHUSDT")
    # binance_client = BinanceClient("XRPUSDT")
    # kucoin_client = KuCoinClient("XRP-USDT")
    price_service = PriceService([bybit_client])
    app.state.price_service = price_service

    # Start WebSocket connections
    websocket_task = asyncio.create_task(price_service.start_websocket_clients())

    yield

    # Shutdown
    await price_service.stop_websocket_clients()
    websocket_task.cancel()
    with suppress(asyncio.CancelledError):
        await websocket_task


def make_app() -> FastAPI:
    app = FastAPI(title="ArbiRich", version="0.0.1", lifespan=lifespan)

    app.include_router(health.router, prefix="/health")
    return app
