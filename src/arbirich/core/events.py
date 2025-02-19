import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import List

from aiokafka import AIOKafkaProducer
from fastapi import APIRouter, FastAPI, WebSocket

from arbirich.services.arbitrage import ArbitrageService
from arbirich.services.data_consumer import consume_market_data
from arbirich.services.price_service import PriceService
from arbirich.services.websocket_producer import WebSocketManager
from src.arbirich.exchange_clients.binance_client import BinanceClient
from src.arbirich.exchange_clients.bybit_client import BybitClient
from src.arbirich.exchange_clients.kucoin_client import KuCoinClient

router = APIRouter()

logger = logging.getLogger(__name__)

active_connections: List[WebSocket] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages startup and shutdown events for the FastAPI application.
    """
    # Initialize exchange clients
    bybit_client = BybitClient("XRPUSDT")
    binance_client = BinanceClient("XRPUSDT")
    kucoin_client = KuCoinClient("XRP-USDT")
    exchange_clients = [bybit_client, binance_client, kucoin_client]

    # Initialize services
    price_service = PriceService()

    # Create Kafka producer for arbitrage alerts
    try:
        alert_producer = AIOKafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await alert_producer.start()
        app.state.alert_producer = alert_producer
    except Exception as e:
        logger.error(f"Failed to start Kafka producer: {e}")
        raise

    arbitrage_service = ArbitrageService(
        price_service=price_service,
        threshold=0.5,
        alert_producer=alert_producer,
        alert_topic="arbitrage_alerts",
    )

    # Initialize and start WebSocketManager (publishing to "market_data")
    try:
        ws_manager = WebSocketManager(
            exchange_clients=exchange_clients,
            kafka_topic="market_data",
            kafka_bootstrap_servers="localhost:9092",
        )
        app.state.ws_manager = ws_manager
        await ws_manager.start()
    except Exception as e:
        logger.error(f"Failed to start WebSocket manager: {e}")
        raise

    # Start the Kafka consumer task to process market data
    market_data_task = asyncio.create_task(
        consume_market_data(price_service, arbitrage_service)
    )

    try:
        yield
    finally:
        market_data_task.cancel()
        with asyncio.suppress(asyncio.CancelledError):
            await market_data_task
        await ws_manager.stop()
        await alert_producer.stop()
