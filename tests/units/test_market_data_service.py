import asyncio
import json
from datetime import datetime
from typing import AsyncGenerator, Dict, List
from unittest.mock import AsyncMock, MagicMock

import pytest
import redis.asyncio as redis

from src.arbirich.flows.arbitrage import ArbitrageFlow, AssetPriceState
from src.arbirich.market_data_service import MarketDataService

# Test data
TEST_PRICES = [
    # Scenario 1: Clear arbitrage opportunity
    [
        {"exchange": "binance", "asset": "BTC/USDT", "price": 50000.0},
        {"exchange": "coinbase", "asset": "BTC/USDT", "price": 50500.0},
    ],
    # Scenario 2: No arbitrage (spread too small)
    [
        {"exchange": "binance", "asset": "ETH/USDT", "price": 2000.0},
        {"exchange": "coinbase", "asset": "ETH/USDT", "price": 2001.0},
    ],
]


class TestMarketDataService(MarketDataService):
    """Test version of MarketDataService"""

    def __init__(self):
        self.opportunities = []
        self.published_prices = []

    async def store_trade_opportunity(self, opportunity: Dict):
        self.opportunities.append(opportunity)

    async def publish_price(self, price_data: Dict):
        self.published_prices.append(price_data)


@pytest.fixture
async def redis_client():
    client = redis.Redis(host="localhost", port=6379, decode_responses=True)
    yield client
    await client.close()


@pytest.fixture
def market_data_service():
    return TestMarketDataService()


@pytest.fixture
def arbitrage_flow(market_data_service):
    return ArbitrageFlow(market_data_service, arbitrage_threshold=0.005)


@pytest.mark.asyncio
async def test_clear_arbitrage_detection(market_data_service, arbitrage_flow):
    """Test detection of clear arbitrage opportunity"""
    # Setup price data
    prices = TEST_PRICES[0]

    # Process prices
    for price in prices:
        await market_data_service.publish_price(price)

    # Allow time for processing
    await asyncio.sleep(0.1)

    # Verify arbitrage detection
    assert len(market_data_service.opportunities) >= 1
    opportunity = market_data_service.opportunities[0]
    assert opportunity["buy_exchange"] == "binance"
    assert opportunity["sell_exchange"] == "coinbase"
    assert opportunity["spread"] > 0.005


@pytest.mark.asyncio
async def test_small_spread_ignored(market_data_service, arbitrage_flow):
    """Test that small spreads are ignored"""
    # Setup price data
    prices = TEST_PRICES[1]

    # Process prices
    for price in prices:
        await market_data_service.publish_price(price)

    # Allow time for processing
    await asyncio.sleep(0.1)

    # Verify no arbitrage detected
    assert len(market_data_service.opportunities) == 0


@pytest.mark.asyncio
async def test_price_updates(market_data_service, arbitrage_flow):
    """Test handling of price updates"""
    prices = [
        {"exchange": "binance", "asset": "BTC/USDT", "price": 50000.0},
        {"exchange": "binance", "asset": "BTC/USDT", "price": 50100.0},  # Update
        {"exchange": "coinbase", "asset": "BTC/USDT", "price": 50600.0},
    ]

    for price in prices:
        await market_data_service.publish_price(price)

    await asyncio.sleep(0.1)

    assert len(market_data_service.opportunities) >= 1
    opportunity = market_data_service.opportunities[-1]
    assert opportunity["buy_exchange"] == "binance"
    assert opportunity["sell_exchange"] == "coinbase"
    assert opportunity["buy_price"] == 50100.0


@pytest.mark.asyncio
async def test_multiple_assets(market_data_service, arbitrage_flow):
    """Test handling multiple assets simultaneously"""
    prices = [
        {"exchange": "binance", "asset": "BTC/USDT", "price": 50000.0},
        {"exchange": "binance", "asset": "ETH/USDT", "price": 2000.0},
        {"exchange": "coinbase", "asset": "BTC/USDT", "price": 50500.0},
        {"exchange": "coinbase", "asset": "ETH/USDT", "price": 2001.0},
    ]

    for price in prices:
        await market_data_service.publish_price(price)

    await asyncio.sleep(0.1)

    btc_opps = [
        opp for opp in market_data_service.opportunities if opp["asset"] == "BTC/USDT"
    ]
    eth_opps = [
        opp for opp in market_data_service.opportunities if opp["asset"] == "ETH/USDT"
    ]

    assert len(btc_opps) >= 1
    assert len(eth_opps) == 0  # Spread too small


@pytest.mark.asyncio
async def test_error_handling(market_data_service, arbitrage_flow):
    """Test handling of invalid price data"""
    invalid_prices = [
        {"exchange": "binance"},  # Missing price
        {"asset": "BTC/USDT", "price": 50000.0},  # Missing exchange
        {"exchange": "binance", "asset": "BTC/USDT", "price": "invalid"},  # Invalid price
    ]

    for price in invalid_prices:
        await market_data_service.publish_price(price)

    await asyncio.sleep(0.1)

    assert len(market_data_service.opportunities) == 0


# Optional: Test Redis integration if needed
@pytest.mark.asyncio
async def test_redis_integration(redis_client, market_data_service, arbitrage_flow):
    """Test actual Redis pub/sub integration"""
    prices = TEST_PRICES[0]

    # Publish prices to Redis
    for price in prices:
        await redis_client.publish("prices", json.dumps(price))

    await asyncio.sleep(0.1)

    # Verify processing through Redis
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("prices")

    messages = []
    for _ in range(len(prices)):
        message = await pubsub.get_message(timeout=1.0)
        if message and message["type"] == "message":
            messages.append(json.loads(message["data"]))

    assert len(messages) == len(prices)
    await pubsub.unsubscribe()
