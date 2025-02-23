import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional, Tuple

from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.operators import filter, input, map, output, stateful_map
from bytewax.run import cli_main

from src.arbirich.market_data_service import MarketDataService

logger = logging.getLogger(__name__)
logger.setLevel("INFO")

redis_client = MarketDataService()


class RedisPricePartition(StatefulSourcePartition):
    def __init__(self, redis_client):
        self.redis_client = redis_client
        self._queue = asyncio.Queue()
        # Start the background task that reads from the async generator.
        self._task = asyncio.create_task(self._run_subscription())
        self._running = True
        self._last_activity = time.time()

    async def _run_subscription(self) -> None:
        # Iterate over the async generator from your Redis client.
        async for price in self.redis_client.subscribe_to_prices():
            if price is not None:  # Only queue valid price updates
                await self._queue.put(price)

    def next_batch(self) -> list:
        # This method is synchronous; it attempts to retrieve an item from the asyncio.Queue.
        try:
            item = self._queue.get_nowait()
            self._last_activity = time.time()
            logger.debug(f"next_batch obtained: {item}")
            return [item]
        except asyncio.QueueEmpty:
            return []

    def snapshot(self) -> dict:
        # Return an empty snapshot.
        return {}


class RedisPriceSource(FixedPartitionedSource):
    def __init__(self, redis_client):
        self.redis_client = redis_client

    def list_parts(self) -> list[str]:
        # We use a single partition for simplicity.
        parts = ["1"]
        logger.info(f"RedisPriceSource partition keys: {parts}")
        return parts

    def build_part(self, step_id, for_key, _resume_state):
        logger.info(f"Building RedisPricePartition for key: {for_key}")
        return RedisPricePartition(self.redis_client)


@dataclass
class AssetPriceState:
    prices: Dict[str, float] = field(default_factory=dict)
    timestamps: Dict[str, float] = field(default_factory=dict)


class ArbitrageFlow:
    def __init__(self, redis_client, arbitrage_threshold=0.00005):
        self.redis_client = redis_client
        self.ARBITRAGE_THRESHOLD = arbitrage_threshold
        self.debounce_state: Dict[str, Tuple[float, dict]] = {}

    def build_flow(self) -> Dataflow:
        logger.info("Building arbitrage flow...")
        flow = Dataflow("arbitrage")
        # Create price source
        price_source = RedisPriceSource(self.redis_client)
        # Input stream
        stream = input("redis_prices", flow, price_source)
        logger.debug("Input stream created from RedisPriceSource.")
        # Key by asset
        stream = map("key_by_asset", stream, lambda x: (x[1], (x[0], x[1], x[2])))
        # Update state
        stream = stateful_map(
            "asset_state", stream, lambda state, x: self._update_state(state, x)
        )
        # Filter for valid states
        stream = filter(
            "ready_state",
            stream,
            lambda x: x[1] is not None,
        )
        # Detect arbitrage
        stream = map(
            "detect_arbitrage", stream, lambda x: self._detect_arbitrage(x[0], x[1])
        )
        # Filter valid opportunities
        stream = filter(
            "valid_opp",
            stream,
            lambda x: x is not None,
        )
        # Debounce
        stream = map(
            "debounce",
            stream,
            self._debounce_opportunity,
        )
        # Final filter
        stream = filter(
            "final_filter",
            stream,
            lambda x: x is not None,
        )
        # Output
        output("push_opp", stream, lambda x: self._push_trade_opportunity(x))
        logger.info("Arbitrage flow built successfully.")
        return flow

    def _update_state(
        self, state: Optional[AssetPriceState], data: Tuple[str, Tuple[str, str, float]]
    ) -> Tuple[AssetPriceState, Optional[AssetPriceState]]:
        if state is None:
            state = AssetPriceState()

        exchange, asset, price = data[1]  # Unpack the inner tuple
        state.prices[exchange] = price
        state.timestamps[exchange] = time.time()

        if len(state.prices) >= 2:
            return state, state
        return state, None

    def _detect_arbitrage(self, asset: str, state: AssetPriceState) -> Optional[dict]:
        if len(state.prices) < 2:
            return None

        buy_exchange = min(state.prices, key=lambda ex: state.prices[ex])
        sell_exchange = max(state.prices, key=lambda ex: state.prices[ex])
        buy_price = state.prices[buy_exchange]
        sell_price = state.prices[sell_exchange]
        spread = (sell_price - buy_price) / buy_price

        if spread > self.ARBITRAGE_THRESHOLD:
            return {
                "asset": asset,
                "buy_exchange": buy_exchange,
                "sell_exchange": sell_exchange,
                "buy_price": buy_price,
                "sell_price": sell_price,
                "spread": spread,
                "timestamp": datetime.utcnow().isoformat(),
            }
        return None

    def _debounce_opportunity(self, opportunity: dict) -> Optional[dict]:
        if not opportunity:
            return None

        asset = opportunity["asset"]
        now = time.time()

        if asset in self.debounce_state:
            last_time, last_opp = self.debounce_state[asset]
            if (
                abs(opportunity["buy_price"] - last_opp["buy_price"])
                / last_opp["buy_price"]
                < 0.001
                and (now - last_time) < 30
            ):
                return None

        self.debounce_state[asset] = (now, opportunity)
        return opportunity

    def _push_trade_opportunity(self, opportunity: dict) -> dict:
        try:
            self.redis_client.store_trade_opportunity(opportunity)
            logger.info(f"Pushed arbitrage opportunity: {json.dumps(opportunity)}")
            return opportunity
        except Exception as e:
            logger.error(f"Error pushing opportunity: {e}")
            return None


async def run_arbitrage_flow(redis_client):
    try:
        logger.info("Starting arbitrage detection pipeline...")
        flow_manager = ArbitrageFlow(redis_client)
        flow = flow_manager.build_flow()

        # Run the Bytewax flow
        execution_task = asyncio.create_task(
            asyncio.to_thread(cli_main, flow, workers_per_process=1)
        )

        # Wait for the flow to complete or be cancelled
        try:
            await execution_task
        except asyncio.CancelledError:
            logger.info("Flow execution cancelled")
            raise

    except Exception as e:
        logger.error(f"Error in arbitrage flow: {e}")
        raise
    finally:
        logger.info("Arbitrage flow shutdown")
        await redis_client.close()


if __name__ == "__main__":

    async def main():
        try:
            await run_arbitrage_flow(redis_client)
        finally:
            await redis_client.close()

    asyncio.run(main())
