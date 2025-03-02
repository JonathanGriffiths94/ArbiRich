import asyncio
import logging

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.run import cli_main

from arbirich.sinks.trade_opportunity import (
    debounce_opportunity,
    publish_trade_opportunity,
)
from src.arbirich.config import REDIS_CONFIG, STRATEGIES
from src.arbirich.processing.process_arbitrage import (
    detect_arbitrage,
    key_by_asset,
    update_asset_state,
)
from src.arbirich.redis_manager import MarketDataService
from src.arbirich.sources.redis_price_partition import RedisPriceSource
from src.arbirich.utils.helpers import build_exchanges_dict

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Instantiate the Redis service.
redis_client = MarketDataService(
    host=REDIS_CONFIG["host"], port=REDIS_CONFIG["port"], db=REDIS_CONFIG["db"]
)


# -----------------------------
# Normalization function: extract asset from product
# -----------------------------
# def normalise_asset(exchange: str, product: str) -> str:
#     # For Coinbase, product is typically "BTC-USD", so take the first part.
#     if exchange == "coinbase":
#         return product.split("-")[0]
#     if exchange == "cryptocom":
#         return product.split("_")[0]
#     # For Binance and Bybit, assume the asset code is the first 3 (or 4) letters.
#     # A more robust implementation might use regex or a mapping.
#     if product.startswith("BTC"):
#         return "BTC"
#     elif product.startswith("ETH"):
#         return "ETH"
#     else:
#         return product


# def normalise_asset(exchange: str, product: str, known_assets=None) -> str:
#     """
#     Dynamically extract the asset symbol from a product string.

#     Args:
#         exchange: The exchange name.
#         product: The product symbol string (e.g., "BTC-USD", "BTC_USDT").
#         known_assets: Optional list of known asset symbols. If not provided, a default list is used.

#     Returns:
#         The normalized asset symbol in uppercase.
#     """
#     if known_assets is None:
#         known_assets = [
#             "BTC",
#             "ETH",
#             "ADA",
#             "XRP",
#             "SOL",
#             "DOT",
#             "DOGE",
#             "LTC",
#             "BCH",
#             "LINK",
#         ]
#     tokens = re.split(r"[-_ ]+", product)
#     for token in tokens:
#         token_upper = token.upper()
#         if token_upper in known_assets:
#             return token_upper
#     return tokens[0].upper() if tokens else product.upper()


# # -----------------------------
# # Map input data to a keyed tuple by asset
# # -----------------------------
# def key_by_asset(record: dict) -> Tuple[str, Tuple[str, str, dict]]:
#     """
#     Extracts the asset symbol and returns a tuple (asset, (exchange, product, data)).
#     """
#     try:
#         exchange = record["exchange"]
#         product = record["symbol"]
#         asset = normalise_asset(exchange, product)
#         return asset, (exchange, product, record)
#     except KeyError as e:
#         logger.error(f"Missing key in key_by_asset: {e}, record: {record}")
#         return None


# # -----------------------------
# # Update state for each asset
# # -----------------------------
# def update_asset_state(
#     state: Optional[AssetPriceState], new_data: Tuple[str, str, dict]
# ) -> Tuple[AssetPriceState, Optional[AssetPriceState]]:
#     if state is None:
#         state = AssetPriceState()

#     exchange, _, data = new_data
#     try:
#         state.bids[exchange] = data["bids"]
#         state.asks[exchange] = data["asks"]
#         state.timestamp[exchange] = data["timestamp"]
#     except KeyError as e:
#         logger.error(f"Missing data key in update_asset_state: {e}, data: {data}")
#         return state, None
#     if len(state.prices) > 2:
#         return state, state
#     return state, None


# # -----------------------------
# # Detect arbitrage opportunities
# # -----------------------------
# def detect_arbitrage(asset: str, state: AssetPriceState, threshold: float):
#     logger.info(f"Asset: {asset}, State: {state}")
#     if not state or not state.bids or not state.asks:
#         logger.warning(
#             f"No sufficient order book data for {asset}, skipping arbitrage detection."
#         )
#         return None

#     # Example logic: Compare top bid from one exchange to top ask from another
#     for bid_exchange, (bid_price, bid_quantity) in state.bids.items():
#         for ask_exchange, (ask_price, ask_quantity) in state.asks.items():
#             if bid_exchange != ask_exchange and bid_price > ask_price:
#                 spread = (bid_price - ask_price) / ask_price
#                 if spread > threshold:
#                     logger.info(
#                         f"Arbitrage Opportunity: {asset} - Buy at {ask_exchange} ({ask_price}), "
#                         f"Sell at {bid_exchange} ({bid_price}), Spread: {spread:.2%}"
#                     )
#                     return {
#                         "asset": asset,
#                         "buy_exchange": ask_exchange,
#                         "sell_exchange": bid_exchange,
#                         "buy_price": ask_price,
#                         "sell_price": bid_price,
#                         "spread": spread,
#                         "volume": min(
#                             bid_quantity, ask_quantity
#                         ),  # Potential arbitrage volume
#                     }
#     return None


# def debounce_opportunity(redis_client, opportunity, expiry_seconds=30):
#     """
#     Avoid duplicate arbitrage opportunities by checking Redis.

#     Only emit an opportunity if:
#     - It has a significant price change (0.1% difference)
#     - OR enough time (expiry_seconds) has passed since the last similar opportunity.
#     """
#     try:
#         asset, buy_ex, sell_ex, buy_price, sell_price, spread = opportunity
#         key = f"last_opp:{asset}:{buy_ex}:{sell_ex}"
#         now = time.time()

#         # Retrieve last stored opportunity from Redis
#         last_opp_data = redis_client.redis_client.get(key)

#         if last_opp_data:
#             last_opp_entry = json.loads(last_opp_data)
#             last_time = last_opp_entry.get("timestamp", 0)
#             last_buy_price = last_opp_entry.get("buy_price", 0)
#             last_sell_price = last_opp_entry.get("sell_price", 0)

#             # Skip if opportunity is too similar AND within the debounce period
#             if (
#                 abs(buy_price - last_buy_price) / (last_buy_price + 1e-8) < 0.001
#                 and abs(sell_price - last_sell_price) / (last_sell_price + 1e-8) < 0.001
#                 and (now - last_time) < expiry_seconds
#             ):
#                 logger.debug(f"Skipping duplicate arbitrage opportunity for {asset}")
#                 return None  # Skip duplicate opportunity

#         # Store the new opportunity in Redis
#         redis_client.redis_client.setex(
#             key,
#             expiry_seconds,
#             json.dumps(
#                 {"timestamp": now, "buy_price": buy_price, "sell_price": sell_price}
#             ),
#         )
#         return opportunity  # Return if it's a valid opportunity

#     except Exception as e:
#         logger.error(f"Error in debounce_opportunity: {e}", exc_info=True)
#         return None


# def publish_trade_opportunity(opportunity: dict) -> dict:
#     try:
#         redis_client.publish_trade_opportunity(opportunity)
#         logger.info(f"Published trade opportunity: {json.dumps(opportunity)}")
#         return opportunity
#     except Exception as e:
#         logger.error(f"Error pushing opportunity: {e}")
#         return None


def build_arbitrage_flow():
    """
    Build the arbitrage flow.
    This flow uses a custom Redis source that yields trade opportunities,
    applies the execute_trade operator, and outputs the result.
    """
    logger.info("Building arbitrage flow...")
    flow = Dataflow("arbitrage")

    # Use the custom RedisPriceSource.
    exchanges = build_exchanges_dict()
    exchange_channels = {exchange: "order_book" for exchange in exchanges.keys()}

    source = RedisPriceSource(exchange_channels)
    stream = op.input("redis_input", flow, source)
    logger.debug("Input stream created from RedisPriceSource.")

    keyed_stream = op.map("key_by_asset", stream, key_by_asset)
    asset_state_stream = op.stateful_map("asset_state", keyed_stream, update_asset_state)

    # Filter not ready states
    ready_state = op.filter("ready", asset_state_stream, lambda kv: kv[1] is not None)

    # Detect arbitrage on the state (the key is asset).
    arb_stream = op.map(
        "detect_arbitrage",
        ready_state,
        lambda kv: detect_arbitrage(kv[0], kv[1], STRATEGIES["arbitrage"]["threshold"]),
    )

    # Filter out None opportunities from detect_arbitrage
    arb_opportunities = op.filter("arb_filter", arb_stream, lambda x: x is not None)

    debounced_opportunities = op.map(
        "debounce_opportunity",
        arb_opportunities,
        lambda x: debounce_opportunity(redis_client, x),
    )

    # Filter out None values from debouncer
    final_opp = op.filter(
        "final_filter", debounced_opportunities, lambda x: x is not None
    )

    redis_sync = op.map("push_trade_opportunity", final_opp, publish_trade_opportunity)

    op.output("stdout", redis_sync, StdOutSink())
    logger.info("Output operator attached (StdOutSink).")
    return flow


async def run_arbitrage_flow():
    try:
        logger.info("Starting arbitrage pipeline...")
        flow = build_arbitrage_flow()

        logger.info("Running cli_main in a separate thread.")
        execution_task = asyncio.create_task(
            asyncio.to_thread(cli_main, flow, workers_per_process=1)
        )

        # Allow interruption to propagate
        try:
            await execution_task
        except asyncio.CancelledError:
            logger.info("arbitrage task cancelled")
            raise
        logger.info("cli_main has finished running.")
    except asyncio.CancelledError:
        logger.info("Arbitrage task cancelled")
        raise
    finally:
        logger.info("Arbitrage flow shutdown")
        redis_client.close()


# Expose the flow for CLI usage.
flow = build_arbitrage_flow()

if __name__ == "__main__":
    asyncio.run(cli_main(flow, workers_per_process=1))
