import logging
from typing import Dict, Optional, Tuple

from pydantic import ValidationError

from src.arbirich.models.dtos import (
    AssetPriceState,
    ExchangeOrderBook,
    OrderBookUpdate,
    TradeOpportunity,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def key_by_asset(record: dict) -> Optional[Tuple[str, OrderBookUpdate]]:
    try:
        asset = record["symbol"]
        update_order = OrderBookUpdate(**record)
        # logger.info(f"Asset: {asset}")
        # logger.info(f"Update order: {update_order}")
        return asset, update_order
    except KeyError as e:
        logger.error(f"Missing key in key_by_asset: {e}, record: {record}")
        return None
    except ValidationError as e:
        logger.error(f"Validation error in key_by_asset: {e.json()}, record: {record}")
        return None


def update_asset_state(
    state: Optional[AssetPriceState], new_data: OrderBookUpdate
) -> Tuple[AssetPriceState, Optional[AssetPriceState]]:
    logger.info(f"New data: {new_data}")
    if state is None:
        state = AssetPriceState()

    try:
        asset = new_data.symbol
        exchange = new_data.exchange
    except Exception as e:
        logger.error(f"Error unpacking new_data: {e}")
        return state, None

    # Initialize the nested state for this asset if it doesn't exist.
    if asset not in state.exchanges:
        state.exchanges[asset] = {}

    # Initialize the sub-state for this exchange if it doesn't exist.
    if exchange not in state.exchanges[asset]:
        state.exchanges[asset][exchange] = ExchangeOrderBook(
            bids=[], asks=[], timestamp=0.0
        )

    try:
        # Update the exchange-specific order book for the asset.
        state.exchanges[asset][exchange].bids = new_data.bids
        state.exchanges[asset][exchange].asks = new_data.asks
        state.exchanges[asset][exchange].timestamp = new_data.timestamp
    except KeyError as e:
        logger.error(f"Missing data key in update_asset_state: {e}, data: {new_data}")
        return state, None

    logger.info(f"State for asset {asset} after update: {state.exchanges[asset]}")
    return state, state


def detect_arbitrage(
    asset: str, state: AssetPriceState, threshold: float
) -> Optional[TradeOpportunity]:
    """
    Detect arbitrage opportunities for a given normalized asset (e.g. "BTC_USDT")
    using a nested state structure that stores exchange-specific order book data.

    It compares each pair of exchanges:
      - For one pair, it compares the highest bid on one exchange with the lowest ask on the other.
      - It does the reverse as well.

    If the spread (relative difference) exceeds the threshold, a TradeOpportunity is returned.
    """
    logger.info(f"Detecting arbitrage for asset: {asset} with threshold: {threshold}")

    # Retrieve the order book state for the asset.
    asset_state = state.exchanges.get(asset)
    if asset_state is None:
        logger.warning(f"No state available for asset: {asset}")
        return None

    exchanges = list(asset_state.keys())
    if len(exchanges) < 2:
        logger.info("Not enough exchanges to compare for arbitrage.")
        return None

    # Loop over all distinct exchange pairs.
    for i in range(len(exchanges)):
        for j in range(i + 1, len(exchanges)):
            exch1 = exchanges[i]
            exch2 = exchanges[j]
            ob1 = asset_state[exch1]
            ob2 = asset_state[exch2]

            # First, check arbitrage from exch1 (selling) to exch2 (buying).
            if ob1.bids and ob2.asks:
                top_bid = max(ob1.bids, key=lambda o: o.price)
                top_ask = min(ob2.asks, key=lambda o: o.price)
                logger.info(f"Comparing {exch1} bid {top_bid} vs {exch2} ask {top_ask}")
                if top_bid.price > top_ask.price:
                    spread = (top_bid.price - top_ask.price) / top_ask.price
                    logger.info(f"Spread for {exch1} (bid) vs {exch2} (ask): {spread}")
                    if spread > threshold:
                        # Choose a timestamp from one of the exchanges.
                        ts = ob1.timestamp if ob1.timestamp else ob2.timestamp
                        opp = TradeOpportunity(
                            asset=asset,
                            buy_exchange=exch2,
                            sell_exchange=exch1,
                            buy_price=top_ask.price,
                            sell_price=top_bid.price,
                            spread=spread,
                            volume=min(top_bid.quantity, top_ask.quantity),
                            timestamp=ts,
                        )
                        logger.info(f"Arbitrage Opportunity found: {opp}")
                        return opp

            # Next, check arbitrage from exch2 (selling) to exch1 (buying).
            if ob2.bids and ob1.asks:
                top_bid_rev = max(ob2.bids, key=lambda o: o.price)
                top_ask_rev = min(ob1.asks, key=lambda o: o.price)
                logger.info(
                    f"Comparing {exch2} bid {top_bid_rev} vs {exch1} ask {top_ask_rev}"
                )
                if top_bid_rev.price > top_ask_rev.price:
                    spread_rev = (
                        top_bid_rev.price - top_ask_rev.price
                    ) / top_ask_rev.price
                    logger.info(
                        f"Spread for {exch2} (bid) vs {exch1} (ask): {spread_rev}"
                    )
                    if spread_rev > threshold:
                        ts = ob2.timestamp if ob2.timestamp else ob1.timestamp
                        opp = TradeOpportunity(
                            asset=asset,
                            buy_exchange=exch1,
                            sell_exchange=exch2,
                            buy_price=top_ask_rev.price,
                            sell_price=top_bid_rev.price,
                            spread=spread_rev,
                            volume=min(top_bid_rev.quantity, top_ask_rev.quantity),
                            timestamp=ts,
                        )
                        logger.info(f"Arbitrage Opportunity found: {opp}")
                        return opp

    logger.info("No arbitrage opportunity detected.")
    return None
