import logging
import time
import uuid
from typing import Dict, Optional, Tuple

from pydantic import ValidationError

from src.arbirich.models.models import (
    OrderBookState,
    OrderBookUpdate,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def key_by_asset(record: dict) -> Optional[Tuple[str, OrderBookUpdate]]:
    try:
        asset = record["symbol"]
        update_order = OrderBookUpdate(**record)
        logger.debug(f"Asset: {asset}")
        logger.debug(f"Update order: {update_order}")
        return asset, update_order
    except KeyError as e:
        logger.error(f"Missing key in key_by_asset: {e}, record: {record}")
        return None
    except ValidationError as e:
        logger.error(f"Validation error in key_by_asset: {e.json()}, record: {record}")
        return None


def update_asset_state(
    state: Optional[OrderBookState], new_data: OrderBookUpdate
) -> Tuple[OrderBookState, Optional[OrderBookState]]:
    logger.debug(f"New data: {new_data}")
    if state is None:
        state = OrderBookState()
    logger.debug(f"state: {state}")
    try:
        asset = new_data.symbol
        exchange = new_data.exchange
    except Exception as e:
        logger.error(f"Error unpacking new_data: {e}")
        return state, None
    logger.debug(f"state: {state}")
    logger.debug(f"asset: {asset}")
    # Initialize the nested state for this asset if it doesn't exist.
    if asset not in state.symbols:
        state.symbols[asset] = {}
    # Initialize the sub-state for this exchange if it doesn't exist.
    if exchange not in state.symbols[asset]:
        state.symbols[asset][exchange] = OrderBookUpdate(
            exchange=exchange, symbol=asset, bids=[], asks=[], timestamp=0.0
        )
    logger.debug(f"state: {state}")
    try:
        # Update the exchange-specific order book for the asset.
        state.symbols[asset][exchange].bids = new_data.id
        state.symbols[asset][exchange].bids = new_data.bids
        state.symbols[asset][exchange].asks = new_data.asks
        state.symbols[asset][exchange].timestamp = new_data.timestamp
    except KeyError as e:
        logger.error(f"Missing data key in update_asset_state: {e}, data: {new_data}")
        return state, None

    logger.debug(f"State for asset {asset} after update: {state.symbols[asset]}")
    return state, state


def find_arbitrage_opportunities(state_and_input: Tuple[Dict, Tuple[str, str]]) -> Tuple[Dict, Optional[Dict]]:
    """
    Find arbitrage opportunities across exchanges.

    Parameters:
        state_and_input: A tuple containing:
            - state: The current state dictionary with order books
            - input: A tuple of (exchange, message) from the source

    Returns:
        A tuple of:
            - updated state
            - opportunity dictionary or None
    """
    state, message = state_and_input

    try:
        # Extract state variables
        state_dict = state.get("state", {})
        strategy = state.get("strategy")
        threshold = state.get("threshold", 0.001)

        # Extract input data
        exchange, order_book_data = message

        # Add current timestamp for debugging
        current_time = time.time()
        logger.debug(f"Processing order book from {exchange} at {current_time}")

        # If the order book data is already an OrderBookUpdate, use it directly
        if isinstance(order_book_data, OrderBookUpdate):
            order_book = order_book_data
        else:
            # Otherwise, assume it's a dictionary and convert it
            try:
                order_book = OrderBookUpdate(
                    exchange=exchange,
                    symbol=order_book_data.get("symbol"),
                    bids=order_book_data.get("bids", []),
                    asks=order_book_data.get("asks", []),
                    timestamp=order_book_data.get("timestamp", current_time),
                )
            except Exception as e:
                logger.error(f"Error creating OrderBookUpdate: {e}, data: {order_book_data}")
                return state, None

        # Don't process older order book updates (if timestamp is present)
        if order_book.symbol in state_dict:
            for ex, ob in state_dict[order_book.symbol].items():
                if ex == exchange and hasattr(ob, "timestamp") and hasattr(order_book, "timestamp"):
                    if ob.timestamp > order_book.timestamp:
                        logger.debug(f"Skipping older order book update for {exchange}:{order_book.symbol}")
                        return state, None

        # Update state with new order book
        if order_book.symbol not in state_dict:
            state_dict[order_book.symbol] = {}

        state_dict[order_book.symbol][exchange] = order_book

        # Check if we have at least two exchanges for this symbol to compare
        if order_book.symbol in state_dict and len(state_dict[order_book.symbol]) >= 2:
            # Find best bid and ask across exchanges
            best_bid = {"price": -1, "exchange": None}
            best_ask = {"price": float("inf"), "exchange": None}

            symbol_data = state_dict[order_book.symbol]

            for ex, ob in symbol_data.items():
                # Skip if order book doesn't have bids or asks
                if not hasattr(ob, "bids") or not hasattr(ob, "asks") or not ob.bids or not ob.asks:
                    continue

                # Find the highest bid
                highest_bid_price = max(ob.bids, key=lambda b: b.price, default=None)
                if highest_bid_price and highest_bid_price.price > best_bid["price"]:
                    best_bid["price"] = highest_bid_price.price
                    best_bid["exchange"] = ex
                    best_bid["quantity"] = highest_bid_price.quantity

                # Find the lowest ask
                lowest_ask_price = min(ob.asks, key=lambda a: a.price, default=None)
                if lowest_ask_price and lowest_ask_price.price < best_ask["price"]:
                    best_ask["price"] = lowest_ask_price.price
                    best_ask["exchange"] = ex
                    best_ask["quantity"] = lowest_ask_price.quantity

            # Check if we found valid best bid and ask from different exchanges
            if best_bid["exchange"] and best_ask["exchange"] and best_bid["exchange"] != best_ask["exchange"]:
                # Calculate spread
                spread = (best_bid["price"] - best_ask["price"]) / best_ask["price"]

                # Check if spread exceeds threshold
                if spread > threshold:
                    # Calculate the volume as the minimum of the bid and ask quantities
                    volume = min(best_bid.get("quantity", 0), best_ask.get("quantity", 0))

                    # Create opportunity
                    opportunity = {
                        "id": str(uuid.uuid4()),
                        "strategy": strategy,
                        "pair": order_book.symbol,
                        "buy_exchange": best_ask["exchange"],
                        "sell_exchange": best_bid["exchange"],
                        "buy_price": best_ask["price"],
                        "sell_price": best_bid["price"],
                        "spread": spread,
                        "volume": volume,
                        "opportunity_timestamp": current_time,
                    }

                    logger.info(f"Found opportunity: {opportunity}")

                    return {"state": state_dict, "strategy": strategy, "threshold": threshold}, opportunity

        # No opportunity found
        return {"state": state_dict, "strategy": strategy, "threshold": threshold}, None

    except Exception as e:
        logger.error(f"Error processing arbitrage opportunity: {e}")
        # Return original state and no opportunity
        return state, None


def format_opportunity(opportunity: Dict) -> Optional[Dict]:
    """
    Format the opportunity for output.

    Parameters:
        opportunity: The opportunity dictionary

    Returns:
        Formatted opportunity or None
    """
    if not opportunity:
        return None

    try:
        # Add formatted log message
        logger.info(
            f"Arbitrage opportunity: {opportunity['pair']}: "
            f"Buy from {opportunity['buy_exchange']} at {opportunity['buy_price']}, "
            f"Sell on {opportunity['sell_exchange']} at {opportunity['sell_price']}, "
            f"Spread: {opportunity['spread']:.4%}, Volume: {opportunity['volume']}"
        )

        return opportunity
    except Exception as e:
        logger.error(f"Error formatting opportunity: {e}")
        return None
