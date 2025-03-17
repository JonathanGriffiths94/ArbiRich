import logging
import time
from typing import Dict, Optional, Tuple

from src.arbirich.models.models import (
    OrderBookState,
    OrderBookUpdate,
    TradeOpportunity,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def detect_arbitrage(
    asset: str, state: OrderBookState, threshold: float, strategy_name: str = "arbitrage"
) -> Optional[TradeOpportunity]:
    """
    Detect arbitrage opportunities for a given asset using the specified strategy.

    Parameters:
        asset: The asset symbol (e.g., 'BTC-USDT')
        state: Current order book state across exchanges
        threshold: Minimum spread threshold to consider as an opportunity
        strategy_name: Name of the strategy to use

    Returns:
        TradeOpportunity object if an opportunity is found, None otherwise
    """
    from src.arbirich.services.strategies.strategy_factory import get_strategy

    try:
        # Get the appropriate strategy
        strategy = get_strategy(strategy_name)

        # Use the strategy to detect arbitrage
        return strategy.detect_arbitrage(asset, state)

    except Exception as e:
        logger.error(f"Error detecting arbitrage with strategy {strategy_name}: {e}")
        return None


def key_by_asset(record) -> Optional[Tuple[str, OrderBookUpdate]]:
    try:
        # Assuming record is always a tuple
        exchange, data = record

        # Handle only if data is already an OrderBookUpdate
        if isinstance(data, OrderBookUpdate):
            asset = data.symbol
            update_order = data

            logger.debug(f"Asset: {asset}")
            logger.debug(f"Update order: {update_order}")
            return asset, update_order
        else:
            logger.error(f"Unsupported data type in key_by_asset: {type(data)}")
            return None
    except Exception as e:
        logger.error(f"Error in key_by_asset: {e}, record: {record}")
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
            exchange=exchange, symbol=asset, bids={}, asks={}, timestamp=0.0
        )
    logger.debug(f"state: {state}")
    try:
        # Update the exchange-specific order book for the asset.
        # Remove the incorrect ID assignment
        state.symbols[asset][exchange].bids = new_data.bids
        state.symbols[asset][exchange].asks = new_data.asks
        state.symbols[asset][exchange].timestamp = new_data.timestamp
    except KeyError as e:
        logger.error(f"Missing data key in update_asset_state: {e}, data: {new_data}")
        return state, None

    logger.debug(f"State for asset {asset} after update: {state.symbols[asset]}")
    return state, state


def find_arbitrage_opportunities(
    state_and_input: Tuple[Dict, Tuple[str, str]],
) -> Tuple[Dict, Optional[OrderBookState]]:
    """
    Backward compatibility wrapper around detect_arbitrage.

    This function converts between the older tuple/dict state model and
    the newer typed model approach with OrderBookState.

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

    # Extract state variables
    state_dict = state.get("state", {})
    strategy = state.get("strategy")
    threshold = state.get("threshold", 0.001)

    # Extract input data
    exchange, order_book_data = message

    # Create or update OrderBookState from the state dict
    book_state = OrderBookState()
    book_state.symbols = state_dict.copy()

    # Update state with new order book data
    order_book = None
    symbol = None

    # Process the incoming order book data
    try:
        # If the order book data is already an OrderBookUpdate, use it directly
        if isinstance(order_book_data, OrderBookUpdate):
            order_book = order_book_data
            symbol = order_book.symbol
        else:
            # Otherwise, assume it's a dictionary and convert it
            symbol = order_book_data.get("symbol")
            if not symbol:
                logger.error("Missing symbol in order book data")
                return state, None

            order_book = OrderBookUpdate(
                exchange=exchange,
                symbol=symbol,
                bids=order_book_data.get("bids", {}),  # Changed from [] to {}
                asks=order_book_data.get("asks", {}),  # Changed from [] to {}
                timestamp=order_book_data.get("timestamp", time.time()),
            )
    except Exception as e:
        logger.error(f"Error processing order book data: {e}")
        return state, None

    # Update state with the new order book
    if symbol not in book_state.symbols:
        book_state.symbols[symbol] = {}
    book_state.symbols[symbol][exchange] = order_book

    # Update the state dict to reflect changes
    state_dict = book_state.symbols

    # Call detect_arbitrage with the updated state
    opportunity = detect_arbitrage(symbol, book_state, threshold, strategy)

    # Return updated state and opportunity
    return {"state": state_dict, "strategy": strategy, "threshold": threshold}, opportunity


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
