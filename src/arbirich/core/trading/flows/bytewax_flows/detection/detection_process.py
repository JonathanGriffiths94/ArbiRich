import logging
import time
from typing import Dict, Optional, Tuple

from src.arbirich.core.trading.strategy.strategy_factory import get_strategy
from src.arbirich.models.models import (
    OrderBookState,
    OrderBookUpdate,
    TradeOpportunity,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def detect_arbitrage(
    asset: str, state: OrderBookState, threshold: float, strategy_name: str
) -> Optional[TradeOpportunity]:
    """
    Detect arbitrage opportunities using the specified strategy.

    Parameters:
        asset: Asset symbol to check for opportunities
        state: Current order book state
        threshold: Minimum profit threshold
        strategy_name: Name of the strategy to use

    Returns:
        TradeOpportunity object if found, None otherwise
    """
    try:
        # First check if we have data from at least 2 exchanges for this asset
        if asset not in state.symbols or len(state.symbols[asset]) < 2:
            logger.info(
                f"Skipping arbitrage detection for {asset}: Need at least 2 exchanges, have {len(state.symbols.get(asset, {}))}"
            )
            return None

        # Get the strategy instance using the factory
        strategy = get_strategy(strategy_name)

        logger.info(f"Running arbitrage detection for {asset} with {len(state.symbols[asset])} exchanges")
        # Log the exchanges we have data for
        for exchange in state.symbols[asset]:
            book = state.symbols[asset][exchange]
            logger.info(f"  Exchange {exchange}: {len(book.bids)} bids, {len(book.asks)} asks")

        # Use the detect_arbitrage method from the strategy
        opportunity = strategy.detect_arbitrage(asset, state)

        if opportunity:
            logger.info(
                f"Found opportunity for {asset} using {strategy_name}: "
                f"Buy from {opportunity.buy_exchange} at {opportunity.buy_price:.2f}, "
                f"Sell on {opportunity.sell_exchange} at {opportunity.sell_price:.2f}, "
                f"Spread: {opportunity.spread:.4%}"
            )

            # Convert Pydantic model to dict for bytewax processing
            if hasattr(opportunity, "model_dump"):
                return opportunity.model_dump()
            elif hasattr(opportunity, "dict"):
                return opportunity.dict()
            else:
                return opportunity

        return None

    except Exception as e:
        logger.error(f"Error detecting arbitrage for {asset} with {strategy_name}: {e}", exc_info=True)
        return None


def key_by_asset(record: Dict) -> Tuple[str, Dict]:
    """Group order book updates by asset symbol"""
    try:
        # Assuming record is always a tuple
        exchange, data = record

        # Handle only if data is already an OrderBookUpdate
        if isinstance(data, OrderBookUpdate):
            asset = data.symbol
            update_order = data

            # Ensure we have a valid asset key - don't return None
            if not asset:
                logger.error(f"Missing symbol in OrderBookUpdate from {exchange}: {data}")
                # Try to extract from exchange:symbol format
                if ":" in exchange:
                    _, extracted_symbol = exchange.split(":", 1)
                    asset = extracted_symbol
                    logger.info(f"Extracted symbol from exchange identifier: {asset}")
                else:
                    logger.error("Cannot extract symbol, using 'UNKNOWN' as fallback")
                    asset = "UNKNOWN"  # Use a fallback value instead of None

            logger.info(f"Grouped data by asset: {asset}")
            logger.debug(f"Update order: {update_order}")
            return asset, update_order
        else:
            logger.error(f"Unsupported data type in key_by_asset: {type(data)}")
            return "UNKNOWN", data  # Return a fallback asset name instead of None
    except Exception as e:
        logger.error(f"Error in key_by_asset: {e}, record: {record}")
        return "ERROR", None  # Return an error indicator instead of None


def update_asset_state(key: str, records: list, state: Dict = None) -> Tuple[str, Optional[OrderBookState]]:
    """Update order book state for an asset"""
    # Check if records is a single item or a collection
    if not isinstance(records, (list, tuple)) and records is not None:
        records = [records]  # Convert single item to a list

    logger.info(f"Processing update_asset_state with key: {key}, records count: {len(records) if records else 0}")

    if state is None:
        logger.info("Creating new OrderBookState (empty state)")
        state = OrderBookState()
    else:
        # Detailed logging of initial state
        logger.info(f"INITIAL STATE: Contains {len(state.symbols)} symbols")
        for symbol, exchanges in state.symbols.items():
            logger.info(f"  • Symbol {symbol} has {len(exchanges)} exchanges: {list(exchanges.keys())}")
            # Log details about each exchange's order book
            for exch, book in exchanges.items():
                bid_count = len(book.bids) if hasattr(book, "bids") else 0
                ask_count = len(book.asks) if hasattr(book, "asks") else 0
                timestamp = getattr(book, "timestamp", "unknown")
                logger.debug(f"    → {exch}: {bid_count} bids, {ask_count} asks, ts: {timestamp}")

    # Return early if no records - don't error out
    if not records:
        logger.warning(f"No records provided for asset {key}, returning current state")
        return key, state

    # Default asset name if key is None or invalid
    asset = key if key and key != "None" else "UNKNOWN"

    try:
        logger.info(f"Starting updates for asset: {asset}")

        for i, new_data in enumerate(records):
            logger.info(f"Processing record {i + 1}/{len(records)} for {asset}")

            # Handle case where new_data is a tuple (exchange, order_book)
            if isinstance(new_data, tuple) and len(new_data) == 2:
                exchange, order_book = new_data
                logger.info(f"Unpacked tuple: exchange={exchange}, order_book type={type(order_book)}")
            else:
                # Assume it's an OrderBookUpdate object directly
                try:
                    exchange = new_data.exchange
                    order_book = new_data
                    logger.info(f"Using direct object: exchange={exchange}, order_book type={type(order_book)}")
                except AttributeError:
                    logger.error(f"Data is not a tuple or OrderBookUpdate: {type(new_data)}")
                    logger.error(f"Data content: {new_data}")
                    continue

            # Log existing data for this exchange if it exists
            if asset in state.symbols and exchange in state.symbols[asset]:
                existing_book = state.symbols[asset][exchange]
                bid_count = len(existing_book.bids) if hasattr(existing_book, "bids") else 0
                ask_count = len(existing_book.asks) if hasattr(existing_book, "asks") else 0
                logger.info(f"BEFORE UPDATE: {exchange}:{asset} has {bid_count} bids, {ask_count} asks")
            else:
                logger.info(f"BEFORE UPDATE: No existing data for {exchange}:{asset}")

            # Extract symbol from order book and use it as the asset key if available
            extracted_symbol = getattr(order_book, "symbol", None)

            # If we have a valid symbol from the data, use it to override the key
            if extracted_symbol and extracted_symbol != "None":
                asset = extracted_symbol
                logger.info(f"Using symbol from data: {asset} (original key: {key})")

            # Fallback if asset is still None or invalid
            if not asset or asset == "None":
                logger.error("No valid asset key found, using exchange identifier")
                if ":" in exchange:
                    _, extracted_symbol = exchange.split(":", 1)
                    asset = extracted_symbol
                    logger.info(f"Extracted asset from exchange identifier: {asset}")
                else:
                    asset = "UNKNOWN"
                    logger.error("Cannot determine asset, using 'UNKNOWN'")

            # Initialize the nested state for this asset if it doesn't exist
            if asset not in state.symbols:
                logger.info(f"Initializing NEW state for asset: {asset}")
                state.symbols[asset] = {}

            # Log what exchanges currently exist for this asset
            logger.info(f"CURRENT EXCHANGES for {asset}: {list(state.symbols[asset].keys())}")

            # Initialize the sub-state for this exchange if it doesn't exist
            if exchange not in state.symbols[asset]:
                logger.info(f"Initializing NEW state for exchange: {exchange} in asset: {asset}")
                # Make sure we always provide a valid symbol string
                state.symbols[asset][exchange] = OrderBookUpdate(
                    exchange=exchange,
                    symbol=asset,  # Use the determined asset name as symbol
                    bids={},
                    asks={},
                    timestamp=0.0,
                )

            # Update the exchange-specific order book for the asset
            try:
                logger.info(f"Updating order book data for {exchange}:{asset}")
                # Log the new data being added
                bid_count = len(order_book.bids) if hasattr(order_book, "bids") else 0
                ask_count = len(order_book.asks) if hasattr(order_book, "asks") else 0
                logger.info(f"ADDING: {exchange}:{asset} with {bid_count} bids, {ask_count} asks")

                state.symbols[asset][exchange].bids = order_book.bids
                state.symbols[asset][exchange].asks = order_book.asks
                state.symbols[asset][exchange].timestamp = order_book.timestamp
                logger.info(f"Updated order book: bids={len(order_book.bids)}, asks={len(order_book.asks)}")
            except Exception as e:
                logger.error(f"Error updating order book attributes: {e}")
                # Create a new OrderBookUpdate with all required fields from the order_book
                logger.info(f"Creating new OrderBookUpdate for {exchange}:{asset}")
                state.symbols[asset][exchange] = OrderBookUpdate(
                    exchange=exchange,
                    symbol=asset,  # Use the determined asset name
                    bids=getattr(order_book, "bids", {}),
                    asks=getattr(order_book, "asks", {}),
                    timestamp=getattr(order_book, "timestamp", time.time()),
                )

            # Log state after this update
            if asset in state.symbols:
                logger.info(
                    f"AFTER UPDATE: Asset {asset} now has {len(state.symbols[asset])} exchanges: {list(state.symbols[asset].keys())}"
                )
                # Log the first few bid/ask entries if any
                for ex, book in state.symbols[asset].items():
                    bid_count = len(book.bids) if hasattr(book, "bids") else 0
                    ask_count = len(book.asks) if hasattr(book, "asks") else 0
                    logger.debug(f"  → {ex}: {bid_count} bids, {ask_count} asks")

    except Exception as e:
        logger.error(f"Error in update_asset_state: {e}", exc_info=True)
        return asset, state  # Return asset instead of key

    # Summary of what we've built
    try:
        assets_count = len(state.symbols)
        exchanges_count = sum(len(exchanges) for exchanges in state.symbols.values())
        logger.info(f"FINAL STATE: Has {assets_count} assets and {exchanges_count} total exchange entries")

        for asset_name, exchanges in state.symbols.items():
            exchange_count = len(exchanges)
            logger.info(f"  • Asset {asset_name}: {exchange_count} exchanges: {list(exchanges.keys())}")

            # Log if we have enough exchanges for arbitrage
            if exchange_count >= 2:
                logger.info(
                    f"  ✅ Asset {asset_name} has sufficient exchanges ({exchange_count}) for arbitrage detection"
                )
            else:
                logger.info(
                    f"  ❌ Asset {asset_name} needs more exchanges for arbitrage detection (have {exchange_count}, need at least 2)"
                )

            # Detailed logging of what's in each exchange's order book
            for exch_name, book in exchanges.items():
                bid_count = len(book.bids) if hasattr(book, "bids") else 0
                ask_count = len(book.asks) if hasattr(book, "asks") else 0
                logger.info(f"    → {exch_name}: {bid_count} bids, {ask_count} asks")
                # Log top bid/ask if available (useful for debugging spread issues)
                if bid_count > 0 and ask_count > 0:
                    try:
                        top_bid = max(book.bids.items(), key=lambda x: float(x[0]))[0] if book.bids else "none"
                        top_ask = min(book.asks.items(), key=lambda x: float(x[0]))[0] if book.asks else "none"
                        logger.debug(f"      Top bid: {top_bid}, Top ask: {top_ask}")
                    except Exception as e:
                        logger.error(f"Error getting top bid/ask: {e}")

    except Exception as e:
        logger.error(f"Error summarizing state: {e}", exc_info=True)

    return asset, state  # Return asset instead of key


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

    # Check if we have enough exchanges for this symbol before calling detect_arbitrage
    if symbol in book_state.symbols and len(book_state.symbols[symbol]) >= 2:
        # Call detect_arbitrage with the updated state
        opportunity = detect_arbitrage(symbol, book_state, threshold, strategy)
    else:
        logger.info(
            f"Waiting for more exchanges for {symbol}, currently have {len(book_state.symbols.get(symbol, {}))} exchange(s)"
        )
        opportunity = None

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
            f"Detection opportunity: {opportunity['pair']}: "
            f"Buy from {opportunity['buy_exchange']} at {opportunity['buy_price']}, "
            f"Sell on {opportunity['sell_exchange']} at {opportunity['sell_price']}, "
            f"Spread: {opportunity['spread']:.4%}, Volume: {opportunity['volume']}"
        )

        return opportunity
    except Exception as e:
        logger.error(f"Error formatting opportunity: {e}")
        return None
