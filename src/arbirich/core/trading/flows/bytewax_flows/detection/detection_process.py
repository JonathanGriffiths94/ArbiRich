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
    start_time = time.time()
    logger.info(f"Starting arbitrage detection for {asset} using strategy {strategy_name}")

    try:
        # First check if we have data from at least 2 exchanges for this asset
        if asset not in state.symbols or len(state.symbols[asset]) < 2:
            logger.info(
                f"Skipping arbitrage detection for {asset}: Need at least 2 exchanges, have {len(state.symbols.get(asset, {}))}"
            )
            return None

        # Get the strategy instance using the factory
        strategy = get_strategy(strategy_name)
        if not strategy:
            logger.error(f"Failed to get strategy instance for {strategy_name}")
            return None

        logger.info(f"Running arbitrage detection for {asset} with {len(state.symbols[asset])} exchanges")
        # Log the exchanges we have data for
        for exchange in state.symbols[asset]:
            book = state.symbols[asset][exchange]
            bid_count = len(book.bids) if hasattr(book, "bids") else 0
            ask_count = len(book.asks) if hasattr(book, "asks") else 0
            logger.info(f"  Exchange {exchange}: {bid_count} bids, {ask_count} asks")

            # Log top bids and asks for better visibility
            if bid_count > 0:
                try:
                    top_bid = max(book.bids.items(), key=lambda x: float(x[0]))[0] if book.bids else "none"
                    top_bid_vol = book.bids.get(top_bid, 0)
                    logger.debug(f"    Top bid: {top_bid} (vol: {top_bid_vol})")
                except Exception as e:
                    logger.error(f"Error getting top bid: {e}")

            if ask_count > 0:
                try:
                    top_ask = min(book.asks.items(), key=lambda x: float(x[0]))[0] if book.asks else "none"
                    top_ask_vol = book.asks.get(top_ask, 0)
                    logger.debug(f"    Top ask: {top_ask} (vol: {top_ask_vol})")
                except Exception as e:
                    logger.error(f"Error getting top ask: {e}")

        # Use the detect_arbitrage method from the strategy
        detection_start = time.time()
        opportunity = strategy.detect_arbitrage(asset, state)
        detection_time = time.time() - detection_start

        logger.info(f"Strategy detection completed in {detection_time:.6f}s")

        if opportunity:
            logger.info(
                f"💰 ARBITRAGE OPPORTUNITY FOUND for {asset} using {strategy_name}:\n"
                f"  • Buy from {opportunity.buy_exchange} at {opportunity.buy_price:.8f}\n"
                f"  • Sell on {opportunity.sell_exchange} at {opportunity.sell_price:.8f}\n"
                f"  • Spread: {opportunity.spread:.6%}\n"
                f"  • Volume: {opportunity.volume:.8f}\n"
                f"  • Est. Profit: {(opportunity.spread * opportunity.volume * opportunity.buy_price):.8f}"
            )

            # Convert Pydantic model to dict for bytewax processing
            if hasattr(opportunity, "model_dump"):
                result = opportunity.model_dump()
            elif hasattr(opportunity, "dict"):
                result = opportunity.dict()
            else:
                result = opportunity

            total_time = time.time() - start_time
            logger.info(f"Total detection processing time: {total_time:.6f}s")
            return result

        logger.info(f"No opportunity found for {asset} using {strategy_name}")
        total_time = time.time() - start_time
        logger.info(f"Total detection processing time: {total_time:.6f}s")
        return None

    except Exception as e:
        logger.error(f"Error detecting arbitrage for {asset} with {strategy_name}: {e}", exc_info=True)
        total_time = time.time() - start_time
        logger.info(f"Detection failed after {total_time:.6f}s")
        return None


def extract_asset_key(record: OrderBookUpdate) -> str:
    """Extract just the asset symbol key from the record"""
    try:
        # Assuming record is always a tuple
        exchange, data = record

        # Make sure symbol is extracted and normalized
        asset = normalize_symbol(data.symbol) if data.symbol else None

        # Ensure symbol isn't None - raise error instead of using fallback
        if not asset:
            logger.error(f"❌ Missing symbol in OrderBookUpdate from {exchange}")
            # Try to extract from exchange:symbol format
            if ":" in exchange:
                _, extracted_symbol = exchange.split(":", 1)
                asset = normalize_symbol(extracted_symbol)
                logger.info(f"📌 Extracted symbol from exchange identifier: {asset}")
            else:
                # Raise error instead of using "UNKNOWN" fallback
                error_msg = f"Cannot extract symbol from exchange {exchange}, no valid key available"
                logger.error(error_msg)
                raise ValueError(error_msg)

        logger.info(f"🔑 EXTRACT_ASSET_KEY: Using key '{asset}' for {exchange}")
        return asset

    except Exception as e:
        logger.error(f"❌ Error in extract_asset_key: {e}, record: {record}")
        # Re-raise the exception to halt processing instead of returning "ERROR"
        raise


def process_order_book_record(record: Dict) -> Tuple[str, Dict]:
    """Process and enrich order book record"""
    try:
        # Assuming record is always a tuple
        exchange, data = record

        # Handle only if data is already an OrderBookUpdate
        if isinstance(data, OrderBookUpdate):
            # Make sure symbol is extracted and normalized
            asset = normalize_symbol(data.symbol) if data.symbol else None

            # Ensure symbol isn't None - raise error instead of using fallback
            if not asset:
                logger.error(f"❌ Missing symbol in OrderBookUpdate from {exchange}")
                # Try to extract from exchange:symbol format
                if ":" in exchange:
                    _, extracted_symbol = exchange.split(":", 1)
                    asset = normalize_symbol(extracted_symbol)
                    logger.info(f"📌 Extracted symbol from exchange identifier: {asset}")
                else:
                    # Raise error instead of using "UNKNOWN" fallback
                    error_msg = f"Cannot extract symbol from exchange {exchange}, no valid key available"
                    logger.error(error_msg)
                    raise ValueError(error_msg)

            logger.info(f"🔄 PROCESS_RECORD: Processed {exchange}:{asset}")

            # Ensure symbol is set in the data object too
            data.symbol = asset

            return asset, data
        else:
            error_msg = f"Unsupported data type in process_order_book_record: {type(data)}"
            logger.error(error_msg)
            raise TypeError(error_msg)
    except Exception as e:
        logger.error(f"❌ Error in process_order_book_record: {e}, record: {record}")
        # Re-raise the exception instead of returning fallback values
        raise


def key_by_asset(record: Dict) -> str:
    """Group order book updates by asset symbol (backward compatibility)"""
    return extract_asset_key(record)


def normalize_symbol(symbol: str) -> str:
    """Normalize symbol format to ensure consistency"""
    if not symbol:
        return "unknown"

    # Replace various separators with standard format and uppercase
    normalized = symbol.replace("_", "-").replace("/", "-").upper()

    return normalized


def update_asset_state(key: str, records: list, state: Dict = None) -> Tuple[str, Optional[OrderBookState]]:
    """Update order book state for an asset"""
    # Check if records is a single item or a collection
    if not isinstance(records, (list, tuple)) and records is not None:
        records = [records]

    logger.info(f"🔄 UPDATE_STATE: Processing {len(records) if records else 0} records for key: {key}")

    if state is None:
        logger.info("🆕 Creating new OrderBookState (empty state)")
        state = OrderBookState()
    else:
        # Log current state summary
        logger.info(f"📊 Current state has {len(state.symbols)} symbols")
        for symbol, exchanges in state.symbols.items():
            logger.info(f"  • Symbol {symbol} has {len(exchanges)} exchanges: {list(exchanges.keys())}")

    # Return early if no records
    if not records:
        logger.warning(f"⚠️ No records provided for asset {key}, returning current state")
        return key, state

    # Validate key - raise error instead of using fallback
    if not key or key == "None":
        error_msg = f"Invalid key provided to update_asset_state: {key}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # Use the validated key
    asset = key

    try:
        logger.info(f"🔄 Processing updates for asset: {asset}")

        for i, record in enumerate(records):
            logger.info(f"📝 Processing record {i + 1}/{len(records)}")

            # Handle tuple (exchange, order_book)
            if isinstance(record, tuple) and len(record) == 2:
                exchange, order_book = record
                logger.info(f"📦 Unpacked tuple: exchange={exchange}")
            else:
                # Assume OrderBookUpdate object
                try:
                    exchange = record.exchange
                    order_book = record
                    logger.info(f"📦 Direct object: exchange={exchange}")
                except AttributeError:
                    error_msg = f"Invalid record type: {type(record)}"
                    logger.error(error_msg)
                    raise ValueError(error_msg)

            # Extract and normalize symbol from order book if needed
            # We require a valid symbol - no fallbacks to UNKNOWN
            symbol = asset

            # Make sure symbol is set in the order book
            order_book.symbol = symbol

            # Initialize symbol entry in state if needed
            if symbol not in state.symbols:
                logger.info(f"🆕 Adding new symbol to state: {symbol}")
                state.symbols[symbol] = {}

            # Check for existing order book for this exchange/symbol
            has_existing = symbol in state.symbols and exchange in state.symbols[symbol]
            if has_existing:
                logger.info(f"🔄 Updating existing order book for {exchange}:{symbol}")
            else:
                logger.info(f"🆕 Adding new exchange {exchange} for symbol {symbol}")

            # Update the order book
            try:
                # Create complete OrderBookUpdate if none exists
                if not has_existing or not isinstance(state.symbols[symbol][exchange], OrderBookUpdate):
                    logger.info(f"🆕 Creating new OrderBookUpdate for {exchange}:{symbol}")
                    state.symbols[symbol][exchange] = OrderBookUpdate(
                        exchange=exchange,
                        symbol=symbol,
                        bids=getattr(order_book, "bids", {}),
                        asks=getattr(order_book, "asks", {}),
                        timestamp=getattr(order_book, "timestamp", time.time()),
                    )
                else:
                    # Update existing order book
                    logger.info(f"🔄 Updating fields for {exchange}:{symbol}")
                    state.symbols[symbol][exchange].bids = order_book.bids
                    state.symbols[symbol][exchange].asks = order_book.asks
                    state.symbols[symbol][exchange].timestamp = order_book.timestamp

                # Log update summary
                bid_count = len(order_book.bids) if hasattr(order_book, "bids") else 0
                ask_count = len(order_book.asks) if hasattr(order_book, "asks") else 0
                logger.info(f"✅ Updated {exchange}:{symbol} with {bid_count} bids, {ask_count} asks")
            except Exception as e:
                logger.error(f"❌ Error updating order book: {e}", exc_info=True)

        # Final state check
        exchanges_for_key = []
        if asset in state.symbols:
            exchanges_for_key = list(state.symbols[asset].keys())

        logger.info(
            f"📊 STATE AFTER UPDATE: Symbol {asset} has {len(exchanges_for_key)} exchanges: {exchanges_for_key}"
        )

        # Most important log - check if we have enough exchanges for comparison
        if len(exchanges_for_key) >= 2:
            logger.info(f"✅ READY FOR COMPARISON: {asset} has {len(exchanges_for_key)} exchanges")
        else:
            logger.warning(f"⚠️ NOT ENOUGH EXCHANGES: {asset} has only {len(exchanges_for_key)} exchange(s)")

    except Exception as e:
        logger.error(f"❌ Error in update_asset_state: {e}", exc_info=True)

    return asset, state


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


def format_opportunity(opportunity: Dict) -> Optional[Tuple[str, Dict]]:
    """
    Format the opportunity for output.

    Parameters:
        opportunity: The opportunity dictionary

    Returns:
        Tuple of (key, opportunity) or None
    """
    if not opportunity:
        return None

    try:
        # Calculate estimated profit for logging
        buy_price = opportunity.get("buy_price", 0)
        volume = opportunity.get("volume", 0)
        spread = opportunity.get("spread", 0)
        estimated_profit = spread * volume * buy_price

        # Add formatted log message with more details
        logger.info(
            f"DETECTION OPPORTUNITY DETAILS:\n"
            f"  Pair: {opportunity['pair']}\n"
            f"  Strategy: {opportunity.get('strategy', 'unknown')}\n"
            f"  Buy: {opportunity['buy_exchange']} @ {opportunity['buy_price']:.8f}\n"
            f"  Sell: {opportunity['sell_exchange']} @ {opportunity['sell_price']:.8f}\n"
            f"  Spread: {opportunity['spread']:.6%}\n"
            f"  Volume: {opportunity['volume']:.8f}\n"
            f"  Est. Profit: {estimated_profit:.8f}\n"
            f"  Timestamp: {opportunity.get('opportunity_timestamp', time.time())}"
        )

        # Return a tuple (key, value) as required by Bytewax for routing
        return (opportunity["pair"], opportunity)
    except Exception as e:
        logger.error(f"Error formatting opportunity: {e}")
        return None
