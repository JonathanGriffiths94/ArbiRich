import logging
import time
from typing import Optional, Tuple

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
        bid_count = len(book.bids)
        ask_count = len(book.asks)
        logger.info(f"  Exchange {exchange}: {bid_count} bids, {ask_count} asks")

        # Log top bids and asks for better visibility
        if bid_count > 0:
            top_bid = max(book.bids.items(), key=lambda x: float(x[0]))[0]
            top_bid_vol = book.bids.get(top_bid, 0)
            logger.debug(f"    Top bid: {top_bid} (vol: {top_bid_vol})")

        if ask_count > 0:
            top_ask = min(book.asks.items(), key=lambda x: float(x[0]))[0]
            top_ask_vol = book.asks.get(top_ask, 0)
            logger.debug(f"    Top ask: {top_ask} (vol: {top_ask_vol})")

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

        total_time = time.time() - start_time
        logger.info(f"Total detection processing time: {total_time:.6f}s")
        return opportunity

    logger.info(f"No opportunity found for {asset} using {strategy_name}")
    total_time = time.time() - start_time
    logger.info(f"Total detection processing time: {total_time:.6f}s")
    return None


def extract_asset_key(record: Tuple[str, OrderBookUpdate]) -> str:
    """Extract just the asset symbol key from the record"""
    exchange, data = record
    asset = normalize_symbol(data.symbol)
    logger.info(f"🔑 EXTRACT_ASSET_KEY: Using key '{asset}' for {exchange}")
    return asset


def process_order_book_record(record: Tuple[str, OrderBookUpdate]) -> Tuple[str, OrderBookUpdate]:
    """Process and enrich order book record"""
    exchange, data = record
    asset = normalize_symbol(data.symbol)
    logger.info(f"🔄 PROCESS_RECORD: Processed {exchange}:{asset}")

    # Ensure symbol is set in the data object too
    data.symbol = asset

    return asset, data


def key_by_asset(record: Tuple[str, OrderBookUpdate]) -> str:
    """Group order book updates by asset symbol"""
    return extract_asset_key(record)


def normalize_symbol(symbol: str) -> str:
    """Normalize symbol format to ensure consistency"""
    # Replace various separators with standard format and uppercase
    normalized = symbol.replace("_", "-").replace("/", "-").upper()
    return normalized


def update_asset_state(key: str, records: list, state: OrderBookState = None) -> Tuple[str, OrderBookState]:
    """Update order book state for an asset"""
    # Check if records is a single item or a collection
    if not isinstance(records, (list, tuple)):
        records = [records]

    logger.info(f"🔄 UPDATE_STATE: Processing {len(records)} records for key: {key}")

    if state is None:
        logger.info("🆕 Creating new OrderBookState")
        state = OrderBookState()

    # Log current state summary
    logger.info(f"📊 Current state has {len(state.symbols)} symbols")
    for symbol, exchanges in state.symbols.items():
        logger.info(f"  • Symbol {symbol} has {len(exchanges)} exchanges: {list(exchanges.keys())}")

    # Use the validated key
    asset = key

    logger.info(f"🔄 Processing updates for asset: {asset}")

    for i, record in enumerate(records):
        logger.info(f"📝 Processing record {i + 1}/{len(records)}")

        # Unpack exchange and order book
        exchange, order_book = record
        logger.info(f"📦 Processing: exchange={exchange}")

        # Make sure symbol is set in the order book
        order_book.symbol = asset

        # Initialize symbol entry in state if needed
        if asset not in state.symbols:
            logger.info(f"🆕 Adding new symbol to state: {asset}")
            state.symbols[asset] = {}

        # Update the order book in state
        state.symbols[asset][exchange] = order_book

        # Log update summary
        bid_count = len(order_book.bids)
        ask_count = len(order_book.asks)
        logger.info(f"✅ Updated {exchange}:{asset} with {bid_count} bids, {ask_count} asks")

    # Final state check
    exchanges_for_key = []
    if asset in state.symbols:
        exchanges_for_key = list(state.symbols[asset].keys())

    logger.info(f"📊 STATE AFTER UPDATE: Symbol {asset} has {len(exchanges_for_key)} exchanges: {exchanges_for_key}")

    # Most important log - check if we have enough exchanges for comparison
    if len(exchanges_for_key) >= 2:
        logger.info(f"✅ READY FOR COMPARISON: {asset} has {len(exchanges_for_key)} exchanges")
    else:
        logger.warning(f"⚠️ NOT ENOUGH EXCHANGES: {asset} has only {len(exchanges_for_key)} exchange(s)")

    return asset, state


def find_arbitrage_opportunities(
    state_and_input: Tuple[OrderBookState, Tuple[str, OrderBookUpdate]],
) -> Tuple[OrderBookState, Optional[TradeOpportunity]]:
    """
    Find arbitrage opportunities with updated state.

    Parameters:
        state_and_input: A tuple containing:
            - state: The current OrderBookState
            - input: A tuple of (exchange, orderbook) from the source

    Returns:
        A tuple of:
            - updated OrderBookState
            - TradeOpportunity or None
    """
    state, message = state_and_input

    # Extract state variables
    strategy = state.strategy
    threshold = state.threshold

    # Extract input data
    exchange, order_book = message

    # Update state with the new order book
    symbol = order_book.symbol
    if symbol not in state.symbols:
        state.symbols[symbol] = {}
    state.symbols[symbol][exchange] = order_book

    # Check if we have enough exchanges for this symbol before calling detect_arbitrage
    if symbol in state.symbols and len(state.symbols[symbol]) >= 2:
        # Call detect_arbitrage with the updated state
        opportunity = detect_arbitrage(symbol, state, threshold, strategy)
    else:
        logger.info(
            f"Waiting for more exchanges for {symbol}, currently have {len(state.symbols.get(symbol, {}))} exchange(s)"
        )
        opportunity = None

    # Return updated state and opportunity
    return state, opportunity


def format_opportunity(opportunity: TradeOpportunity) -> Optional[Tuple[str, TradeOpportunity]]:
    """
    Format the opportunity for output.

    Parameters:
        opportunity: The trade opportunity model

    Returns:
        Tuple of (key, opportunity) or None
    """
    if not opportunity:
        return None

    # Calculate estimated profit for logging
    estimated_profit = opportunity.spread * opportunity.volume * opportunity.buy_price

    # Add formatted log message with more details
    logger.info(
        f"DETECTION OPPORTUNITY DETAILS:\n"
        f"  Pair: {opportunity.pair}\n"
        f"  Strategy: {opportunity.strategy}\n"
        f"  Buy: {opportunity.buy_exchange} @ {opportunity.buy_price:.8f}\n"
        f"  Sell: {opportunity.sell_exchange} @ {opportunity.sell_price:.8f}\n"
        f"  Spread: {opportunity.spread:.6%}\n"
        f"  Volume: {opportunity.volume:.8f}\n"
        f"  Est. Profit: {estimated_profit:.8f}\n"
        f"  Timestamp: {opportunity.opportunity_timestamp}"
    )

    # Return a tuple (key, value) as required by Bytewax for routing
    return (opportunity.pair, opportunity)
