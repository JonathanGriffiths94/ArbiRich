import logging

from arbirich.models.dtos import OrderBookUpdate
from src.arbirich.processing.helpers import normalise_timestamp

logger = logging.getLogger(__name__)


def process_order_book(order_book_update: tuple) -> OrderBookUpdate:
    """
    Process order book updates by ensuring we have the exchange, product,
    and order book data, and return an OrderBookUpdate instance.
    """
    # If order_book_update is a tuple...
    if isinstance(order_book_update, tuple):
        if len(order_book_update) == 3:
            exchange, product_id, order_book = order_book_update
        elif len(order_book_update) == 2:
            product_id, order_book = order_book_update
            exchange = "unknown"
        else:
            raise ValueError("Order book update tuple has unexpected length.")
    elif isinstance(order_book_update, dict):
        exchange = order_book_update.get("exchange", "unknown")
        product_id = order_book_update.get("symbol", "unknown")
        order_book = order_book_update
    else:
        raise ValueError("Order book update is not in an expected format.")

    # Validate required fields.
    if (
        not order_book
        or "bids" not in order_book
        or "asks" not in order_book
        or "timestamp" not in order_book
    ):
        raise ValueError(
            f"Invalid order book update received from {exchange} for {product_id}: {order_book}"
        )

    normalized_timestamp = normalise_timestamp(order_book["timestamp"])

    # Convert bids and asks to list of dicts.
    bids = [
        {"price": float(k), "quantity": float(v)} for k, v in order_book["bids"].items()
    ]
    asks = [
        {"price": float(k), "quantity": float(v)} for k, v in order_book["asks"].items()
    ]
    order_book_update = OrderBookUpdate(
        exchange=exchange,
        symbol=product_id,
        bids=bids,
        asks=asks,
        timestamp=normalized_timestamp,
    )
    return order_book_update
