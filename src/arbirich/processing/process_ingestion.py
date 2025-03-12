import logging

from arbirich.models.dtos import Order, OrderBookUpdate
from src.arbirich.processing.helpers import normalise_timestamp

logger = logging.getLogger(__name__)


def process_order_book(order_book_update: tuple) -> OrderBookUpdate:
    """
    Process order book updates by ensuring we have the exchange, product,
    and order book data, and return an OrderBookUpdate instance.
    """
    if isinstance(order_book_update, tuple):
        if len(order_book_update) == 3:
            exchange, product_id, order_book = order_book_update
    else:
        raise ValueError("Order book update is not in an expected format.")

    normalised_timestamp = normalise_timestamp(order_book["timestamp"])

    bids = [
        Order(price=float(k), quantity=float(v)) for k, v in order_book["bids"].items()
    ]
    asks = [
        Order(price=float(k), quantity=float(v)) for k, v in order_book["asks"].items()
    ]

    order_book_update = OrderBookUpdate(
        exchange=exchange,
        symbol=product_id,
        bids=bids,
        asks=asks,
        timestamp=normalised_timestamp,
    )
    return order_book_update
