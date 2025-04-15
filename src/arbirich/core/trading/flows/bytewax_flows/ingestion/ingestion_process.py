import logging
from typing import Optional, Tuple

from src.arbirich.models.models import OrderBookUpdate

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def process_order_book(data: Tuple[str, str, OrderBookUpdate]) -> Optional[OrderBookUpdate]:
    """
    Process order book data, assuming it's already an OrderBookUpdate model.

    Parameters:
        data: A tuple of (exchange, symbol, order_book) where order_book is an OrderBookUpdate

    Returns:
        OrderBookUpdate model or None if processing fails
    """
    if not data:
        return None

    try:
        # Assume data is a tuple with the third element being an OrderBookUpdate
        if isinstance(data, tuple) and len(data) == 3:
            # Simply return the OrderBookUpdate model
            order_book = data[2]

            # Make sure it's the right type
            if not isinstance(order_book, OrderBookUpdate):
                logger.error(f"❌ Expected OrderBookUpdate model, got {type(order_book)}")
                return None

            return order_book
        else:
            logger.error(f"❌ Invalid data format received: {type(data)}")
            return None
    except Exception as e:
        logger.error(f"❌ Error processing order book: {e}", exc_info=True)
        return None
