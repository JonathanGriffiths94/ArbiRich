import logging
import time
from typing import Dict, Optional, Tuple, Union

from src.arbirich.models.models import OrderBookUpdate

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def process_order_book(data: Union[Dict, Tuple]) -> Optional[OrderBookUpdate]:
    """
    Process raw order book data into a standardized OrderBookUpdate format.

    Parameters:
        data: Raw order book data as a Tuple of (exchange, symbol, order_book_dict)

    Returns:
        OrderBookUpdate model or None if processing fails
    """
    if not data:
        return None

    try:
        # Only handle tuple format: (exchange, symbol, order_book_dict)
        if isinstance(data, tuple) and len(data) == 3:
            exchange = data[0]
            symbol = data[1]
            order_book_dict = data[2]

            # Normalize timestamp
            raw_timestamp = order_book_dict.get("timestamp", time.time())

            # Timestamp normalization - ensure it's in seconds since epoch
            if isinstance(raw_timestamp, (int, float)):
                # If timestamp is in milliseconds (common in crypto exchanges)
                if raw_timestamp > 1600000000000:  # If timestamp is past 2020 in milliseconds
                    timestamp = raw_timestamp / 1000.0
                else:
                    timestamp = raw_timestamp
            else:
                # If it's not a number, use current time
                timestamp = time.time()

            sequence = order_book_dict.get("sequence")

            # Get bids and asks from the dictionary part
            # Since we're receiving real dictionary data from Bybit and Crypto.com,
            # we can simplify by using it directly
            bids_dict = order_book_dict.get("bids", {})
            asks_dict = order_book_dict.get("asks", {})

            # Create OrderBookUpdate with the dictionary format
            order_book_update = OrderBookUpdate(
                exchange=exchange, symbol=symbol, bids=bids_dict, asks=asks_dict, timestamp=timestamp, sequence=sequence
            )

            return order_book_update
        else:
            logger.error(f"Invalid data format received: {type(data)}")
            return None
    except Exception as e:
        logger.error(f"Error processing order book: {e}", exc_info=True)
        return None
