from typing import Dict, List, Set

from pydantic import BaseModel


class AssetPriceState(BaseModel):
    # Store bids and asks as dictionaries mapping exchange names to a list of order dicts.
    bids: Dict[str, List[dict]] = {}
    asks: Dict[str, List[dict]] = {}
    # Timestamps are stored as a dictionary mapping exchange name to a float timestamp.
    timestamp: Dict[str, float] = {}

    @property
    def prices(self) -> Set[float]:
        """
        Aggregate all unique bid prices from all exchanges.
        (You could also combine ask prices if needed.)
        """
        result = set()
        for orders in self.bids.values():
            for order in orders:
                price = order.get("price")
                if price is not None:
                    result.add(price)
        return result

    def __str__(self):
        return f"""
            AssetPriceState(bids={self.bids},
            asks={self.asks},
            timestamp={self.timestamp})
        """
