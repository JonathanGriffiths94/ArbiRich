from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class Order(BaseModel):
    price: float
    quantity: float


class OrderBookUpdate(BaseModel):
    exchange: str
    symbol: str
    bids: List[Order]
    asks: List[Order]
    timestamp: float


class ExchangeOrderBook(BaseModel):
    bids: List[Order] = Field(default_factory=list)
    asks: List[Order] = Field(default_factory=list)
    timestamp: float


class AssetPriceState(BaseModel):
    # Mapping from a normalized asset (e.g. "BTC-USDT") to a dictionary
    # that maps exchange names to their corresponding ExchangeOrderBook.
    exchanges: Dict[str, Dict[str, ExchangeOrderBook]] = Field(default_factory=dict)

    @property
    def prices(self) -> set:
        """
        Aggregate all unique bid prices from all exchanges across all assets.
        """
        result = set()
        for asset, exch_dict in self.exchanges.items():
            for order_book in exch_dict.values():
                for order in order_book.bids:
                    result.add(order.price)
        return result

    def __str__(self):
        return f"AssetPriceState(exchanges={self.exchanges})"


class TradeOpportunity(BaseModel):
    asset: str
    buy_exchange: str
    sell_exchange: str
    buy_price: float
    sell_price: float
    spread: float = Field(..., description="Difference between best ask and best bid")
    volume: float
    timestamp: float

    def __str__(self):
        return (
            f"TradeOpportunity(asset={self.asset}, "
            f"buy_exchange={self.buy_exchange}, sell_exchange={self.sell_exchange}, "
            f"buy_price={self.buy_price}, sell_price={self.sell_price}, "
            f"spread={self.spread}, volume={self.volume}, "
            f"timestamp={self.timestamp})"
        )


class TradeExecution(BaseModel):
    asset: str
    buy_exchange: str
    sell_exchange: str
    executed_buy_price: float
    executed_sell_price: float
    spread: float
    volume: float
    execution_timestamp: float
    execution_id: Optional[str] = None  # Optional unique execution identifier

    def __str__(self):
        return (
            f"TradeExecution(asset={self.asset}, buy_exchange={self.buy_exchange}, "
            f"sell_exchange={self.sell_exchange}, executed_buy_price={self.executed_buy_price}, "
            f"executed_sell_price={self.executed_sell_price}, spread={self.spread}, "
            f"volume={self.volume}, execution_timestamp={self.execution_timestamp})"
        )
