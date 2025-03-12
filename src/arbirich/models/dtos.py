import hashlib
import json
import time
from typing import Dict, List, Optional, Tuple
from uuid import uuid4

from pydantic import BaseModel, Field, computed_field


class CryptocomOrderBookSnapshot(BaseModel):
    bids: List[Tuple[float, float, float]]
    asks: List[Tuple[float, float, float]]
    u: int = Field(..., description="Update ID")
    pu: Optional[int] = Field(None, description="Previous update ID")
    t: Optional[int] = Field(None, description="Timestamp")


class BybitOrderBookSnapshot(BaseModel):
    bids: List[Tuple[float, float, float]]
    asks: List[Tuple[float, float, float]]
    u: int = Field(..., description="Update ID")
    pu: Optional[int] = Field(..., description="Previous update ID")
    t: Optional[int] = Field(None, description="Timestamp")


class BinanceOrderBookSnapshot(BaseModel):
    bids: List[Tuple[float, float, float]]
    asks: List[Tuple[float, float, float]]
    u: int = Field(..., description="Update ID")
    U: Optional[int] = Field(..., description="First update ID")
    ts: Optional[int] = Field(None, description="Timestamp")


class Order(BaseModel):
    price: float
    quantity: float


class OrderBookUpdate(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    exchange: str
    symbol: str
    bids: List[Order]
    asks: List[Order]
    timestamp: float
    # snapshot_id: float

    @computed_field
    def hash(self) -> str:
        bids_json = json.dumps([bid.model_dump() for bid in self.bids], sort_keys=True)
        asks_json = json.dumps([ask.model_dump() for ask in self.asks], sort_keys=True)
        combined = bids_json + asks_json
        return hashlib.sha256(combined.encode("utf-8")).hexdigest()


class AssetPriceState(BaseModel):
    # Mapping from a normalized asset symbol (e.g. "BTC-USDT") to a dictionary
    # that maps exchange names to their corresponding OrderBookUpdate.
    symbols: Dict[str, Dict[str, OrderBookUpdate]] = Field(default_factory=dict)

    @property
    def prices(self) -> set:
        result = set()
        for asset, exch_dict in self.symbols.items():
            for order_book in exch_dict.values():
                for order in order_book.bids:
                    result.add(order.price)
        return result

    def __str__(self):
        return f"AssetPriceState(symbols={self.symbols})"


class TradeOpportunity(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    asset: str
    buy_exchange: str
    sell_exchange: str
    buy_price: float
    sell_price: float
    spread: float = Field(..., description="Difference between best ask and best bid")
    volume: float
    timestamp: float = Field(default_factory=time.time)

    def __str__(self):
        return (
            f"TradeOpportunity(asset={self.asset}, "
            f"buy_exchange={self.buy_exchange}, sell_exchange={self.sell_exchange}, "
            f"buy_price={self.buy_price}, sell_price={self.sell_price}, "
            f"spread={self.spread}, volume={self.volume}, "
            f"timestamp={self.timestamp})"
        )


class TradeExecution(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    asset: str
    buy_exchange: str
    sell_exchange: str
    executed_buy_price: float
    executed_sell_price: float
    spread: float
    volume: float
    execution_timestamp: float
    execution_id: Optional[str] = None
    opportunity_id: str

    def __str__(self):
        return (
            f"TradeExecution(asset={self.asset}, buy_exchange={self.buy_exchange}, "
            f"sell_exchange={self.sell_exchange}, executed_buy_price={self.executed_buy_price}, "
            f"executed_sell_price={self.executed_sell_price}, spread={self.spread}, "
            f"volume={self.volume}, execution_timestamp={self.execution_timestamp})"
        )
