from pydantic import BaseModel


class TradeRequest(BaseModel):
    exchange: str
    symbol: str
    side: str  # "buy" or "sell"
    quantity: float
    price: float
