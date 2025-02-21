from fastapi import APIRouter, HTTPException

from arbirich.execution import TradeExecutor
from arbirich.models.trade_request import TradeRequest

router = APIRouter()
trade_executor = TradeExecutor()


@router.get("/")
async def read_trades():
    return {"message": "List of trades"}


@router.post("/execute")
async def place_trade(trade: TradeRequest):
    """Places a trade on an exchange"""
    try:
        response = trade_executor.execute_trade(
            exchange=trade.exchange,
            symbol=trade.symbol,
            side=trade.side,
            quantity=trade.quantity,
            price=trade.price,
        )
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
