from fastapi import APIRouter, HTTPException

from arbirich.services.trade_executor import execute_trade
from src.arbirich.models.trade import TradeRequest

router = APIRouter()


@router.get("/")
async def read_trades():
    return {"message": "List of trades"}


@router.post("/execute")
async def place_trade(trade: TradeRequest):
    """Places a trade on an exchange"""
    try:
        response = await execute_trade(
            exchange=trade.exchange,
            symbol=trade.symbol,
            side=trade.side,
            quantity=trade.quantity,
            price=trade.price,
        )
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
