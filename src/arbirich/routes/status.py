from fastapi import APIRouter

from src.arbirich.market_data_service import MarketDataService

router = APIRouter()
price_service = MarketDataService()


@router.get("/")
async def health_check() -> dict[str, str]:
    return {"status": "OK", "status_code": "200"}


@router.get("/price/{exchange}/{pair}")
async def get_price(exchange: str, pair: str):
    price = price_service.get_price(exchange, pair.upper())
    return {"exchange": exchange, "pair": pair, "price": price}
