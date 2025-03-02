from fastapi import APIRouter

from arbirich.redis_manager import MarketDataService

router = APIRouter()
price_service = MarketDataService()


@router.get("/")
async def health_check() -> dict[str, str]:
    return {"status": "OK", "status_code": "200"}
