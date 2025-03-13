from fastapi import APIRouter

from src.arbirich.redis_manager import ArbiDataService

router = APIRouter()
price_service = ArbiDataService()


@router.get("/")
async def health_check() -> dict[str, str]:
    return {"status": "OK", "status_code": "200"}
