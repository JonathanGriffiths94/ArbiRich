from fastapi import APIRouter

from arbirich.services.redis.redis_service import RedisService

router = APIRouter()
price_service = RedisService()


@router.get("/")
async def health_check() -> dict[str, str]:
    return {"status": "OK", "status_code": "200"}
