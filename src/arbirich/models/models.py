import datetime
from typing import Dict, Optional

from src.arbirich.models.base import BaseModel


class SystemHealthCheck(BaseModel):
    id: Optional[int] = None
    timestamp: datetime.datetime
    database_healthy: bool
    redis_healthy: bool
    exchanges: Dict[str, str]
    overall_health: bool
