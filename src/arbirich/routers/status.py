import logging
import os
from typing import Dict

import psycopg2
import redis
from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api", tags=["Status"])
logger = logging.getLogger(__name__)


@router.get("/health")
async def health_check() -> Dict[str, str]:
    """
    Health check endpoint to verify API, Redis, and PostgreSQL connectivity
    """
    status = {"api": "healthy", "redis": "unknown", "database": "unknown"}

    # Try to connect to Redis
    try:
        redis_host = os.getenv("REDIS_HOST", "localhost")
        redis_port = int(os.getenv("REDIS_PORT", 6379))
        redis_client = redis.Redis(host=redis_host, port=redis_port, db=0)
        redis_client.ping()
        status["redis"] = "healthy"
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        status["redis"] = "unhealthy"

    # Try to connect to PostgreSQL
    try:
        db_host = os.getenv("POSTGRES_HOST", "localhost")
        db_port = os.getenv("POSTGRES_PORT", "5432")
        db_name = os.getenv("POSTGRES_DB", "arbirich_db")
        db_user = os.getenv("POSTGRES_USER", "arbiuser")
        db_password = os.getenv("POSTGRES_PASSWORD", "postgres")

        conn = psycopg2.connect(host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        status["database"] = "healthy"
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        status["database"] = "unhealthy"

    # If any component is unhealthy, return a 503 status
    if "unhealthy" in status.values():
        raise HTTPException(status_code=503, detail=status)

    return status


@router.get("/")
async def root() -> Dict[str, str]:
    """
    Root endpoint
    """
    return {"message": "Welcome to ArbiRich API"}
