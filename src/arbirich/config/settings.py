import os
from typing import Optional


# Environment variable loading
def get_env(key: str, default: Optional[str] = None) -> str:
    """Get an environment variable or return a default value"""
    return os.environ.get(key, default)


# Database settings
DB_NAME = get_env("POSTGRES_DB", "arbirich_db")
DB_USER = get_env("POSTGRES_USER", "arbiuser")
DB_PASSWORD = get_env("POSTGRES_PASSWORD", "postgres")
DB_HOST = get_env("POSTGRES_HOST", "localhost")
DB_PORT = int(get_env("POSTGRES_PORT", "5432"))

# Construct database URL
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Redis settings
REDIS_HOST = get_env("REDIS_HOST", "localhost")
REDIS_PORT = int(get_env("REDIS_PORT", "6379"))
REDIS_DB = int(get_env("REDIS_DB", "0"))
REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"

# API and web settings
WEB_HOST = get_env("WEB_HOST", "0.0.0.0")
WEB_PORT = int(get_env("WEB_PORT", "8080"))

# Strategy settings
DEFAULT_STRATEGY = get_env("STRATEGY_SELECTION", "basic_arbitrage")
