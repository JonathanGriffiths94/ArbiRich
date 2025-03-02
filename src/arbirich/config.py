import os

from src.arbirich.exchange_configs import EXCHANGE_CONFIGS

EXCHANGES = ["bybit", "cryptocom"]
ASSETS = [("BTC", "USDT")]
EXCHANGE_CONFIGS = EXCHANGE_CONFIGS

REDIS_CONFIG = {
    "host": os.getenv("REDIS_HOST", "localhost"),
    "port": 6379,
    "db": 0,
}

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "user": os.getenv("POSTGRES_USER", "your_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "your_password"),
    "database": os.getenv("POSTGRES_DB", "your_database"),
}

STRATEGIES = {
    "arbitrage": {
        "threshold": 0.00001,
    },
}
