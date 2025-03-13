import os

from dotenv import load_dotenv

from src.arbirich.exchange_configs import EXCHANGE_CONFIGS

load_dotenv()

ASSETS = [("BTC", "USDT")]

EXCHANGES = ["cryptocom", "bybit"]

EXCHANGE_CONFIGS = EXCHANGE_CONFIGS

REDIS_CONFIG = {
    "host": os.getenv("REDIS_HOST", "redis"),
    "port": 6379,
    "db": 0,
}

POSTGRES_DB = os.getenv("POSTGRES_DB", "arbidb")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "arbiuser")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "arbipassword")

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

STRATEGIES = {
    "arbitrage": {
        "threshold": 0.0001,
    },
}
