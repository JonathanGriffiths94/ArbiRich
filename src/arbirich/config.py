import os

from dotenv import load_dotenv

from src.arbirich.exchange_configs import EXCHANGE_CONFIGS

load_dotenv()

ASSETS = [("BTC", "USDT")]

EXCHANGES = ["cryptocom", "bybit"]

EXCHANGE_CONFIGS = EXCHANGE_CONFIGS

REDIS_CONFIG = {
    "host": os.getenv("REDIS_HOST", "localhost"),
    "port": 6379,
    "db": 0,
}

DB_NAME = os.getenv("DB_NAME", "arbidb")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "arbiuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "arbipassword")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


STRATEGIES = {
    "arbitrage": {
        "threshold": 0.0001,
    },
}
