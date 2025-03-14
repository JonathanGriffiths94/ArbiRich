import os
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv

from src.arbirich.exchange_configs import EXCHANGE_CONFIGS

load_dotenv()

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/arbirich")

# Exchange configuration
EXCHANGES = [
    # "binance",
    "bybit",
    "cryptocom",
    # Add more exchanges as needed
]

EXCHANGE_CONFIGS = EXCHANGE_CONFIGS

# Pairs configuration
PAIRS: List[Tuple[str, str]] = [
    ("BTC", "USDT"),
    # ("ETH", "USDT"),
    # ("BNB", "USDT"),
    # ("XRP", "USDT"),
    # ("SOL", "USDT"),
    # ("ADA", "USDT"),
    # ("DOGE", "USDT"),
    # Add more pairs as needed
]

# Strategy configuration
STRATEGIES: Dict[str, Dict[str, Any]] = {
    "Basic Arbitrage": {
        "starting_capital": 10000.0,
        "min_spread": 0.001,  # 0.1%
        "additional_info": {"description": "Basic arbitrage strategy looking for price differences between exchanges"},
    },
    # Add more strategies as needed
}

# API configuration
API_PORT = int(os.getenv("API_PORT", "8088"))
API_HOST = os.getenv("API_HOST", "0.0.0.0")

POSTGRES_DB = os.getenv("POSTGRES_DB", "arbidb")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "arbiuser")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "arbipassword")

DATABASE_URL = f"postgresql://{os.getenv('POSTGRES_USER', 'arbiuser')}:{os.getenv('POSTGRES_PASSWORD', 'arbipassword')}@{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'arbidb')}"
