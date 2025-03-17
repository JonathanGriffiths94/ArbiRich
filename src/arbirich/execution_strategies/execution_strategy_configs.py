import os

EXECUTION_STRATEGIES = {
    "basic_market": {
        "risk_limit": float(os.getenv("BASIC_MARKET_RISK_LIMIT", "0.05")),
        "max_slippage": float(os.getenv("BASIC_MARKET_MAX_SLIPPAGE", "0.001")),
        "min_profit": float(os.getenv("BASIC_MARKET_MIN_PROFIT", "0.0005")),
        "execution_timeout": float(os.getenv("BASIC_MARKET_TIMEOUT", "5.0")),
        "min_volume": float(os.getenv("BASIC_MARKET_MIN_VOLUME", "0.001")),
        "max_volume": float(os.getenv("BASIC_MARKET_MAX_VOLUME", "1.0")),
    },
    "limit_order": {
        "risk_limit": float(os.getenv("LIMIT_ORDER_RISK_LIMIT", "0.04")),
        "max_slippage": float(os.getenv("LIMIT_ORDER_MAX_SLIPPAGE", "0.0008")),
        "min_profit": float(os.getenv("LIMIT_ORDER_MIN_PROFIT", "0.0008")),  # Higher profit requirement
        "execution_timeout": float(os.getenv("LIMIT_ORDER_TIMEOUT", "30.0")),  # Longer timeout for limit orders
        "min_volume": float(os.getenv("LIMIT_ORDER_MIN_VOLUME", "0.001")),
        "max_volume": float(os.getenv("LIMIT_ORDER_MAX_VOLUME", "1.0")),
        "price_improvement": float(
            os.getenv("LIMIT_ORDER_PRICE_IMPROVEMENT", "0.0005")
        ),  # How much to improve on quote
    },
    # Add configurations for other execution strategies as needed
}
