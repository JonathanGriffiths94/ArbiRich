import json
import os

EXCHANGE_CONFIGS = {
    "cryptocom": {
        "credentials": {
            "api_key": os.getenv("CRYPTOCOM_API_KEY"),
            "api_secret": os.getenv("CRYPTOCOM_API_SECRET"),
        },
        "ws": {
            "ws_url": "wss://stream.crypto.com/v2/market/",
            "snapshot_url": "",
            "processor": "CryptocomOrderBookProcessor",
            "subscription_type": "snapshot",
            "use_rest_snapshot": False,
        },
        "rest": {},
        "delimiter": "_",
        "trade_fee": 0.0025,
        "withdrawal_fee": {"BTC": 0.0005, "ETH": 0.005, "USDT": 0.01},
        "api_response_time": 300,
    },
    "binance": {
        "credentials": {},
        "ws": {
            "ws_url": lambda product_id: f"wss://stream.binance.com:9443/ws/{product_id}@depth@1000ms",
            "snapshot_url": lambda product_id: f"https://api.binance.com/api/v3/depth?symbol={product_id}&limit=100",
            "subscription_message": lambda product_id: json.dumps(
                {
                    "method": "SUBSCRIBE",
                    "params": [f"{product_id.lower()}@depth"],
                    "id": 1,
                }
            ),
            "processor": "BinanceOrderBookProcessor",
            "subscription_type": "delta",
            "use_rest_snapshot": True,
        },
        "rest": {},
        "delimiter": "",
        "mapping": {},
        "trade_fee": 0.0025,
        "withdrawal_fee": {"BTC": 0.0005, "ETH": 0.005, "USDT": 0.01},
        "api_response_time": 300,
    },
    "bybit": {
        "credentials": {
            "api_key": os.getenv("BYBIT_API_KEY"),
            "api_secret": os.getenv("BYBIT_API_SECRET"),
            "api_passphrase": os.getenv("BYBIT_API_PASSPHRASE"),
        },
        "ws": {
            "ws_url": "wss://stream.bybit.com/v5/public/spot",
            "snapshot_url": "",
            "subscription_message": lambda product: json.dumps(
                {"op": "subscribe", "args": [f"orderbook.200.{product}"]}
            ),
            "processor": "BybitOrderBookProcessor",
            "subscription_type": "snapshot",
            "use_rest_snapshot": False,
        },
        "rest": {},
        "delimiter": "",
        "trade_fee": 0.0025,
        "withdrawal_fee": {"BTC": 0.0005, "ETH": 0.005, "USDT": 0.01},
        "api_response_time": 300,
    },
}
