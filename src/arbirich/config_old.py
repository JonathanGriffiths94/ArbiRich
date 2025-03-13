import json

EXCHANGE_CONFIG = {
    # "coinbase": {
    #     "url": "wss://ws-feed.exchange.coinbase.com",
    #     "subscribe": lambda product_id: json.dumps(
    #         {
    #             "type": "subscribe",
    #             "channels": [{"name": "level2", "product_ids": [product_id]}],
    #         }
    #     ),
    #     "extract": lambda data: {
    #         "bids": data["bids"],
    #         "asks": data["asks"],
    #         "timestamp": data["time"],
    #     }
    #     if data["type"] == "snapshot"
    #     else {
    #         "changes": data["changes"],
    #         "timestamp": data["time"],
    #     }
    #     if data["type"] == "l2update"
    #     else None,
    #     "message_type": lambda message: "snapshot"
    #     if message.get("type") == "snapshot"
    #     else "delta",
    # },
    "cryptocom": {
        "url": "wss://stream.crypto.com/v2/market/",
        "subscribe": lambda product_id: json.dumps(
            {
                "id": 1,
                "method": "subscribe",
                "params": {"channels": [f"book.{product_id}.10"]},
                "book_subscription_type": "SNAPSHOT_AND_UPDATE",
                "book_update_frequency": 10,
            }
        ),
        "extract": lambda data: (
            {
                "bids": data["result"]["data"][0]["bids"],
                "asks": data["result"]["data"][0]["asks"],
                "timestamp": data["result"]["data"][0]["t"],
            }
            if "result" in data and "data" in data["result"]
            else None
        ),
        "message_type": lambda message: ("snapshot" if message.get("method") == "subscribe" else "delta"),
    },
    "binance": {
        "url": lambda product_id: f"wss://stream.binance.com:9443/ws/{product_id.lower()}@depth",
        "subscribe": lambda product_id: json.dumps(
            {
                "method": "SUBSCRIBE",
                "params": [
                    f"{product_id.lower()}@depth",
                ],
                "id": 1,
            }
        ),
        "extract": lambda data: (
            {
                "bids": data["b"],
                "asks": data["a"],
                "timestamp": data["E"],
            }
            if ("b" in data and "a" in data and "E" in data)
            else None
        ),
        "message_type": lambda message: "delta",
    },
    "bybit": {
        "url": "wss://stream.bybit.com/v5/public/spot",
        "subscribe": lambda product_id: json.dumps({"op": "subscribe", "args": [f"orderbook.200.{product_id}"]}),
        "extract": lambda data: (
            {
                "bids": data.get("b"),
                "asks": data.get("a"),
                "timestamp": data.get("ts"),
            }
            if "b" in data and "a" in data and "ts" in data
            else None
        ),
        "message_type": None,
    },
    "kucoin": {
        "url": "wss://ws-api.kucoin.com/endpoint",
        "subscribe": lambda product_id: json.dumps(
            {
                "id": 1,
                "type": "subscribe",
                "topic": f"/market/level2:{product_id}",
                "privateChannel": False,
                "response": True,
            }
        ),
        "extract": lambda data: (
            {
                "bids": data["data"]["bids"],
                "asks": data["data"]["asks"],
                "timestamp": data["data"]["time"],
            }
            if ("data" in data and "bids" in data["data"] and "asks" in data["data"] and "time" in data["data"])
            else None
        ),
        "message_type": lambda message: ("snapshot" if message.get("subject") == "orderBookSnapshot" else "delta"),
    },
}
