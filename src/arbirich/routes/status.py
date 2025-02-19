import json

from fastapi import APIRouter
from kafka import KafkaConsumer

router = APIRouter()


@router.get("/")
async def health_check() -> dict[str, str]:
    return {"status": "OK", "status_code": "200"}


@router.get("/prices")
async def get_latest_prices():
    """Fetch latest market prices from Kafka"""
    consumer = KafkaConsumer(
        "market_data",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",
    )

    latest_prices = {}
    for message in consumer:
        data = message.value
        latest_prices[data["symbol"]] = data["price"]

        # Stop reading after one iteration (avoids infinite loop in API)
        break  # Remove this line to read all messages

    return latest_prices
