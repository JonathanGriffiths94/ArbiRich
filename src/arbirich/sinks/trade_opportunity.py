import json
import logging
import time
from typing import Optional

from src.arbirich.config import REDIS_CONFIG
from src.arbirich.models.dtos import TradeOpportunity
from src.arbirich.redis_manager import MarketDataService

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Instantiate the Redis service.
redis_client = MarketDataService(
    host=REDIS_CONFIG["host"], port=REDIS_CONFIG["port"], db=REDIS_CONFIG["db"]
)


def debounce_opportunity(
    redis_client, opportunity: TradeOpportunity, expiry_seconds=30
) -> Optional[TradeOpportunity]:
    try:
        key = f"last_opp:{opportunity.asset}:{opportunity.buy_exchange}:{opportunity.sell_exchange}"
        now = time.time()

        last_opp_data = redis_client.redis_client.get(key)
        if last_opp_data:
            last_opp_entry = json.loads(last_opp_data)
            last_time = last_opp_entry.get("timestamp", 0)
            last_buy_price = last_opp_entry.get("buy_price", 0)
            last_sell_price = last_opp_entry.get("sell_price", 0)
            if (
                abs(opportunity.buy_price - last_buy_price) / (last_buy_price + 1e-8)
                < 0.001
                and abs(opportunity.sell_price - last_sell_price)
                / (last_sell_price + 1e-8)
                < 0.001
                and (now - last_time) < expiry_seconds
            ):
                logger.debug(
                    f"Skipping duplicate arbitrage opportunity for {opportunity.asset}"
                )
                return None

        redis_client.redis_client.setex(
            key,
            expiry_seconds,
            json.dumps(
                {
                    "timestamp": now,
                    "buy_price": opportunity.buy_price,
                    "sell_price": opportunity.sell_price,
                }
            ),
        )
        return opportunity
    except Exception as e:
        logger.error(f"Error in debounce_opportunity: {e}", exc_info=True)
        return None


def publish_trade_opportunity(opportunity: TradeOpportunity) -> TradeOpportunity:
    try:
        # Publish the opportunity (if your redis_client expects a dict, convert it)
        redis_client.publish_trade_opportunity(opportunity.dict())
        logger.info(f"Published trade opportunity: {opportunity.json()}")
        return opportunity
    except Exception as e:
        logger.error(f"Error pushing opportunity: {e}")
        return None
