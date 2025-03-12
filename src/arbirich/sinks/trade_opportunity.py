import json
import logging
import time
from typing import Optional

from src.arbirich.config import REDIS_CONFIG
from src.arbirich.models.dtos import TradeOpportunity
from src.arbirich.redis_manager import ArbiDataService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

redis_client = ArbiDataService(
    host=REDIS_CONFIG["host"], port=REDIS_CONFIG["port"], db=REDIS_CONFIG["db"]
)


def debounce_opportunity(
    redis_client, opportunity: TradeOpportunity, expiry_seconds=30
) -> Optional[TradeOpportunity]:
    try:
        logger.info(f"Opportunity: {opportunity}")
        key = f"last_opp:{opportunity.asset}:{opportunity.buy_exchange}:{opportunity.sell_exchange}"
        now = time.time()
        logger.info(f"Key: {key}")
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

        return opportunity
    except Exception as e:
        logger.error(f"Error in debounce_opportunity: {e}", exc_info=True)
        return None


def publish_trade_opportunity(opportunity: TradeOpportunity) -> TradeOpportunity:
    if not opportunity:
        return None
    try:
        # Publish the opportunity (if your redis_client expects a dict, convert it)
        redis_client.publish_trade_opportunity(opportunity)
        logger.info(f"Published trade opportunity: {opportunity}")
        return opportunity
    except Exception as e:
        logger.error(f"Error pushing opportunity: {e}")
        return None
