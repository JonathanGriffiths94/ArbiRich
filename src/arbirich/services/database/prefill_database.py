import logging
import sys
from datetime import UTC, datetime

import sqlalchemy as sa

from src.arbirich.config.config import EXCHANGE_CONFIGS, EXCHANGES, PAIRS, STRATEGIES
from src.arbirich.models.models import Exchange, Pair, Strategy
from src.arbirich.services.database.database_service import DatabaseService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# Update the prefill_database function to check for existing data
def prefill_database():
    logger.info("Starting database prefill process")
    logger.info(f"Exchanges to add: {EXCHANGES}")

    with DatabaseService() as db:
        try:
            # Prefill Exchanges
            for exchange_name in EXCHANGES:
                exchange_config = EXCHANGE_CONFIGS.get(exchange_name)
                try:
                    logger.info(f"Processing exchange: {exchange_name}")
                    exchanges = db.get_all_exchanges()
                    exchange_names = [e.name for e in exchanges]
                    logger.info(f"Existing exchanges: {exchange_names}")

                    if exchange_name not in exchange_names:
                        logger.info(f"Inserting exchange: {exchange_name}")

                        # Extract WS URL - handles if it's a lambda function
                        ws_config = exchange_config.get("ws", {})
                        ws_url = ws_config.get("ws_url")
                        if callable(ws_url):
                            ws_url_value = "dynamic_url"  # Just store a placeholder for function-based URLs
                        else:
                            ws_url_value = ws_url

                        # Extract REST URL
                        rest_config = exchange_config.get("rest", {})
                        rest_url = rest_config.get("url", "")

                        additional_info = {}

                        # Create exchange object with dedicated fields
                        exchange = Exchange(
                            name=exchange_name,
                            api_rate_limit=exchange_config.get("api_response_time"),
                            trade_fees=exchange_config.get("trade_fee"),
                            rest_url=rest_url,
                            ws_url=ws_url_value,
                            delimiter=exchange_config.get("delimiter", ""),
                            withdrawal_fee=exchange_config.get("withdrawal_fee", {}),
                            api_response_time=exchange_config.get("api_response_time"),
                            mapping=exchange_config.get("mapping", {}),
                            additional_info=additional_info,
                            created_at=datetime.now(UTC),
                        )
                        result = db.create_exchange(exchange)
                        logger.info(f"Exchange created with ID: {result.id}")
                    else:
                        logger.info(f"Exchange {exchange_name} already exists, skipping")
                except Exception as e:
                    logger.error(f"Error processing exchange {exchange_name}", exc_info=e)

            # Prefill Pairs
            for base, quote in PAIRS:
                try:
                    logger.info(f"Processing pair: {base}-{quote}")
                    pairs = db.get_all_pairs()
                    symbol = f"{base}-{quote}"

                    if not any(p.base_currency == base and p.quote_currency == quote for p in pairs):
                        logger.info(f"Inserting pair: {symbol}")

                        # Here's the fix - use a direct SQL query as a fallback
                        try:
                            result = db.create_pair(Pair(base_currency=base, quote_currency=quote, symbol=symbol))
                            logger.info(f"Pair created with ID: {result.id}")
                        except Exception as e:
                            logger.warning(f"Error creating pair with Pair model: {e}, trying direct SQL")

                            # Fallback to direct SQL if the model approach fails
                            conn = db.engine.connect()
                            conn.execute(
                                sa.text(
                                    """
                                INSERT INTO pairs (base_currency, quote_currency, symbol) 
                                VALUES (:base, :quote, :symbol)
                                ON CONFLICT (symbol) DO NOTHING
                                """
                                ),
                                {"base": base, "quote": quote, "symbol": symbol},
                            )
                            conn.commit()
                            conn.close()
                            logger.info(f"Pair created using direct SQL: {symbol}")
                    else:
                        logger.info(f"Pair {symbol} already exists, skipping")
                except Exception as e:
                    logger.error(f"Error processing pair {base}-{quote}", exc_info=e)

            # Prefill Strategies
            for strategy_name, config in STRATEGIES.items():
                try:
                    logger.info(f"Processing strategy: {strategy_name}")
                    strategies = db.get_all_strategies()
                    if not any(s.name == strategy_name for s in strategies):
                        logger.info(f"Inserting strategy: {strategy_name}")
                        result = db.create_strategy(
                            Strategy(
                                name=strategy_name,
                                starting_capital=config.get("starting_capital"),
                                min_spread=config.get("min_spread"),
                                additional_info=config.get("additional_info"),
                            )
                        )
                        logger.info(f"Strategy created with ID: {result.id}")
                    else:
                        logger.info(f"Strategy {strategy_name} already exists, skipping")
                except Exception as e:
                    logger.error(f"Error processing strategy {strategy_name}", exc_info=e)

            logger.info("Database prefill completed.")
        except Exception as e:
            logger.error("Error pre-filling database", exc_info=e)
            raise


if __name__ == "__main__":
    try:
        prefill_database()
    except Exception as e:
        logger.error("Fatal error in prefill_database", exc_info=e)
        sys.exit(1)
