"""
Script to create seed data for development purposes.
Creates trading opportunities and executions for existing strategies, exchanges, and pairs.
"""

import logging
import os
import random
import sys
import uuid
from datetime import datetime, timedelta

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set up logging before imports
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Try to load environment variables
try:
    from dotenv import load_dotenv

    load_dotenv()
    logger.info("Environment variables loaded from .env file")
except ImportError:
    logger.warning("python-dotenv not installed, using environment variables as is")

# Now do the main imports
try:
    from src.arbirich.config.config import get_strategy_config
    from src.arbirich.models.models import TradeExecution, TradeOpportunity
    from src.arbirich.services.database.database_service import DatabaseService
except ImportError as e:
    logger.error(f"Failed to import required modules: {e}")
    logger.error("Make sure you're running this script from the project root directory")
    logger.error("Try: poetry run python scripts/create_seed_data.py")
    sys.exit(1)


def get_active_entities(db_service):
    """
    Get active exchanges, pairs, and strategies from the database.

    Returns:
        tuple: (exchanges, pairs, strategies)
    """
    logger.info("Getting active entities from database...")

    # Get strategies from the database
    strategies = db_service.get_all_strategies()
    logger.info(f"Found {len(strategies)} strategies in the database")

    # Get exchanges from the database
    exchanges = db_service.get_all_exchanges()
    logger.info(f"Found {len(exchanges)} exchanges in the database")

    # Get pairs from the database
    pairs = db_service.get_all_pairs()
    logger.info(f"Found {len(pairs)} trading pairs in the database")

    return exchanges, pairs, strategies


def create_opportunities_and_executions(db_service, strategies, exchanges, pairs, opportunities_per_strategy=5):
    """
    Create sample trade opportunities and executions for each strategy.

    Args:
        db_service: Database service instance
        strategies: List of strategy objects
        exchanges: List of exchange objects
        pairs: List of pair objects
        opportunities_per_strategy: Number of opportunities to create per strategy
    """
    if not strategies or not exchanges or not pairs:
        logger.error("Missing required data to create opportunities and executions")
        return

    logger.info(f"Creating {opportunities_per_strategy} opportunities per strategy...")
    total_opportunities = 0
    total_executions = 0

    # For each strategy, create some opportunities and executions
    for strategy in strategies:
        # Get strategy config for additional details
        strategy_config = get_strategy_config(strategy.name)
        strategy_pairs = []

        # Find pairs for this strategy
        if strategy_config and "pairs" in strategy_config:
            # Convert tuple pairs to symbols
            config_pairs = strategy_config.get("pairs", [])
            for pair_tuple in config_pairs:
                if isinstance(pair_tuple, tuple) and len(pair_tuple) == 2:
                    pair_symbol = f"{pair_tuple[0]}-{pair_tuple[1]}"
                    # Find matching pair in database pairs
                    for db_pair in pairs:
                        if db_pair.symbol == pair_symbol:
                            strategy_pairs.append(db_pair)
                            break

        # If no pairs defined in config or found, use all pairs
        if not strategy_pairs:
            strategy_pairs = pairs

        # Get exchanges for this strategy
        strategy_exchanges = []
        if strategy_config and "exchanges" in strategy_config:
            config_exchanges = strategy_config.get("exchanges", [])
            for exchange_name in config_exchanges:
                # Find matching exchange in database exchanges
                for db_exchange in exchanges:
                    if db_exchange.name.lower() == exchange_name.lower():
                        strategy_exchanges.append(db_exchange)
                        break

        # If no exchanges defined in config or found, use all exchanges
        if not strategy_exchanges:
            strategy_exchanges = exchanges

        logger.info(
            f"Strategy {strategy.name}: Using {len(strategy_pairs)} pairs and {len(strategy_exchanges)} exchanges"
        )

        # Create opportunities for this strategy
        for _ in range(opportunities_per_strategy):
            # We need at least 2 exchanges to create an opportunity
            if len(strategy_exchanges) < 2:
                logger.warning(f"Not enough exchanges for strategy {strategy.name}, skipping opportunity creation")
                continue

            # Select random pair and exchanges
            pair = random.choice(strategy_pairs)
            buy_exchange = random.choice(strategy_exchanges)

            # Ensure different exchange for selling
            available_sell_exchanges = [e for e in strategy_exchanges if e.id != buy_exchange.id]
            if not available_sell_exchanges:
                logger.warning(f"Not enough unique exchanges for strategy {strategy.name}, skipping opportunity")
                continue

            sell_exchange = random.choice(available_sell_exchanges)

            # Create fake price data
            base_price = random.uniform(100, 50000)  # Random base price
            spread_pct = random.uniform(strategy.min_spread, strategy.min_spread * 5)  # Random spread
            buy_price = base_price
            sell_price = base_price * (1 + spread_pct)
            volume = random.uniform(0.01, 1.0)  # Random volume

            # Create opportunity with a timestamp in the past (1-72 hours ago)
            opp_timestamp = datetime.now() - timedelta(hours=random.randint(1, 72))

            opportunity = TradeOpportunity(
                id=str(uuid.uuid4()),
                strategy=strategy.name,
                pair=pair.symbol,
                buy_exchange=buy_exchange.name,
                sell_exchange=sell_exchange.name,
                buy_price=buy_price,
                sell_price=sell_price,
                spread=spread_pct,
                volume=volume,
                opportunity_timestamp=opp_timestamp.timestamp(),
            )

            try:
                created_opp = db_service.create_trade_opportunity(opportunity)
                logger.info(f"Created opportunity: {created_opp.id} for {created_opp.strategy}")
                total_opportunities += 1

                # 80% chance to execute the opportunity
                if random.random() < 0.8:
                    # Add slight slippage to execution prices
                    executed_buy_price = buy_price * random.uniform(0.999, 1.001)
                    executed_sell_price = sell_price * random.uniform(0.999, 1.001)

                    # Create execution a short time after the opportunity (5-60 seconds)
                    exec_timestamp = opp_timestamp + timedelta(seconds=random.randint(5, 60))

                    execution = TradeExecution(
                        id=str(uuid.uuid4()),
                        strategy=strategy.name,
                        pair=pair.symbol,
                        buy_exchange=buy_exchange.name,
                        sell_exchange=sell_exchange.name,
                        executed_buy_price=executed_buy_price,
                        executed_sell_price=executed_sell_price,
                        spread=spread_pct,
                        volume=volume,
                        execution_timestamp=exec_timestamp.timestamp(),
                        opportunity_id=created_opp.id,
                    )

                    created_exec = db_service.create_trade_execution(execution)
                    logger.info(f"Created execution: {created_exec.id} for opportunity {created_opp.id}")
                    total_executions += 1

                    # Update strategy stats
                    profit = (executed_sell_price - executed_buy_price) * volume

                    db_service.update_strategy_stats(
                        strategy_name=strategy.name,
                        profit=profit if profit > 0 else 0,
                        loss=abs(profit) if profit < 0 else 0,
                        trade_count=1,
                    )
            except Exception as e:
                logger.error(f"Error creating opportunity/execution: {e}")

    logger.info(f"Created {total_opportunities} opportunities and {total_executions} executions")


def main():
    """Create trade opportunities and executions for development"""
    try:
        logger.info("Connecting to database...")
        with DatabaseService() as db_service:
            logger.info("Creating seed data...")

            # Get entities from database
            exchanges, pairs, strategies = get_active_entities(db_service)

            if not exchanges or not pairs or not strategies:
                logger.error("No exchanges, pairs, or strategies found in the database.")
                logger.error("Make sure to run the prefill_database.py script first.")
                sys.exit(1)

            # Create opportunities and executions (10 per strategy)
            create_opportunities_and_executions(db_service, strategies, exchanges, pairs, opportunities_per_strategy=10)

            logger.info("Seed data creation complete!")
    except Exception as e:
        logger.error(f"Error creating seed data: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
