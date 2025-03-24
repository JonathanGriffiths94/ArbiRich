#!/usr/bin/env python3
"""
Prefill Database Script - Initializes the database with configurations from the exchange_config file.

This script populates the database with:
1. Exchanges defined in ALL_EXCHANGES
2. Trading pairs defined in ALL_PAIRS
3. Strategies defined in ALL_STRATEGIES

Usage:
    poetry run python -m src.arbirich.services.database.prefill_database [--activate]

Options:
    --activate    Activate all entities instead of leaving them inactive by default
"""

import argparse
import json
import logging
import sys
import threading

import sqlalchemy as sa

from src.arbirich.config.config import (
    ALL_EXCHANGES,
    ALL_PAIRS,
    ALL_STRATEGIES,
    get_all_exchange_names,
    get_all_pair_symbols,
    get_all_strategy_names,
)
from src.arbirich.services.database.database_service import DatabaseService

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Try to load environment variables
try:
    from dotenv import load_dotenv

    load_dotenv()
    logger.info("Environment variables loaded from .env file")
except ImportError:
    logger.warning("python-dotenv not installed, using environment variables as is")


_prefill_lock = threading.Lock()
_prefill_in_progress = False


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Prefill database with configuration data")
    parser.add_argument(
        "--activate", action="store_true", help="Activate all entities instead of leaving them inactive"
    )
    return parser.parse_args()


def create_exchanges(db_service, activate=False):
    """
    Create exchanges from the exchange_config file.

    Args:
        db_service: Database service instance
        activate: Whether to activate the entities

    Returns:
        List of created exchanges
    """
    logger.info("Creating exchanges...")
    created_exchanges = []

    for exchange_name in get_all_exchange_names():
        config = ALL_EXCHANGES.get(exchange_name, {})
        if not config:
            logger.warning(f"No configuration found for exchange: {exchange_name}")
            continue

        try:
            # First check if exchange already exists
            existing = db_service.get_exchange_by_name(exchange_name)

            if existing:
                logger.info(f"Exchange already exists: {exchange_name}, updating...")

                # Update existing exchange (don't try to modify the ID)
                with db_service.engine.begin() as conn:
                    # Prepare data for update
                    update_data = {
                        "api_rate_limit": config.get("api_rate_limit"),
                        "trade_fees": config.get("trade_fees"),
                        "rest_url": config.get("rest_url"),
                        "ws_url": config.get("ws_url"),
                        "delimiter": config.get("delimiter"),
                        "withdrawal_fee": json.dumps(config.get("withdrawal_fee")),
                        "api_response_time": config.get("api_response_time"),
                        "mapping": json.dumps(config.get("mapping")),
                        "additional_info": json.dumps(config.get("additional_info")),
                        "is_active": activate,
                    }

                    # Create the update statement without the type casts
                    update_stmt = sa.text(
                        """
                    UPDATE exchanges
                    SET 
                        api_rate_limit = :api_rate_limit,
                        trade_fees = :trade_fees,
                        rest_url = :rest_url,
                        ws_url = :ws_url,
                        delimiter = :delimiter,
                        withdrawal_fee = :withdrawal_fee,
                        api_response_time = :api_response_time,
                        mapping = :mapping,
                        additional_info = :additional_info,
                        is_active = :is_active
                    WHERE name = :name
                    RETURNING id, name, api_rate_limit, trade_fees, is_active
                    """
                    )

                    result = conn.execute(update_stmt, {**update_data, "name": exchange_name})

                    updated = result.first()
                    if updated:
                        logger.info(f"Updated exchange: {updated.name}, active: {updated.is_active}")
                        # Create a simple dict for the response since we don't need the full object
                        created_exchanges.append(
                            {"id": updated.id, "name": updated.name, "is_active": updated.is_active}
                        )
            else:
                # Create new exchange, without specifying the id
                with db_service.engine.begin() as conn:
                    # Prepare data for insert
                    insert_data = {
                        "name": config.get("name"),
                        "api_rate_limit": config.get("api_rate_limit"),
                        "trade_fees": config.get("trade_fees"),
                        "rest_url": config.get("rest_url"),
                        "ws_url": config.get("ws_url"),
                        "delimiter": config.get("delimiter"),
                        "withdrawal_fee": json.dumps(config.get("withdrawal_fee")),
                        "api_response_time": config.get("api_response_time"),
                        "mapping": json.dumps(config.get("mapping")),
                        "additional_info": json.dumps(config.get("additional_info")),
                        "is_active": activate,
                    }

                    # Create the insert statement without the type casts
                    insert_stmt = sa.text(
                        """
                    INSERT INTO exchanges
                    (name, api_rate_limit, trade_fees, rest_url, ws_url, delimiter, 
                     withdrawal_fee, api_response_time, mapping, additional_info, is_active)
                    VALUES
                    (:name, :api_rate_limit, :trade_fees, :rest_url, :ws_url, :delimiter,
                     :withdrawal_fee, :api_response_time, :mapping, :additional_info, :is_active)
                    RETURNING id, name, api_rate_limit, trade_fees, is_active
                    """
                    )

                    result = conn.execute(insert_stmt, insert_data)

                    created = result.first()
                    if created:
                        logger.info(f"Created exchange: {created.name}, active: {created.is_active}")
                        # Create a simple dict for the response
                        created_exchanges.append(
                            {"id": created.id, "name": created.name, "is_active": created.is_active}
                        )

        except Exception as e:
            logger.error(f"Error creating/updating exchange {exchange_name}: {e}")

    logger.info(f"Created/updated {len(created_exchanges)} exchanges")
    return created_exchanges


def create_pairs(db_service, activate=False):
    """
    Create trading pairs from the exchange_config file.

    Args:
        db_service: Database service instance
        activate: Whether to activate the entities

    Returns:
        List of created pairs
    """
    logger.info("Creating trading pairs...")
    created_pairs = []

    for pair_symbol in get_all_pair_symbols():
        config = ALL_PAIRS.get(pair_symbol, {})
        if not config:
            logger.warning(f"No configuration found for pair: {pair_symbol}")
            continue

        try:
            # First check if pair already exists
            existing = db_service.get_pair_by_symbol(pair_symbol)

            if existing:
                logger.info(f"Pair already exists: {pair_symbol}, updating...")

                # Update existing pair (don't try to modify the ID)
                with db_service.engine.begin() as conn:
                    # Prepare data for update
                    update_data = {
                        "base_currency": config.get("base_currency"),
                        "quote_currency": config.get("quote_currency"),
                        "is_active": activate,
                    }

                    # Execute the update
                    update_stmt = sa.text(
                        """
                    UPDATE pairs
                    SET 
                        base_currency = :base_currency,
                        quote_currency = :quote_currency,
                        is_active = :is_active
                    WHERE symbol = :symbol
                    RETURNING id, symbol, base_currency, quote_currency, is_active
                    """
                    )

                    result = conn.execute(update_stmt, {**update_data, "symbol": pair_symbol})

                    updated = result.first()
                    if updated:
                        logger.info(f"Updated pair: {updated.symbol}, active: {updated.is_active}")
                        # Create a simple dict for the response
                        created_pairs.append(
                            {"id": updated.id, "symbol": updated.symbol, "is_active": updated.is_active}
                        )
            else:
                # Create new pair, without specifying the id
                with db_service.engine.begin() as conn:
                    # Prepare data for insert
                    insert_data = {
                        "base_currency": config.get("base_currency"),
                        "quote_currency": config.get("quote_currency"),
                        "symbol": pair_symbol,
                        "is_active": activate,
                    }

                    # Execute the insert without including id in the values
                    insert_stmt = sa.text(
                        """
                    INSERT INTO pairs
                    (base_currency, quote_currency, symbol, is_active)
                    VALUES
                    (:base_currency, :quote_currency, :symbol, :is_active)
                    RETURNING id, symbol, base_currency, quote_currency, is_active
                    """
                    )

                    result = conn.execute(insert_stmt, insert_data)

                    created = result.first()
                    if created:
                        logger.info(f"Created pair: {created.symbol}, active: {created.is_active}")
                        # Create a simple dict for the response
                        created_pairs.append(
                            {"id": created.id, "symbol": created.symbol, "is_active": created.is_active}
                        )

        except Exception as e:
            logger.error(f"Error creating/updating pair {pair_symbol}: {e}")

    logger.info(f"Created/updated {len(created_pairs)} pairs")
    return created_pairs


def create_strategies(db_service, activate=False):
    """
    Create strategies from the exchange_config file.

    Args:
        db_service: Database service instance
        activate: Whether to activate the entities

    Returns:
        List of created strategies
    """
    logger.info("Creating strategies...")
    created_strategies = []

    for strategy_name in get_all_strategy_names():
        config = ALL_STRATEGIES.get(strategy_name, {})
        if not config:
            logger.warning(f"No configuration found for strategy: {strategy_name}")
            continue

        try:
            # Create additional_info from config
            additional_info = {}
            if "additional_info" in config:
                additional_info.update(config.get("additional_info", {}))

            # Ensure exchanges and pairs are properly stored in additional_info
            # This is critical for the strategy to function correctly
            additional_info["type"] = config.get("type", "basic")

            # Make sure exchanges are explicitly included
            exchanges = config.get("exchanges", [])
            additional_info["exchanges"] = exchanges
            logger.info(f"Strategy {strategy_name} uses exchanges: {exchanges}")

            # Make sure pairs are explicitly included
            pairs = config.get("pairs", [])

            # Convert pair tuples to list format for better JSON storage
            if pairs and isinstance(pairs[0], tuple):
                formatted_pairs = []
                for pair in pairs:
                    if len(pair) == 2:
                        formatted_pairs.append(list(pair))
                additional_info["pairs"] = formatted_pairs
                logger.info(f"Strategy {strategy_name} uses pairs: {formatted_pairs}")
            else:
                additional_info["pairs"] = pairs
                logger.info(f"Strategy {strategy_name} uses pairs: {pairs}")

            # Add any strategy-specific parameters to additional_info
            for param in ["min_depth", "min_volume", "max_slippage", "execution_delay"]:
                if param in config:
                    additional_info[param] = config.get(param)

            # First check if strategy already exists
            existing = db_service.get_strategy_by_name(strategy_name)

            if existing:
                logger.info(f"Strategy already exists: {strategy_name}, updating...")

                # Update existing strategy (don't try to modify the ID)
                with db_service.engine.begin() as conn:
                    # Prepare data for update
                    update_data = {
                        "starting_capital": config.get("starting_capital"),
                        "min_spread": config.get("min_spread"),
                        "additional_info": json.dumps(additional_info),
                        "is_active": activate,
                    }

                    # Execute the update without type casts
                    update_stmt = sa.text(
                        """
                    UPDATE strategies
                    SET 
                        starting_capital = :starting_capital,
                        min_spread = :min_spread,
                        additional_info = :additional_info,
                        is_active = :is_active
                    WHERE name = :name
                    RETURNING id, name, starting_capital, min_spread, is_active
                    """
                    )

                    result = conn.execute(update_stmt, {**update_data, "name": strategy_name})

                    updated = result.first()
                    if updated:
                        logger.info(f"Updated strategy: {updated.name}, active: {updated.is_active}")
                        # Create a simple dict for the response
                        created_strategies.append(
                            {"id": updated.id, "name": updated.name, "is_active": updated.is_active}
                        )
            else:
                # Create new strategy, without specifying the id
                with db_service.engine.begin() as conn:
                    # Prepare data for insert
                    insert_data = {
                        "name": strategy_name,
                        "starting_capital": config.get("starting_capital"),
                        "min_spread": config.get("min_spread"),
                        "additional_info": json.dumps(additional_info),
                        "is_active": activate,
                        "total_profit": 0,
                        "total_loss": 0,
                        "net_profit": 0,
                        "trade_count": 0,
                    }

                    # Execute the insert without type casts
                    insert_stmt = sa.text(
                        """
                    INSERT INTO strategies
                    (name, starting_capital, min_spread, additional_info, is_active, 
                     total_profit, total_loss, net_profit, trade_count)
                    VALUES
                    (:name, :starting_capital, :min_spread, :additional_info, 
                     :is_active, :total_profit, :total_loss, :net_profit, :trade_count)
                    RETURNING id, name, starting_capital, min_spread, is_active
                    """
                    )

                    result = conn.execute(insert_stmt, insert_data)

                    created = result.first()
                    if created:
                        logger.info(f"Created strategy: {created.name}, active: {created.is_active}")
                        # Create a simple dict for the response
                        created_strategies.append(
                            {"id": created.id, "name": created.name, "is_active": created.is_active}
                        )

        except Exception as e:
            logger.error(f"Error creating/updating strategy {strategy_name}: {e}")

    logger.info(f"Created/updated {len(created_strategies)} strategies")
    return created_strategies


def prefill_database(activate=False):
    """
    Main function to prefill the database.

    Args:
        activate: Whether to activate entities by default
    """
    global _prefill_in_progress

    # Use a lock to prevent concurrent prefills
    if not _prefill_lock.acquire(blocking=False):
        logger.info("Database prefill already in progress, skipping this call")
        return

    try:
        # Additional check with a flag in case the lock mechanism fails
        if _prefill_in_progress:
            logger.info("Database prefill already in progress (flag check), skipping this call")
            return

        _prefill_in_progress = True

        # Skip command-line arguments when called programmatically
        if not activate:
            args = parse_args()
            activate = args.activate

        logger.info(f"Starting database prefill (activate={activate})...")

        try:
            with DatabaseService() as db_service:
                # Create entities in the correct order (exchanges and pairs first, then strategies)
                exchanges = create_exchanges(db_service, activate)
                pairs = create_pairs(db_service, activate)
                strategies = create_strategies(db_service, activate)

                # Final summary
                logger.info("Database prefill complete!")
                logger.info(f"Created/updated {len(exchanges)} exchanges")
                logger.info(f"Created/updated {len(pairs)} trading pairs")
                logger.info(f"Created/updated {len(strategies)} strategies")

                if activate:
                    logger.info("All entities were activated")
                else:
                    logger.info("All entities are inactive by default. Use '--activate' to activate them.")
                    logger.info("You can activate them individually through the admin interface.")

        except Exception as e:
            logger.error(f"Error prefilling database: {e}", exc_info=True)
            if not sys.argv[0].endswith("prefill_database.py"):
                # Only exit if run as a script directly
                pass
            else:
                exit(1)
    finally:
        _prefill_in_progress = False
        _prefill_lock.release()


if __name__ == "__main__":
    try:
        prefill_database()
    except Exception as e:
        logger.error("Fatal error in prefill_database", exc_info=e)
        sys.exit(1)
