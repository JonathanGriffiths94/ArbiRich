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
    --smart-activate    Only activate exchanges and pairs used by active strategies
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
    STRATEGIES,
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
    activation_group = parser.add_mutually_exclusive_group()
    activation_group.add_argument(
        "--activate", action="store_true", help="Activate all entities instead of leaving them inactive"
    )
    activation_group.add_argument(
        "--smart-activate", action="store_true", help="Only activate exchanges and pairs used by active strategies"
    )
    return parser.parse_args()


def get_active_strategy_dependencies():
    """
    Get the exchanges and pairs that are used by active strategies.

    Returns:
        Tuple of (set of exchange names, set of pair symbols, set of strategy names)
    """
    active_exchanges = set()
    active_pairs = set()
    active_strategies = set()

    # Include strategies defined in STRATEGIES dictionary

    # Add all strategies from STRATEGIES to active_strategies
    active_strategies.update(STRATEGIES.keys())

    # Iterate through active strategies
    for strategy_name, strategy_config in STRATEGIES.items():
        # Get exchanges used by this strategy
        exchanges = strategy_config.get("exchanges", [])
        active_exchanges.update(exchanges)

        # Get pairs used by this strategy
        pairs = strategy_config.get("pairs", [])
        # Convert pairs from tuples to symbols
        for pair in pairs:
            if isinstance(pair, tuple) and len(pair) == 2:
                pair_symbol = f"{pair[0]}-{pair[1]}"
                active_pairs.add(pair_symbol)

    logger.info(f"Active exchanges from strategies: {active_exchanges}")
    logger.info(f"Active pairs from strategies: {active_pairs}")
    logger.info(f"Active strategies from config: {active_strategies}")

    return active_exchanges, active_pairs, active_strategies


def create_strategies(db_service, activate=False, smart_activate=False):
    """
    Create strategies from the exchange_config file.

    Args:
        db_service: Database service instance
        activate: Whether to activate all entities
        smart_activate: Whether to only activate strategies defined in STRATEGIES

    Returns:
        List of created strategies
    """
    logger.info("Creating strategies...")
    created_strategies = []

    # Get active strategies if using smart activation
    active_strategy_names = set()
    if smart_activate:
        _, _, active_strategy_names = get_active_strategy_dependencies()

    for strategy_name in get_all_strategy_names():
        config = ALL_STRATEGIES.get(strategy_name, {})
        if not config:
            logger.warning(f"No configuration found for strategy: {strategy_name}")
            continue

        # Determine if this strategy should be activated
        should_activate = activate or (smart_activate and strategy_name in active_strategy_names)

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
                        "is_active": should_activate,  # Use the smart activation logic
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
                        "is_active": should_activate,  # Use the smart activation logic
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


def create_exchanges(db_service, activate=False, smart_activate=False):
    """
    Create exchanges from the exchange_config file.

    Args:
        db_service: Database service instance
        activate: Whether to activate all entities
        smart_activate: Whether to only activate exchanges used by active strategies

    Returns:
        List of created exchanges
    """
    logger.info("Creating exchanges...")
    created_exchanges = []

    # Get list of exchanges to activate if using smart activation
    active_exchanges = set()
    if smart_activate:
        active_exchanges, _, _ = get_active_strategy_dependencies()  # Unpack 3 values, discarding pairs and strategies

    for exchange_name in get_all_exchange_names():
        config = ALL_EXCHANGES.get(exchange_name, {})
        if not config:
            logger.warning(f"No configuration found for exchange: {exchange_name}")
            continue

        # Determine if this exchange should be activated
        should_activate = activate or (smart_activate and exchange_name in active_exchanges)

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
                        "is_active": should_activate,
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
                        "is_active": should_activate,
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


def create_pairs(db_service, activate=False, smart_activate=False):
    """
    Create trading pairs from the exchange_config file.

    Args:
        db_service: Database service instance
        activate: Whether to activate all entities
        smart_activate: Whether to only activate pairs used by active strategies

    Returns:
        List of created pairs
    """
    logger.info("Creating trading pairs...")
    created_pairs = []

    # Get list of pairs to activate if using smart activation
    active_pairs = set()
    if smart_activate:
        _, active_pairs, _ = get_active_strategy_dependencies()  # Unpack 3 values, discarding exchanges and strategies

    for pair_symbol in get_all_pair_symbols():
        config = ALL_PAIRS.get(pair_symbol, {})
        if not config:
            logger.warning(f"No configuration found for pair: {pair_symbol}")
            continue

        # Determine if this pair should be activated
        should_activate = activate or (smart_activate and pair_symbol in active_pairs)

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
                        "is_active": should_activate,
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
                        "is_active": should_activate,
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


# def create_strategies(db_service, activate=False):
#     """
#     Create strategies from the exchange_config file.

#     Args:
#         db_service: Database service instance
#         activate: Whether to activate the entities

#     Returns:
#         List of created strategies
#     """
#     logger.info("Creating strategies...")
#     created_strategies = []

#     for strategy_name in get_all_strategy_names():
#         config = ALL_STRATEGIES.get(strategy_name, {})
#         if not config:
#             logger.warning(f"No configuration found for strategy: {strategy_name}")
#             continue

#         try:
#             # Create additional_info from config
#             additional_info = {}
#             if "additional_info" in config:
#                 additional_info.update(config.get("additional_info", {}))

#             # Ensure exchanges and pairs are properly stored in additional_info
#             # This is critical for the strategy to function correctly
#             additional_info["type"] = config.get("type", "basic")

#             # Make sure exchanges are explicitly included
#             exchanges = config.get("exchanges", [])
#             additional_info["exchanges"] = exchanges
#             logger.info(f"Strategy {strategy_name} uses exchanges: {exchanges}")

#             # Make sure pairs are explicitly included
#             pairs = config.get("pairs", [])

#             # Convert pair tuples to list format for better JSON storage
#             if pairs and isinstance(pairs[0], tuple):
#                 formatted_pairs = []
#                 for pair in pairs:
#                     if len(pair) == 2:
#                         formatted_pairs.append(list(pair))
#                 additional_info["pairs"] = formatted_pairs
#                 logger.info(f"Strategy {strategy_name} uses pairs: {formatted_pairs}")
#             else:
#                 additional_info["pairs"] = pairs
#                 logger.info(f"Strategy {strategy_name} uses pairs: {pairs}")

#             # Add any strategy-specific parameters to additional_info
#             for param in ["min_depth", "min_volume", "max_slippage", "execution_delay"]:
#                 if param in config:
#                     additional_info[param] = config.get(param)

#             # First check if strategy already exists
#             existing = db_service.get_strategy_by_name(strategy_name)

#             if existing:
#                 logger.info(f"Strategy already exists: {strategy_name}, updating...")

#                 # Update existing strategy (don't try to modify the ID)
#                 with db_service.engine.begin() as conn:
#                     # Prepare data for update
#                     update_data = {
#                         "starting_capital": config.get("starting_capital"),
#                         "min_spread": config.get("min_spread"),
#                         "additional_info": json.dumps(additional_info),
#                         "is_active": activate,
#                     }

#                     # Execute the update without type casts
#                     update_stmt = sa.text(
#                         """
#                     UPDATE strategies
#                     SET
#                         starting_capital = :starting_capital,
#                         min_spread = :min_spread,
#                         additional_info = :additional_info,
#                         is_active = :is_active
#                     WHERE name = :name
#                     RETURNING id, name, starting_capital, min_spread, is_active
#                     """
#                     )

#                     result = conn.execute(update_stmt, {**update_data, "name": strategy_name})

#                     updated = result.first()
#                     if updated:
#                         logger.info(f"Updated strategy: {updated.name}, active: {updated.is_active}")
#                         # Create a simple dict for the response
#                         created_strategies.append(
#                             {"id": updated.id, "name": updated.name, "is_active": updated.is_active}
#                         )
#             else:
#                 # Create new strategy, without specifying the id
#                 with db_service.engine.begin() as conn:
#                     # Prepare data for insert
#                     insert_data = {
#                         "name": strategy_name,
#                         "starting_capital": config.get("starting_capital"),
#                         "min_spread": config.get("min_spread"),
#                         "additional_info": json.dumps(additional_info),
#                         "is_active": activate,
#                         "total_profit": 0,
#                         "total_loss": 0,
#                         "net_profit": 0,
#                         "trade_count": 0,
#                     }

#                     # Execute the insert without type casts
#                     insert_stmt = sa.text(
#                         """
#                     INSERT INTO strategies
#                     (name, starting_capital, min_spread, additional_info, is_active,
#                      total_profit, total_loss, net_profit, trade_count)
#                     VALUES
#                     (:name, :starting_capital, :min_spread, :additional_info,
#                      :is_active, :total_profit, :total_loss, :net_profit, :trade_count)
#                     RETURNING id, name, starting_capital, min_spread, is_active
#                     """
#                     )

#                     result = conn.execute(insert_stmt, insert_data)

#                     created = result.first()
#                     if created:
#                         logger.info(f"Created strategy: {created.name}, active: {created.is_active}")
#                         # Create a simple dict for the response
#                         created_strategies.append(
#                             {"id": created.id, "name": created.name, "is_active": created.is_active}
#                         )

#         except Exception as e:
#             logger.error(f"Error creating/updating strategy {strategy_name}: {e}")

#     logger.info(f"Created/updated {len(created_strategies)} strategies")
#     return created_strategies


def prefill_database(activate=False, smart_activate=False):
    """
    Main function to prefill the database.

    Args:
        activate: Whether to activate all entities by default
        smart_activate: Whether to only activate entities used by active strategies
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
        if not activate and not smart_activate:
            args = parse_args()
            activate = args.activate
            smart_activate = args.smart_activate

        logger.info(f"Starting database prefill (activate={activate}, smart_activate={smart_activate})...")

        try:
            with DatabaseService() as db_service:
                # Create entities in the correct order (exchanges and pairs first, then strategies)
                exchanges = create_exchanges(db_service, activate, smart_activate)
                pairs = create_pairs(db_service, activate, smart_activate)
                strategies = create_strategies(
                    db_service, activate, smart_activate
                )  # Pass smart_activate to create_strategies

                # Final summary
                logger.info("Database prefill complete!")
                logger.info(f"Created/updated {len(exchanges)} exchanges")
                logger.info(f"Created/updated {len(pairs)} trading pairs")
                logger.info(f"Created/updated {len(strategies)} strategies")

                if activate:
                    logger.info("All entities were activated")

                    # Double-check that strategies are active in the database
                    try:
                        strategies_check = db_service.get_active_strategies()
                        logger.info(f"Verified {len(strategies_check)} active strategies in database")
                        if strategies_check:
                            for strat in strategies_check:
                                logger.info(f"  - Active strategy: {strat.name}")
                        else:
                            logger.warning("No active strategies found after activation!")

                            # Force activate all strategies if none are active
                            logger.info("Forcing activation of all strategies...")
                            with db_service.engine.begin() as conn:
                                result = conn.execute(sa.text("UPDATE strategies SET is_active = TRUE"))
                                if result.rowcount > 0:
                                    logger.info(f"Successfully activated {result.rowcount} strategies")
                    except Exception as e:
                        logger.error(f"Error checking active strategies: {e}")
                elif smart_activate:
                    logger.info("Entities used by active strategies were activated")

                    # Double-check that strategies from STRATEGIES config are active
                    try:
                        strategies_check = db_service.get_active_strategies()
                        logger.info(f"Verified {len(strategies_check)} active strategies in database")
                        if strategies_check:
                            for strat in strategies_check:
                                logger.info(f"  - Active strategy: {strat.name}")
                    except Exception as e:
                        logger.error(f"Error checking active strategies: {e}")
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
