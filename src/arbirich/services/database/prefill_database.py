#!/usr/bin/env python3
"""
Prefill Database Script - Initializes the database with configurations from the exchange_config file.

This script populates the database with:
1. Exchanges defined in ALL_EXCHANGES
2. Trading pairs defined in ALL_TRADING_PAIRS
3. Strategies defined in ALL_STRATEGIES

Usage:
    poetry run python -m src.arbirich.services.database.prefill_database [--activate]

Options:
    --activate    Activate all entities instead of leaving them inactive by default
    --smart-activate    Only activate exchanges and trading pairs used by active strategies
"""

import argparse
import json
import logging
import sys
import threading

import sqlalchemy as sa

from src.arbirich.config.config import (
    ALL_EXCHANGES,
    ALL_STRATEGIES,
    ALL_TRADING_PAIRS,
    EXECUTION_METHODS,
    RISK_PROFILES,
    STRATEGIES,
    TRADING_PAIRS,
    get_all_exchange_names,
    get_all_strategy_names,
)
from src.arbirich.models import TradingPair
from src.arbirich.services.database.database_service import DatabaseService

# Set up logging
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
        "--smart-activate",
        action="store_true",
        help="Only activate exchanges and trading pairs used by active strategies",
    )
    return parser.parse_args()


def get_active_strategy_dependencies():
    """
    Get the exchanges and trading pairs that are used by active strategies.

    Returns:
        Tuple of (set of exchange names, set of trading pair symbols, set of strategy names)
    """
    active_exchanges = set()
    active_trading_pairs = set()
    active_strategies = set()

    # Include strategies defined in STRATEGIES dictionary

    # Add all strategies from STRATEGIES to active_strategies
    active_strategies.update(STRATEGIES.keys())

    # Iterate through active strategies
    for strategy_name, strategy_config in STRATEGIES.items():
        # Get exchanges used by this strategy
        exchanges = strategy_config.get("exchanges", [])
        active_exchanges.update(exchanges)

        # Get trading pairs used by this strategy
        trading_pairs = strategy_config.get("pairs", [])
        # Convert trading pairs from tuples to symbols
        for trading_pair in trading_pairs:
            if isinstance(trading_pair, tuple) and len(trading_pair) == 2:
                trading_pair_symbol = f"{trading_pair[0]}-{trading_pair[1]}"
                active_trading_pairs.add(trading_pair_symbol)

    logger.debug(f"Active exchanges from strategies: {active_exchanges}")
    logger.debug(f"Active trading pairs from strategies: {active_trading_pairs}")
    logger.debug(f"Active strategies from config: {active_strategies}")

    return active_exchanges, active_trading_pairs, active_strategies


def create_risk_profiles(db_service):
    """Create or update risk profiles from config"""
    logger.info("Creating risk profiles...")
    created_profiles = []

    try:
        with db_service.engine.begin() as conn:
            for profile_name, profile_config in RISK_PROFILES.items():
                # Prepare data for insert/update
                profile_data = {
                    "name": profile_name,
                    "description": profile_config.get("description"),
                    "max_position_size_percentage": profile_config.get("max_position_size_percentage"),
                    "max_drawdown_percentage": profile_config.get("max_drawdown_percentage"),
                    "max_exposure_per_asset_percentage": profile_config.get("max_exposure_per_asset_percentage"),
                    "circuit_breaker_conditions": json.dumps(profile_config.get("circuit_breaker_conditions", {})),
                }

                # Check if profile exists
                result = conn.execute(
                    sa.text("SELECT id FROM risk_profiles WHERE name = :name"), {"name": profile_name}
                )
                existing = result.first()

                if existing:
                    # Update existing profile
                    conn.execute(
                        sa.text("""
                            UPDATE risk_profiles
                            SET description = :description,
                                max_position_size_percentage = :max_position_size_percentage,
                                max_drawdown_percentage = :max_drawdown_percentage,
                                max_exposure_per_asset_percentage = :max_exposure_per_asset_percentage,
                                circuit_breaker_conditions = :circuit_breaker_conditions
                            WHERE name = :name
                        """),
                        profile_data,
                    )
                    logger.debug(f"Updated risk profile: {profile_name}")
                else:
                    # Create new profile
                    result = conn.execute(
                        sa.text("""
                            INSERT INTO risk_profiles
                            (name, description, max_position_size_percentage, max_drawdown_percentage, 
                             max_exposure_per_asset_percentage, circuit_breaker_conditions)
                            VALUES
                            (:name, :description, :max_position_size_percentage, :max_drawdown_percentage,
                             :max_exposure_per_asset_percentage, :circuit_breaker_conditions)
                            RETURNING id
                        """),
                        profile_data,
                    )
                    created_profiles.append(profile_name)
                    logger.debug(f"Created risk profile: {profile_name}")

        logger.info(f"Created/updated {len(created_profiles)} risk profiles")
        return created_profiles
    except Exception as e:
        logger.error(f"Error creating risk profiles: {e}", exc_info=True)
        raise


def create_execution_methods(db_service):
    """Create or update execution strategies from config"""
    logger.info("Creating execution strategies...")
    created_strategies = []

    try:
        with db_service.engine.begin() as conn:
            for method_name, method_config in EXECUTION_METHODS.items():
                # Prepare data for insert/update
                strategy_data = {
                    "name": method_name,
                    "description": method_config.get("description"),
                    "timeout": method_config.get("timeout", 3000),
                    "retry_attempts": method_config.get("retry_attempts", 2),
                    "parameters": json.dumps(method_config),
                }

                # Check if strategy exists
                result = conn.execute(
                    sa.text("SELECT id FROM execution_methods WHERE name = :name"), {"name": method_name}
                )
                existing = result.first()

                if existing:
                    # Update existing strategy
                    conn.execute(
                        sa.text("""
                            UPDATE execution_methods
                            SET description = :description,
                                timeout = :timeout,
                                retry_attempts = :retry_attempts,
                                parameters = :parameters
                            WHERE name = :name
                        """),
                        strategy_data,
                    )
                    logger.debug(f"Updated execution strategy: {method_name}")
                else:
                    # Create new strategy
                    result = conn.execute(
                        sa.text("""
                            INSERT INTO execution_methods
                            (name, description, timeout, retry_attempts, parameters)
                            VALUES
                            (:name, :description, :timeout, :retry_attempts, :parameters)
                            RETURNING id
                        """),
                        strategy_data,
                    )
                    created_strategies.append(method_name)
                    logger.debug(f"Created execution strategy: {method_name}")

        logger.info(f"Created/updated {len(created_strategies)} execution strategies")
        return created_strategies
    except Exception as e:
        logger.error(f"Error creating execution strategies: {e}", exc_info=True)
        raise


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

            # Ensure exchanges and trading pairs are properly stored in additional_info
            # This is critical for the strategy to function correctly
            additional_info["type"] = config.get("type", "basic")

            # Make sure exchanges are explicitly included
            exchanges = config.get("exchanges", [])
            additional_info["exchanges"] = exchanges
            logger.debug(f"Strategy {strategy_name} uses exchanges: {exchanges}")

            # Make sure trading pairs are explicitly included
            trading_pairs = config.get("pairs", [])
            logger.debug(f"Strategy {strategy_name} uses trading pairs: {trading_pairs}")
            # Convert trading pair tuples to list format for better JSON storage
            if trading_pairs and isinstance(trading_pairs[0], tuple):
                formatted_trading_pairs = []
                for trading_pair in trading_pairs:
                    if len(trading_pair) == 2:
                        formatted_trading_pairs.append(list(trading_pair))
                additional_info["pairs"] = formatted_trading_pairs
                logger.debug(f"Strategy {strategy_name} uses trading pairs: {formatted_trading_pairs}")
            else:
                additional_info["pairs"] = trading_pairs
                logger.debug(f"Strategy {strategy_name} uses trading pairs: {trading_pairs}")

            # Add any strategy-specific parameters to additional_info
            for param in ["min_depth", "min_volume", "max_slippage", "execution_delay"]:
                if param in config:
                    additional_info[param] = config.get(param)

            # First check if strategy already exists
            existing = db_service.get_strategy_by_name(strategy_name)

            with db_service.engine.begin() as conn:
                if existing:
                    logger.debug(f"Strategy already exists: {strategy_name}, updating...")

                    # Update the strategy record (main table)
                    # Note: min_spread is NOT in the main strategies table according to schema.py
                    strategy_update_data = {
                        "starting_capital": config.get("starting_capital"),
                        "description": config.get("description", f"{strategy_name} strategy"),
                        "is_active": should_activate,
                    }

                    # Get or set strategy_type_id
                    strategy_type = config.get("type", "basic")
                    strategy_type_id = get_or_create_strategy_type(conn, strategy_type)
                    strategy_update_data["strategy_type_id"] = strategy_type_id

                    # Get or set risk_profile_id
                    risk_profile_id = get_or_create_risk_profile(conn, "default")
                    strategy_update_data["risk_profile_id"] = risk_profile_id

                    # Execute the update for the strategy itself
                    update_stmt = sa.text(
                        """
                        UPDATE strategies
                        SET 
                            starting_capital = :starting_capital,
                            description = :description,
                            is_active = :is_active,
                            strategy_type_id = :strategy_type_id,
                            risk_profile_id = :risk_profile_id
                        WHERE name = :name
                        RETURNING id, name, starting_capital, is_active
                        """
                    )

                    result = conn.execute(update_stmt, {**strategy_update_data, "name": strategy_name})
                    updated = result.first()

                    if updated:
                        strategy_id = updated.id
                        logger.debug(f"Updated strategy: {updated.name}, active: {updated.is_active}")

                        # Now update the strategy_parameters table with min_spread and other parameters
                        update_strategy_parameters(conn, strategy_id, config)

                        # Create strategy-exchange-pair mappings
                        create_strategy_exchange_pair_mapping(conn, strategy_id, config)

                        # Create a simple dict for the response
                        created_strategies.append(
                            {"id": updated.id, "name": updated.name, "is_active": updated.is_active}
                        )
                else:
                    # Create new strategy
                    # Get or set strategy_type_id
                    strategy_type = config.get("type", "basic")
                    strategy_type_id = get_or_create_strategy_type(conn, strategy_type)

                    # Get or set risk_profile_id
                    risk_profile_id = get_or_create_risk_profile(conn, "default")

                    # Prepare data for insert - matching the schema
                    insert_data = {
                        "name": strategy_name,
                        "description": config.get("description", f"{strategy_name} strategy"),
                        "strategy_type_id": strategy_type_id,
                        "risk_profile_id": risk_profile_id,
                        "starting_capital": config.get("starting_capital"),
                        "is_active": should_activate,
                        "total_profit": 0,
                        "total_loss": 0,
                        "net_profit": 0,
                        "trade_count": 0,
                    }

                    # Execute the insert without min_spread
                    insert_stmt = sa.text(
                        """
                        INSERT INTO strategies
                        (name, description, strategy_type_id, risk_profile_id, starting_capital, 
                         is_active, total_profit, total_loss, net_profit, trade_count)
                        VALUES
                        (:name, :description, :strategy_type_id, :risk_profile_id, :starting_capital, 
                         :is_active, :total_profit, :total_loss, :net_profit, :trade_count)
                        RETURNING id, name, starting_capital, is_active
                        """
                    )

                    result = conn.execute(insert_stmt, insert_data)
                    created = result.first()

                    if created:
                        strategy_id = created.id
                        logger.debug(f"Created strategy: {created.name}, active: {created.is_active}")

                        # Now create the strategy_parameters entry with min_spread
                        update_strategy_parameters(conn, strategy_id, config)

                        # Create strategy-exchange-pair mappings
                        create_strategy_exchange_pair_mapping(conn, strategy_id, config)

                        # Create a simple dict for the response
                        created_strategies.append(
                            {"id": created.id, "name": created.name, "is_active": created.is_active}
                        )

        except Exception as e:
            logger.error(f"Error creating/updating strategy {strategy_name}: {e}")

    logger.debug(f"Created/updated {len(created_strategies)} strategies")
    return created_strategies


def get_or_create_strategy_type(conn, strategy_type_name):
    """Get or create a strategy type and return its ID."""
    # First, try to get the existing strategy type
    select_stmt = sa.text("SELECT id FROM strategy_types WHERE name = :name")
    result = conn.execute(select_stmt, {"name": strategy_type_name})
    strategy_type = result.first()

    if strategy_type:
        return strategy_type.id

    # If not found, create a new one
    insert_stmt = sa.text(
        """
        INSERT INTO strategy_types (name, description, implementation_class)
        VALUES (:name, :description, :implementation_class)
        RETURNING id
        """
    )

    result = conn.execute(
        insert_stmt,
        {
            "name": strategy_type_name,
            "description": f"{strategy_type_name} strategy type",
            "implementation_class": f"{strategy_type_name.title()}Strategy",
        },
    )

    return result.scalar()


def get_or_create_risk_profile(conn, profile_name):
    """Get or create a risk profile and return its ID."""
    # First, try to get the existing risk profile
    select_stmt = sa.text("SELECT id FROM risk_profiles WHERE name = :name")
    result = conn.execute(select_stmt, {"name": profile_name})
    risk_profile = result.first()

    if risk_profile:
        return risk_profile.id

    # If not found, create a new one
    insert_stmt = sa.text(
        """
        INSERT INTO risk_profiles 
        (name, description, max_position_size_percentage, max_drawdown_percentage, max_exposure_per_asset_percentage)
        VALUES (:name, :description, :max_position_size_percentage, :max_drawdown_percentage, :max_exposure_per_asset_percentage)
        RETURNING id
        """
    )

    result = conn.execute(
        insert_stmt,
        {
            "name": profile_name,
            "description": f"{profile_name.title()} risk profile",
            "max_position_size_percentage": 10.0,  # Default values
            "max_drawdown_percentage": 5.0,
            "max_exposure_per_asset_percentage": 20.0,
        },
    )

    return result.scalar()


def update_strategy_parameters(conn, strategy_id, config):
    """Update or create strategy parameters."""
    # Check if parameters already exist
    select_stmt = sa.text("SELECT id FROM strategy_parameters WHERE strategy_id = :strategy_id")
    result = conn.execute(select_stmt, {"strategy_id": strategy_id})
    existing = result.first()

    # Prepare parameters data
    params_data = {
        "strategy_id": strategy_id,
        "min_spread": config.get("min_spread", 0.0001),
        "threshold": config.get("threshold", 0.0001),
        "max_slippage": config.get("max_slippage", 0.0005),
        "min_volume": config.get("min_volume", 0.001),
        "max_execution_time_ms": config.get("max_execution_time_ms", 3000),
        "additional_parameters": json.dumps(config.get("additional_info", {})),
    }

    if existing:
        # Update existing parameters
        update_stmt = sa.text(
            """
            UPDATE strategy_parameters
            SET min_spread = :min_spread,
                threshold = :threshold,
                max_slippage = :max_slippage,
                min_volume = :min_volume,
                max_execution_time_ms = :max_execution_time_ms,
                additional_parameters = :additional_parameters
            WHERE strategy_id = :strategy_id
            """
        )
        conn.execute(update_stmt, params_data)
    else:
        # Create new parameters
        insert_stmt = sa.text(
            """
            INSERT INTO strategy_parameters
            (strategy_id, min_spread, threshold, max_slippage, min_volume, max_execution_time_ms, additional_parameters)
            VALUES
            (:strategy_id, :min_spread, :threshold, :max_slippage, :min_volume, :max_execution_time_ms, :additional_parameters)
            """
        )
        conn.execute(insert_stmt, params_data)

    # Handle strategy type specific parameters if needed
    type_params = {
        "strategy_id": strategy_id,
        "target_volume": config.get("target_volume"),
        "min_depth": config.get("min_depth"),
        "min_depth_percentage": config.get("min_depth_percentage"),
        "parameters": json.dumps(config.get("type_parameters", {})),
    }

    # Check if type parameters already exist
    select_type_stmt = sa.text("SELECT id FROM strategy_type_parameters WHERE strategy_id = :strategy_id")
    type_result = conn.execute(select_type_stmt, {"strategy_id": strategy_id})
    existing_type = type_result.first()

    if existing_type:
        # Update existing type parameters
        update_type_stmt = sa.text(
            """
            UPDATE strategy_type_parameters
            SET target_volume = :target_volume,
                min_depth = :min_depth,
                min_depth_percentage = :min_depth_percentage,
                parameters = :parameters
            WHERE strategy_id = :strategy_id
            """
        )
        conn.execute(update_type_stmt, type_params)
    else:
        # Create new type parameters
        insert_type_stmt = sa.text(
            """
            INSERT INTO strategy_type_parameters
            (strategy_id, target_volume, min_depth, min_depth_percentage, parameters)
            VALUES
            (:strategy_id, :target_volume, :min_depth, :min_depth_percentage, :parameters)
            """
        )
        conn.execute(insert_type_stmt, type_params)

    # Set up execution strategy mapping
    execution_method = config.get("execution", {}).get("method", "parallel")
    execution_id = get_or_create_execution_strategy(conn, execution_method)

    # Check if execution mapping already exists
    select_exec_stmt = sa.text("SELECT id FROM strategy_execution_mapping WHERE strategy_id = :strategy_id")
    exec_result = conn.execute(select_exec_stmt, {"strategy_id": strategy_id})
    existing_exec = exec_result.first()

    if existing_exec:
        # Update execution mapping
        update_exec_stmt = sa.text(
            """
            UPDATE strategy_execution_mapping
            SET execution_strategy_id = :execution_strategy_id,
                is_active = TRUE
            WHERE strategy_id = :strategy_id
            """
        )
        conn.execute(update_exec_stmt, {"strategy_id": strategy_id, "execution_strategy_id": execution_id})
    else:
        # Create execution mapping
        insert_exec_stmt = sa.text(
            """
            INSERT INTO strategy_execution_mapping
            (strategy_id, execution_strategy_id, is_active, priority)
            VALUES
            (:strategy_id, :execution_strategy_id, TRUE, 100)
            """
        )
        conn.execute(insert_exec_stmt, {"strategy_id": strategy_id, "execution_strategy_id": execution_id})


def get_or_create_execution_strategy(conn, execution_method):
    """Get or create an execution strategy and return its ID."""
    # First, try to get the existing execution strategy
    select_stmt = sa.text("SELECT id FROM execution_methods WHERE name = :name")
    result = conn.execute(select_stmt, {"name": execution_method})
    execution = result.first()

    if execution:
        return execution.id

    # If not found, create a new one
    insert_stmt = sa.text(
        """
        INSERT INTO execution_methods 
        (name, description, timeout, retry_attempts, parameters)
        VALUES (:name, :description, :timeout, :retry_attempts, :parameters)
        RETURNING id
        """
    )

    # Get execution config from EXECUTION_METHODS if available
    execution_config = EXECUTION_METHODS.get(execution_method, {})

    result = conn.execute(
        insert_stmt,
        {
            "name": execution_method,
            "description": f"{execution_method.title()} execution strategy",
            "timeout": execution_config.get("timeout", 3000),
            "retry_attempts": execution_config.get("retry_attempts", 2),
            "parameters": json.dumps(execution_config),
        },
    )

    return result.scalar()


def create_strategy_exchange_pair_mapping(conn, strategy_id, config):
    """
    Create mappings between strategy, exchanges, and trading pairs.

    Args:
        conn: Database connection
        strategy_id: ID of the strategy
        config: Strategy configuration
    """
    try:
        # Get exchanges and trading pairs from config
        exchanges = config.get("exchanges", [])
        trading_pairs_data = config.get("pairs", [])

        # Convert trading pairs to consistent format
        trading_pairs = []
        for pair in trading_pairs_data:
            if isinstance(pair, tuple) and len(pair) == 2:
                trading_pairs.append(f"{pair[0]}-{pair[1]}")
            elif isinstance(pair, list) and len(pair) == 2:
                trading_pairs.append(f"{pair[0]}-{pair[1]}")
            elif isinstance(pair, str) and "-" in pair:
                trading_pairs.append(pair)

        logger.debug(
            f"Creating mappings for strategy_id={strategy_id}, exchanges={exchanges}, trading_pairs={trading_pairs}"
        )

        # For each exchange and trading pair combination, create a mapping
        for exchange in exchanges:
            # Get the exchange ID
            exchange_result = conn.execute(sa.text("SELECT id FROM exchanges WHERE name = :name"), {"name": exchange})
            exchange_row = exchange_result.first()
            if not exchange_row:
                logger.warning(f"Exchange {exchange} not found, skipping mappings")
                continue

            exchange_id = exchange_row.id

            for pair_symbol in trading_pairs:
                # Get the trading pair ID
                pair_result = conn.execute(
                    sa.text("SELECT id FROM trading_pairs WHERE symbol = :symbol"), {"symbol": pair_symbol}
                )
                pair_row = pair_result.first()
                if not pair_row:
                    logger.warning(f"Trading pair {pair_symbol} not found, skipping mapping")
                    continue

                pair_id = pair_row.id

                # Check if mapping already exists
                mapping_result = conn.execute(
                    sa.text("""
                        SELECT id FROM strategy_exchange_pair_mapping 
                        WHERE strategy_id = :strategy_id 
                        AND exchange_id = :exchange_id 
                        AND trading_pair_id = :trading_pair_id
                    """),
                    {"strategy_id": strategy_id, "exchange_id": exchange_id, "trading_pair_id": pair_id},
                )
                mapping_row = mapping_result.first()

                if mapping_row:
                    # Update existing mapping
                    conn.execute(
                        sa.text("""
                            UPDATE strategy_exchange_pair_mapping
                            SET is_active = TRUE
                            WHERE id = :id
                        """),
                        {"id": mapping_row.id},
                    )
                    logger.debug(f"Updated mapping for strategy={strategy_id}, exchange={exchange}, pair={pair_symbol}")
                else:
                    # Create new mapping
                    conn.execute(
                        sa.text("""
                            INSERT INTO strategy_exchange_pair_mapping
                            (strategy_id, exchange_id, trading_pair_id, is_active)
                            VALUES
                            (:strategy_id, :exchange_id, :trading_pair_id, TRUE)
                        """),
                        {"strategy_id": strategy_id, "exchange_id": exchange_id, "trading_pair_id": pair_id},
                    )
                    logger.debug(f"Created mapping for strategy={strategy_id}, exchange={exchange}, pair={pair_symbol}")
    except Exception as e:
        logger.error(f"Error creating strategy-exchange-pair mappings: {e}")


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
        active_exchanges, _, _ = (
            get_active_strategy_dependencies()
        )  # Unpack 3 values, discarding trading pairs and strategies

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
                logger.debug(f"Exchange already exists: {exchange_name}, updating...")
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
                        logger.debug(f"Updated exchange: {updated.name}, active: {updated.is_active}")
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
                        logger.debug(f"Created exchange: {created.name}, active: {created.is_active}")
                        # Create a simple dict for the response
                        created_exchanges.append(
                            {"id": created.id, "name": created.name, "is_active": created.is_active}
                        )
        except Exception as e:
            logger.error(f"Error creating/updating exchange {exchange_name}: {e}")

    logger.debug(f"Created/updated {len(created_exchanges)} exchanges")
    return created_exchanges


def create_trading_pairs(db_service, activate=False, smart_activate=False):
    """Create or update trading pairs in the database"""
    active_trading_pairs = set()

    # Get active trading pairs from strategies if using smart activation
    if smart_activate:
        _, active_trading_pairs, _ = get_active_strategy_dependencies()
        logger.debug(f"Smart activation enabled for trading pairs: {active_trading_pairs}")

    # Track created/updated trading pairs
    created_count = 0
    updated_count = 0

    # Use TRADING_PAIRS list instead of ALL_TRADING_PAIRS.keys()
    for base_currency, quote_currency in TRADING_PAIRS:
        # Construct the symbol
        symbol = f"{base_currency}-{quote_currency}"

        # Get full configuration data
        trading_pair_config = ALL_TRADING_PAIRS.get(symbol.upper(), {})
        if not trading_pair_config:
            # Create basic config if not found
            trading_pair_config = {
                "base_currency": base_currency,
                "quote_currency": quote_currency,
            }

        # Determine if this trading pair should be activated
        should_activate = activate or (smart_activate and symbol in active_trading_pairs)

        try:
            # Check if trading pair already exists
            existing = db_service.get_pair_by_symbol(symbol)
            if existing:
                # Update existing trading pair
                pair = TradingPair(
                    id=existing.id,
                    base_currency=base_currency,
                    quote_currency=quote_currency,
                    symbol=symbol,
                    is_active=should_activate,
                )
                db_service.trading_pair_repo.update(pair)
                updated_count += 1
                logger.debug(f"Updated trading pair: {symbol} (Active: {should_activate})")
            else:
                # Create new trading pair
                pair = TradingPair(
                    base_currency=base_currency,
                    quote_currency=quote_currency,
                    symbol=symbol,
                    is_active=should_activate,
                )
                db_service.trading_pair_repo.create(pair)
                created_count += 1
                logger.debug(f"Created trading pair: {symbol} (Active: {should_activate})")
        except Exception as e:
            logger.error(f"Error creating/updating trading pair {symbol}: {e}")

    logger.debug(f"Created {created_count} trading pairs, updated {updated_count} trading pairs")
    return created_count + updated_count


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
            logger.debug("Database prefill already in progress (flag check), skipping this call")
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
                # Create entities in the correct order
                # 1. Risk profiles and execution strategies first
                risk_profiles = create_risk_profiles(db_service)
                execution_methods = create_execution_methods(db_service)

                # 2. Then exchanges and trading pairs
                exchanges = create_exchanges(db_service, activate, smart_activate)
                trading_pairs = create_trading_pairs(db_service, activate, smart_activate)

                # 3. Finally create strategies that reference the above entities
                strategies = create_strategies(db_service, activate, smart_activate)

                # Final summary
                logger.info("Database prefill complete!")
                logger.info(f"Created/updated {len(risk_profiles)} risk profiles")
                logger.info(f"Created/updated {len(execution_methods)} execution strategies")
                logger.info(f"Created/updated {len(exchanges)} exchanges")
                logger.info(f"Created/updated {trading_pairs} trading pairs")
                logger.info(f"Created/updated {len(strategies)} strategies")

                if activate:
                    logger.info("All entities were activated")

                    # Double-check that strategies are active in the database
                    try:
                        strategies_check = db_service.get_active_strategies()
                        logger.debug(f"Verified {len(strategies_check)} active strategies in database")
                        if strategies_check:
                            for strat in strategies_check:
                                logger.debug(f"  - Active strategy: {strat.name}")
                        else:
                            logger.warning("No active strategies found after activation!")

                            # Force activate all strategies if none are active
                            logger.debug("Forcing activation of all strategies...")
                            with db_service.engine.begin() as conn:
                                result = conn.execute(sa.text("UPDATE strategies SET is_active = TRUE"))
                                if result.rowcount > 0:
                                    logger.debug(f"Successfully activated {result.rowcount} strategies")
                    except Exception as e:
                        logger.error(f"Error checking active strategies: {e}")
                elif smart_activate:
                    logger.info("Entities used by active strategies were activated")

                    # Double-check that strategies from STRATEGIES config are active
                    try:
                        strategies_check = db_service.get_active_strategies()
                        logger.debug(f"Verified {len(strategies_check)} active strategies in database")
                        if strategies_check:
                            for strat in strategies_check:
                                logger.debug(f"  - Active strategy: {strat.name}")
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
