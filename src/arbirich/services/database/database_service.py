import json
import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.sql import select

from src.arbirich.config.config import DATABASE_URL
from src.arbirich.models.models import (
    Exchange,
    Pair,
    Strategy,
    StrategyMetrics,
    TradeExecution,
    TradeOpportunity,
)
from src.arbirich.models.schema import (
    exchanges,
    metadata,
    pairs,
    strategies,
    trade_executions,
    trade_opportunities,
)

# Create engine
engine = sa.create_engine(DATABASE_URL)
metadata.create_all(engine)

logger = logging.getLogger(__name__)


class DatabaseService:
    """Service class to handle database operations"""

    def __init__(self, engine: Engine = engine):
        self.engine = engine
        self.connection = None
        self._session = None
        self.Session = sessionmaker(bind=engine)
        # Reference to tables for easier access
        self.tables = type(
            "Tables",
            (),
            {
                "exchanges": exchanges,
                "pairs": pairs,
                "strategies": strategies,
                "trade_opportunities": trade_opportunities,
                "trade_executions": trade_executions,
            },
        )
        self.logger = logger

    def __enter__(self):
        self.connection = self.engine.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()
            self.connection = None
        if self._session:
            self._session.close()
            self._session = None

    @property
    def session(self) -> Session:
        """Get a SQLAlchemy session."""
        if self._session is None:
            self._session = self.Session()
        return self._session

    def close(self):
        """
        Close the database connection and release resources.
        """
        try:
            if hasattr(self, "session"):
                self.session.close()
            if hasattr(self, "engine"):
                self.engine.dispose()
            logger.debug("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")

    # ---- Exchange operations ----
    def create_exchange(self, exchange: Exchange) -> Exchange:
        with self.engine.begin() as conn:
            # Convert to db dict if it's a Pydantic model
            if hasattr(exchange, "to_db_dict"):
                exchange_data = exchange.to_db_dict()
            else:
                exchange_data = {
                    "name": exchange.name,
                    "api_rate_limit": exchange.api_rate_limit,
                    "trade_fees": exchange.trade_fees,
                    "rest_url": exchange.rest_url,
                    "ws_url": exchange.ws_url,
                    "delimiter": exchange.delimiter,
                    "withdrawal_fee": exchange.withdrawal_fee,
                    "api_response_time": exchange.api_response_time,
                    "mapping": exchange.mapping,
                    "additional_info": exchange.additional_info,
                    "is_active": exchange.is_active,
                }

            result = conn.execute(exchanges.insert().values(**exchange_data).returning(*exchanges.c))
            row = result.first()
            return Exchange.model_validate(row._asdict())

    def get_exchange(self, exchange_id: int) -> Optional[Exchange]:
        with self.engine.begin() as conn:
            result = conn.execute(exchanges.select().where(exchanges.c.id == exchange_id))
            row = result.first()
            return Exchange.model_validate(row._asdict()) if row else None

    def get_exchange_by_name(self, name: str) -> Optional[Exchange]:
        """
        Get an exchange by its name.

        Args:
            name: The name of the exchange

        Returns:
            Exchange object if found, None otherwise
        """
        try:
            with self.engine.begin() as conn:
                result = conn.execute(exchanges.select().where(exchanges.c.name == name))
                row = result.first()
                return Exchange.model_validate(row._asdict()) if row else None
        except Exception as e:
            self.logger.error(f"Error getting exchange by name {name}: {e}")
            raise

    def get_all_exchanges(self) -> List[Exchange]:
        with self.engine.begin() as conn:
            result = conn.execute(exchanges.select())
            return [Exchange.model_validate(row._asdict()) for row in result]

    def get_active_exchanges(self) -> List[Exchange]:
        """Get all active exchanges."""
        with self.engine.begin() as conn:
            result = conn.execute(exchanges.select().where(exchanges.c.is_active == True))
            return [Exchange.model_validate(row._asdict()) for row in result]

    def update_exchange(self, exchange: Exchange) -> Exchange:
        """
        Update an existing exchange.

        Args:
            exchange: Exchange object to update

        Returns:
            Updated Exchange object
        """
        try:
            # Convert Pydantic model to dict for database
            if hasattr(exchange, "to_db_dict"):
                exchange_data = exchange.to_db_dict()
            else:
                exchange_data = exchange.model_dump()

            # Remove id from the data if present
            if "id" in exchange_data:
                exchange_id = exchange_data["id"]
                del exchange_data["id"]
            else:
                exchange_id = exchange.id

            # Update the exchange
            with self.engine.begin() as conn:
                conn.execute(exchanges.update().where(exchanges.c.id == exchange_id).values(**exchange_data))
                result = conn.execute(exchanges.select().where(exchanges.c.id == exchange_id))
                row = result.first()
                return Exchange.model_validate(row._asdict())

        except Exception as e:
            self.logger.error(f"Error updating exchange {exchange.name}: {e}")
            raise

    def activate_exchange(self, exchange_id: int) -> bool:
        """Activate an exchange by ID."""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(exchanges.update().where(exchanges.c.id == exchange_id).values(is_active=True))
                return result.rowcount > 0
        except Exception as e:
            logger.error(f"Error activating exchange {exchange_id}: {e}")
            return False

    def deactivate_exchange(self, exchange_id: int) -> bool:
        """Deactivate an exchange by ID."""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(exchanges.update().where(exchanges.c.id == exchange_id).values(is_active=False))
                return result.rowcount > 0
        except Exception as e:
            logger.error(f"Error deactivating exchange {exchange_id}: {e}")
            return False

    def delete_exchange(self, exchange_id: int) -> bool:
        with self.engine.begin() as conn:
            result = conn.execute(exchanges.delete().where(exchanges.c.id == exchange_id))
            return result.rowcount > 0

    def set_exchange_active(self, exchange: str, active: bool):
        """Set the active status of an exchange."""
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    sa.text("UPDATE exchanges SET is_active = :active WHERE name = :exchange"),
                    {"active": active, "exchange": exchange},
                )
        except Exception as e:
            logger.error(f"Error setting exchange '{exchange}' active status to {active}: {e}")

    def is_exchange_in_use(self, exchange: str) -> bool:
        """Check if an exchange is in use by any active strategy."""
        try:
            # Get all active strategies and check their additional_info field for the exchange
            strategies = self.get_active_strategies()
            for strategy in strategies:
                # Check if strategy has additional_info
                if not hasattr(strategy, "additional_info") or not strategy.additional_info:
                    continue

                additional_info = strategy.additional_info

                # Handle both dict and JSON string formats
                if isinstance(additional_info, str):
                    try:
                        additional_info = json.loads(additional_info)
                    except json.JSONDecodeError:
                        continue

                if isinstance(additional_info, dict):
                    # Check if 'exchanges' exists in additional_info and contains the target exchange
                    if "exchanges" in additional_info and isinstance(additional_info["exchanges"], list):
                        if exchange in additional_info["exchanges"]:
                            self.logger.debug(f"Exchange {exchange} is used by strategy {strategy.name}")
                            return True

            # No active strategy is using this exchange
            return False
        except Exception as e:
            logger.error(f"Error checking if exchange '{exchange}' is in use: {e}")
            # Default to False to allow deactivation
            return False

    # ---- Pair operations ----
    def create_pair(self, pair: Pair) -> Pair:
        """Create a pair with proper symbol handling."""
        try:
            # Ensure symbol is set if not provided
            if not pair.symbol:
                pair.symbol = f"{pair.base_currency}-{pair.quote_currency}"

            # Debug log to verify symbol is being set
            logger.debug(f"Creating pair with symbol: {pair.symbol}")

            # Convert to db dict if it's a Pydantic model
            if hasattr(pair, "to_db_dict"):
                pair_data = pair.to_db_dict()
            else:
                pair_data = {
                    "base_currency": pair.base_currency,
                    "quote_currency": pair.quote_currency,
                    "symbol": pair.symbol,
                    "is_active": pair.is_active,
                }

            # Remove the 'id' field if it exists, as it should be auto-generated
            pair_data.pop("id", None)

            with self.engine.begin() as conn:
                # Important: Include symbol in the values dictionary
                result = conn.execute(pairs.insert().values(**pair_data).returning(*pairs.c))
                row = result.first()
                return Pair.model_validate(row._asdict())
        except Exception as e:
            logger.error(f"Error creating pair: {e}")
            raise

    def get_pair(self, pair_id: int) -> Optional[Pair]:
        with self.engine.begin() as conn:
            result = conn.execute(pairs.select().where(pairs.c.id == pair_id))
            row = result.first()
            return Pair.model_validate(row._asdict()) if row else None

    def get_pair_by_symbol(self, symbol: str) -> Optional[Pair]:
        """
        Get a trading pair by its symbol.

        Args:
            symbol: The symbol of the trading pair

        Returns:
            Pair object if found, None otherwise
        """
        try:
            with self.engine.begin() as conn:
                result = conn.execute(pairs.select().where(pairs.c.symbol == symbol))
                row = result.first()
                return Pair.model_validate(row._asdict()) if row else None
        except Exception as e:
            self.logger.error(f"Error getting pair by symbol {symbol}: {e}")
            raise

    def get_all_pairs(self) -> List[Pair]:
        with self.engine.begin() as conn:
            result = conn.execute(pairs.select())
            return [Pair.model_validate(row._asdict()) for row in result]

    def get_active_pairs(self) -> List[Pair]:
        """Get all active trading pairs."""
        with self.engine.begin() as conn:
            result = conn.execute(pairs.select().where(pairs.c.is_active == True))
            return [Pair.model_validate(row._asdict()) for row in result]

    def update_pair(self, pair: Pair) -> Pair:
        """
        Update an existing trading pair.

        Args:
            pair: Pair object to update

        Returns:
            Updated Pair object
        """
        try:
            # Convert Pydantic model to dict for database
            if hasattr(pair, "to_db_dict"):
                pair_data = pair.to_db_dict()
            else:
                pair_data = pair.model_dump()

            # Remove id from the data if present
            if "id" in pair_data:
                pair_id = pair_data["id"]
                del pair_data["id"]
            else:
                pair_id = pair.id

            # Update the pair
            with self.engine.begin() as conn:
                conn.execute(pairs.update().where(pairs.c.id == pair_id).values(**pair_data))
                result = conn.execute(pairs.select().where(pairs.c.id == pair_id))
                row = result.first()
                return Pair.model_validate(row._asdict())

        except Exception as e:
            self.logger.error(f"Error updating pair {pair.symbol}: {e}")
            raise

    def activate_pair(self, pair_id: int) -> bool:
        """Activate a pair by ID."""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(pairs.update().where(pairs.c.id == pair_id).values(is_active=True))
                return result.rowcount > 0
        except Exception as e:
            logger.error(f"Error activating pair {pair_id}: {e}")
            return False

    def deactivate_pair(self, pair_id: int) -> bool:
        """Deactivate a pair by ID."""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(pairs.update().where(pairs.c.id == pair_id).values(is_active=False))
                return result.rowcount > 0
        except Exception as e:
            logger.error(f"Error deactivating pair {pair_id}: {e}")
            return False

    def set_pair_active(self, pair: tuple, active: bool):
        """
        Set the active status of a trading pair.

        Args:
            pair: A tuple containing (base_currency, quote_currency).
            active: Boolean indicating whether the pair should be active.
        """
        try:
            symbol = "-".join(pair)  # Construct the symbol dynamically
            with self.engine.begin() as conn:
                conn.execute(
                    sa.text("UPDATE pairs SET is_active = :active WHERE symbol = :symbol"),
                    {"active": active, "symbol": symbol},
                )
        except Exception as e:
            logger.error(f"Error setting pair '{pair}' active status to {active}: {e}")

    def is_pair_in_use(self, pair: tuple) -> bool:
        """
        Check if a trading pair is in use by any active strategy.

        Args:
            pair: A tuple containing (base_currency, quote_currency).

        Returns:
            True if the pair is in use, False otherwise.
        """
        try:
            symbol = "-".join(pair)  # Construct the symbol dynamically

            # Get all active strategies and check their additional_info field for the pair
            strategies = self.get_active_strategies()
            for strategy in strategies:
                # Check if strategy has additional_info
                if not hasattr(strategy, "additional_info") or not strategy.additional_info:
                    continue

                additional_info = strategy.additional_info

                # Handle both dict and JSON string formats
                if isinstance(additional_info, str):
                    try:
                        additional_info = json.loads(additional_info)
                    except json.JSONDecodeError:
                        continue

                if isinstance(additional_info, dict):
                    # Check if 'pairs' exists in additional_info and contains the target pair
                    if "pairs" in additional_info and isinstance(additional_info["pairs"], list):
                        if symbol in additional_info["pairs"]:
                            self.logger.debug(f"Pair {symbol} is used by strategy {strategy.name}")
                            return True

            # No active strategy is using this pair
            return False
        except Exception as e:
            logger.error(f"Error checking if pair '{pair}' is in use: {e}")
            # Default to False to allow deactivation
            return False

    # ---- Strategy operations ----
    def create_strategy(self, strategy: Strategy) -> Strategy:
        try:
            # Convert additional_info to JSON string if it's a dict
            strategy_data = {}
            if hasattr(strategy, "model_dump"):
                strategy_dict = strategy.model_dump()
                if isinstance(strategy_dict.get("additional_info"), dict):
                    strategy_dict["additional_info"] = json.dumps(strategy_dict["additional_info"])
                strategy_data = strategy_dict
            else:
                strategy_data = {
                    "name": strategy.name,
                    "starting_capital": strategy.starting_capital,
                    "min_spread": strategy.min_spread,
                    "additional_info": (
                        json.dumps(strategy.additional_info)
                        if isinstance(strategy.additional_info, dict)
                        else strategy.additional_info
                    ),
                    "is_active": strategy.is_active,
                }

            # Remove fields that shouldn't be part of the insert
            for field in ["id", "metrics", "latest_metrics"]:
                if field in strategy_data:
                    del strategy_data[field]

            with self.engine.begin() as conn:
                result = conn.execute(strategies.insert().values(**strategy_data).returning(*strategies.c))
                row = result.first()
                return Strategy.model_validate(row._asdict())
        except Exception as e:
            logger.error(f"Error creating strategy: {e}")
            raise

    def get_strategy(self, strategy_id: int) -> Optional[Strategy]:
        with self.engine.begin() as conn:
            result = conn.execute(strategies.select().where(strategies.c.id == strategy_id))
            row = result.first()
            return Strategy.model_validate(row._asdict()) if row else None

    def get_strategy_by_name(self, name: str) -> Optional[Strategy]:
        """
        Get a strategy by its name.

        Args:
            name: The name of the strategy

        Returns:
            Strategy object if found, None otherwise
        """
        try:
            with self.engine.begin() as conn:
                result = conn.execute(strategies.select().where(strategies.c.name == name))
                row = result.first()
                return Strategy.model_validate(row._asdict()) if row else None
        except Exception as e:
            self.logger.error(f"Error getting strategy by name {name}: {e}")
            raise

    def get_strategy_by_id(self, strategy_id: int) -> Optional[Strategy]:
        """
        Get a strategy by its ID.

        This is an alias for the get_strategy method for consistency
        with other methods that work with strategy by ID.

        Args:
            strategy_id: ID of the strategy to retrieve

        Returns:
            Strategy object if found, None otherwise
        """
        return self.get_strategy(strategy_id)

    def update_strategy_status(self, strategy_id: int, is_active: bool) -> bool:
        """Update strategy status by ID."""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(
                    strategies.update()
                    .where(strategies.c.id == strategy_id)
                    .values(is_active=is_active)
                    .returning(strategies.c.id)
                )
                updated_id = result.scalar()
                return updated_id is not None
        except Exception as e:
            logger.error(f"Error updating strategy status for ID {strategy_id}: {e}")
            return False

    def update_strategy_status_by_name(self, strategy_name: str, is_active: bool) -> bool:
        """Update strategy status by name."""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(
                    strategies.update()
                    .where(strategies.c.name == strategy_name)
                    .values(is_active=is_active)
                    .returning(strategies.c.id)
                )
                updated_id = result.scalar()
                return updated_id is not None
        except Exception as e:
            logger.error(f"Error updating strategy status for '{strategy_name}': {e}")
            return False

    def activate_strategy_by_name(self, strategy_name: str) -> bool:
        """Activate a strategy by name."""
        return self.update_strategy_status_by_name(strategy_name, True)

    def deactivate_strategy_by_name(self, strategy_name: str) -> bool:
        """Deactivate a strategy by name."""
        return self.update_strategy_status_by_name(strategy_name, False)

    def get_all_strategies(self) -> List[Strategy]:
        with self.engine.begin() as conn:
            result = conn.execute(strategies.select())
            return [Strategy.model_validate(row._asdict()) for row in result]

    def get_active_strategies(self) -> List[Strategy]:
        """Get all active strategies."""
        with self.engine.begin() as conn:
            result = conn.execute(strategies.select().where(strategies.c.is_active == True))
            return [Strategy.model_validate(row._asdict()) for row in result]

    def update_strategy(self, strategy: Strategy) -> Strategy:
        """
        Update an existing strategy.

        Args:
            strategy: Strategy object to update

        Returns:
            Updated Strategy object
        """
        try:
            # Convert additional_info to JSON string if it's a dict
            if hasattr(strategy, "model_dump"):
                strategy_dict = strategy.model_dump()
                if isinstance(strategy_dict.get("additional_info"), dict):
                    strategy_dict["additional_info"] = json.dumps(strategy_dict["additional_info"])
            else:
                strategy_dict = {
                    "name": strategy.name,
                    "starting_capital": strategy.starting_capital,
                    "min_spread": strategy.min_spread,
                    "additional_info": (
                        json.dumps(strategy.additional_info)
                        if isinstance(strategy.additional_info, dict)
                        else strategy.additional_info
                    ),
                    "total_profit": strategy.total_profit,
                    "total_loss": strategy.total_loss,
                    "net_profit": strategy.net_profit,
                    "trade_count": strategy.trade_count,
                    "is_active": strategy.is_active,
                }

            # Remove id from the dict
            if "id" in strategy_dict:
                strategy_id = strategy_dict["id"]
                del strategy_dict["id"]
            else:
                strategy_id = strategy.id

            # Remove fields that shouldn't be part of the update
            for field in ["metrics", "latest_metrics"]:
                if field in strategy_dict:
                    del strategy_dict[field]

            # Update the strategy
            with self.engine.begin() as conn:
                conn.execute(strategies.update().where(strategies.c.id == strategy_id).values(**strategy_dict))
                result = conn.execute(strategies.select().where(strategies.c.id == strategy_id))
                row = result.first()
                return Strategy.model_validate(row._asdict())
        except Exception as e:
            self.logger.error(f"Error updating strategy {strategy.name}: {e}")
            raise

    def update_strategy_stats(self, strategy_name: str, profit: float = 0, loss: float = 0, trade_count: int = 0):
        """Update strategy statistics after a trade execution"""
        try:
            with self.engine.begin() as conn:
                # Get current stats
                query = select(strategies.c.total_profit, strategies.c.total_loss, strategies.c.trade_count).where(
                    strategies.c.name == strategy_name
                )

                result = conn.execute(query).first()
                if result:
                    # Convert to compatible types
                    current_profit = Decimal(str(result.total_profit))
                    current_loss = Decimal(str(result.total_loss))
                    current_trade_count = result.trade_count

                    # Convert input params to Decimal to match database type
                    profit_decimal = Decimal(str(profit))
                    loss_decimal = Decimal(str(loss))

                    # Calculate new values
                    new_total_profit = current_profit + profit_decimal
                    new_total_loss = current_loss + loss_decimal
                    new_net_profit = new_total_profit - new_total_loss

                    # Update with new values
                    conn.execute(
                        strategies.update()
                        .where(strategies.c.name == strategy_name)
                        .values(
                            total_profit=new_total_profit,
                            total_loss=new_total_loss,
                            net_profit=new_net_profit,  # Add net_profit calculation
                            trade_count=current_trade_count + trade_count,
                            last_updated=datetime.now(),
                        )
                    )

                    logger.info(
                        f"Updated stats for {strategy_name}: "
                        f"profit +{profit}, loss +{loss}, net profit = {new_net_profit}, trades +{trade_count}"
                    )
                else:
                    logger.warning(f"Strategy {strategy_name} not found in database")
        except Exception as e:
            logger.error(f"Error updating strategy stats: {e}", exc_info=True)

    def activate_strategy(self, strategy_id: int) -> bool:
        """Activate a strategy by ID."""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(strategies.update().where(strategies.c.id == strategy_id).values(is_active=True))
                return result.rowcount > 0
        except Exception as e:
            logger.error(f"Error activating strategy {strategy_id}: {e}")
            return False

    def deactivate_strategy(self, strategy_id: int) -> bool:
        """Deactivate a strategy by ID."""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(strategies.update().where(strategies.c.id == strategy_id).values(is_active=False))
                return result.rowcount > 0
        except Exception as e:
            logger.error(f"Error deactivating strategy {strategy_id}: {e}")
            return False

    # ---- TradeOpportunity operations ----
    def create_trade_opportunity(self, opportunity: TradeOpportunity) -> TradeOpportunity:
        with self.engine.begin() as conn:
            result = conn.execute(
                trade_opportunities.insert().values(**opportunity.to_db_dict()).returning(*trade_opportunities.c)
            )
            row = result.first()
            return TradeOpportunity.model_validate(
                {
                    **row._asdict(),
                    "opportunity_timestamp": row.opportunity_timestamp.timestamp(),
                    "id": str(row.id),
                }
            )

    def get_trade_opportunity(self, opportunity_id: Union[str, UUID]) -> Optional[TradeOpportunity]:
        if isinstance(opportunity_id, str):
            opportunity_id = UUID(opportunity_id)

        with self.engine.begin() as conn:
            result = conn.execute(trade_opportunities.select().where(trade_opportunities.c.id == opportunity_id))
            row = result.first()
            if not row:
                return None

            return TradeOpportunity.model_validate(
                {
                    **row._asdict(),
                    "opportunity_timestamp": row.opportunity_timestamp.timestamp(),
                    "id": str(row.id),
                }
            )

    def get_opportunities_by_strategy(self, strategy_name: str) -> List[TradeOpportunity]:
        with self.engine.begin() as conn:
            result = conn.execute(trade_opportunities.select().where(trade_opportunities.c.strategy == strategy_name))
            opportunities = []
            for row in result:
                opportunity = TradeOpportunity.model_validate(
                    {
                        **row._asdict(),
                        "opportunity_timestamp": row.opportunity_timestamp.timestamp(),
                        "id": str(row.id),
                    }
                )
                opportunities.append(opportunity)
            return opportunities

    def get_recent_opportunities(self, count: int = 10, strategy_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get most recent trade opportunities from the database.

        Args:
            count: Maximum number of opportunities to return
            strategy_name: Optional strategy name to filter opportunities

        Returns:
            List of trade opportunities as dictionaries
        """
        try:
            query = (
                trade_opportunities.select().order_by(trade_opportunities.c.opportunity_timestamp.desc()).limit(count)
            )

            # Add strategy filter if provided
            if strategy_name:
                query = query.where(trade_opportunities.c.strategy == strategy_name)

            with self.engine.begin() as conn:
                result = conn.execute(query)
                opportunities = []

                for row in result:
                    opportunity = {
                        **row._asdict(),
                        "opportunity_timestamp": row.opportunity_timestamp.timestamp(),
                        "id": str(row.id),
                    }
                    opportunities.append(opportunity)

                return opportunities
        except Exception as e:
            logger.error(f"Error fetching recent opportunities: {e}")
            return []

    # ---- TradeExecution operations ----
    def create_trade_execution(self, execution: TradeExecution) -> TradeExecution:
        try:
            # Convert to db dict and log
            db_dict = execution.to_db_dict()
            logger.info(f"Converting execution to db dict: {db_dict}")
            with self.engine.begin() as conn:
                result = conn.execute(trade_executions.insert().values(**db_dict).returning(*trade_executions.c))
                row = result.first()
                return TradeExecution.model_validate(
                    {
                        **row._asdict(),
                        "execution_timestamp": row.execution_timestamp.timestamp(),
                        "id": str(row.id),
                        "opportunity_id": str(row.opportunity_id) if row.opportunity_id else None,
                    }
                )
        except Exception as e:
            logger.error(f"Error creating trade execution: {e}", exc_info=True)
            raise

    def get_trade_execution(self, execution_id: Union[str, UUID]) -> Optional[TradeExecution]:
        if isinstance(execution_id, str):
            execution_id = UUID(execution_id)
        with self.engine.begin() as conn:
            result = conn.execute(trade_executions.select().where(trade_executions.c.id == execution_id))
            row = result.first()
            if not row:
                return None

            return TradeExecution.model_validate(
                {
                    **row._asdict(),
                    "execution_timestamp": row.execution_timestamp.timestamp(),
                    "id": str(row.id),
                    "opportunity_id": str(row.opportunity_id) if row.opportunity_id else None,
                }
            )

    def get_executions_by_strategy(self, strategy_name: str) -> List[TradeExecution]:
        with self.engine.begin() as conn:
            result = conn.execute(trade_executions.select().where(trade_executions.c.strategy == strategy_name))
            executions = []
            for row in result:
                execution = TradeExecution.model_validate(
                    {
                        **row._asdict(),
                        "execution_timestamp": row.execution_timestamp.timestamp(),
                        "id": str(row.id),
                        "opportunity_id": str(row.opportunity_id) if row.opportunity_id else None,
                    }
                )
                executions.append(execution)
            return executions

    def get_recent_executions(self, count: int = 10, strategy_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get most recent trade executions from the database.

        Args:
            count: Maximum number of executions to return
            strategy_name: Optional strategy name to filter executions

        Returns:
            List of trade executions as dictionaries
        """
        try:
            query = trade_executions.select().order_by(trade_executions.c.execution_timestamp.desc()).limit(count)
            # Add strategy filter if provided
            if strategy_name:
                query = query.where(trade_executions.c.strategy == strategy_name)
            with self.engine.begin() as conn:
                result = conn.execute(query)
                executions = []

                for row in result:
                    execution = {
                        **row._asdict(),
                        "execution_timestamp": row.execution_timestamp.timestamp(),
                        "id": str(row.id),
                        "opportunity_id": str(row.opportunity_id) if row.opportunity_id else None,
                    }
                    executions.append(execution)

                return executions
        except Exception as e:
            logger.error(f"Error fetching recent executions: {e}")
            return []

    # ---- Strategy Metrics operations ----
    def create_strategy_metrics(self, metrics):
        """Create a new strategy metrics record."""
        self.session.add(metrics)
        self.session.commit()
        return metrics

    def get_strategy_metrics(self, metrics_id=None, strategy_id=None):
        """
        Get strategy metrics by ID or all metrics for a specific strategy.

        Args:
            metrics_id: Optional ID of the specific metrics record
            strategy_id: Optional ID of the strategy to get all metrics for

        Returns:
            A single metrics record or list of metrics records
        """
        try:
            if metrics_id is not None:
                return self.session.query(StrategyMetrics).filter(StrategyMetrics.id == metrics_id).first()
            elif strategy_id is not None:
                return (
                    self.session.query(StrategyMetrics)
                    .filter(StrategyMetrics.strategy_id == strategy_id)
                    .order_by(StrategyMetrics.period_end.desc())
                    .all()
                )
            else:
                return None
        except Exception as e:
            logger.error(f"Error getting strategy metrics: {e}")

    def get_latest_strategy_metrics(self, strategy_id):
        """Get the most recent metrics for a strategy."""
        return (
            self.session.query(StrategyMetrics)
            .filter(StrategyMetrics.strategy_id == strategy_id)
            .order_by(StrategyMetrics.period_end.desc())
            .first()
        )

    def get_strategy_metrics_for_period(
        self, strategy_id: int, start_date: datetime, end_date: datetime
    ) -> List[StrategyMetrics]:
        """Get all metrics for a strategy within a period."""
        try:
            return (
                self.session.query(StrategyMetrics)
                .filter(
                    StrategyMetrics.strategy_id == strategy_id,
                    StrategyMetrics.period_end >= start_date,
                    StrategyMetrics.period_end <= end_date,
                )
                .order_by(StrategyMetrics.period_end.asc())
                .all()
            )
        except Exception as e:
            logger.error(f"Error getting metrics for strategy {strategy_id} in period {start_date} to {end_date}: {e}")
            return []

    def create_strategy_trading_pair_metrics(self, metrics):
        """Create a new strategy trading pair metrics record."""
        self.session.add(metrics)
        self.session.commit()
        return metrics

    def create_strategy_exchange_metrics(self, metrics):
        """Create a new strategy exchange metrics record."""
        self.session.add(metrics)
        self.session.commit()
        return metrics

    def save_strategy_metrics(self, metrics):
        """Save strategy metrics to the database."""
        try:
            self.session.add(metrics)
            self.session.commit()
            return metrics
        except Exception as e:
            self.session.rollback()
            raise e


def cleanup_db_connections():
    """
    Cleanup any open database connections.
    This is called during application shutdown.
    """
    import sqlalchemy

    logger = logging.getLogger(__name__)
    try:
        # Dispose of the engine's connection pool
        db_instance = DatabaseService()
        logger.info("Disposing of database engine connections")

        if hasattr(db_instance, "engine") and db_instance.engine:
            db_instance.engine.dispose()
            logger.info("Database engine connections disposed")

        # Check for SQLAlchemy's global engine registry
        if hasattr(sqlalchemy, "_registries"):
            for registry in sqlalchemy._registries:
                if hasattr(registry, "dispose_all"):
                    logger.info("Disposing of all engines in registry")
                    registry.dispose_all()

        logger.info("Database connections cleanup completed")
    except Exception as e:
        logger.error(f"Error cleaning up database connections: {e}")
