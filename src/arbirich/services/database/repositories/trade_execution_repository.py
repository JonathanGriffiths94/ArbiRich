import logging
from typing import List, Optional, Union
from uuid import UUID

import sqlalchemy as sa

from arbirich.models.db.schema import exchanges, strategies, trade_executions, trading_pairs
from src.arbirich.models import TradeExecution
from src.arbirich.services.database.base_repository import BaseRepository
from src.arbirich.services.database.utils.timestamp_converter import convert_unix_timestamp_for_db


class TradeExecutionRepository(BaseRepository[TradeExecution]):
    """Repository for Trade Execution entity operations"""

    def __init__(self, *args, **kwargs):
        super().__init__(model_class=TradeExecution, *args, **kwargs)
        self.table = trade_executions
        self.strategies_table = strategies
        self.trading_pairs_table = trading_pairs
        self.exchanges_table = exchanges
        self.logger = logging.getLogger(__name__)

    def get_strategy_id_by_name(self, strategy_name: str) -> int:
        """
        Look up a strategy ID by its name.

        Args:
            strategy_name: The name of the strategy

        Returns:
            The integer ID of the strategy

        Raises:
            ValueError: If strategy with the given name doesn't exist
        """
        try:
            with self.engine.begin() as conn:
                # Query the strategies table to find the ID
                query = sa.select(self.strategies_table.c.id).where(self.strategies_table.c.name == strategy_name)
                result = conn.execute(query).first()

                if result is None:
                    self.logger.error(f"Strategy with name '{strategy_name}' not found")
                    raise ValueError(f"Strategy with name '{strategy_name}' not found")

                strategy_id = result[0]
                self.logger.debug(f"Found strategy ID {strategy_id} for name '{strategy_name}'")
                return strategy_id
        except Exception as e:
            self.logger.error(f"Error getting strategy ID for name '{strategy_name}': {e}")
            raise

    def get_trading_pair_id(self, pair_symbol: str) -> int:
        """
        Look up a trading pair ID by its symbol.

        Args:
            pair_symbol: The trading pair symbol (e.g., 'LINK-USDT')

        Returns:
            The integer ID of the trading pair

        Raises:
            ValueError: If trading pair with the given symbol doesn't exist
        """
        try:
            with self.engine.begin() as conn:
                # Query the trading_pairs table to find the ID
                query = sa.select(self.trading_pairs_table.c.id).where(self.trading_pairs_table.c.symbol == pair_symbol)
                result = conn.execute(query).first()

                if result is None:
                    self.logger.error(f"Trading pair with symbol '{pair_symbol}' not found")
                    raise ValueError(f"Trading pair with symbol '{pair_symbol}' not found")

                pair_id = result[0]
                return pair_id
        except Exception as e:
            self.logger.error(f"Error getting trading pair ID for symbol '{pair_symbol}': {e}")
            raise

    def get_exchange_id(self, exchange_name: str) -> int:
        """
        Look up an exchange ID by its name.

        Args:
            exchange_name: The exchange name (e.g., 'cryptocom', 'bybit')

        Returns:
            The integer ID of the exchange

        Raises:
            ValueError: If exchange with the given name doesn't exist
        """
        try:
            with self.engine.begin() as conn:
                # Query the exchanges table to find the ID
                query = sa.select(self.exchanges_table.c.id).where(self.exchanges_table.c.name == exchange_name)
                result = conn.execute(query).first()

                if result is None:
                    self.logger.error(f"Exchange with name '{exchange_name}' not found")
                    raise ValueError(f"Exchange with name '{exchange_name}' not found")

                exchange_id = result[0]
                return exchange_id
        except Exception as e:
            self.logger.error(f"Error getting exchange ID for name '{exchange_name}': {e}")
            raise

    def create(self, execution: TradeExecution) -> TradeExecution:
        """
        Create a new trade execution

        Raises:
            ValueError: If any referenced entity (strategy, trading pair, exchange) doesn't exist
        """
        try:
            # Get strategy ID if strategy is a string name
            if execution.strategy:
                if isinstance(execution.strategy, str):
                    strategy_id = self.get_strategy_id_by_name(execution.strategy)
                elif isinstance(execution.strategy, int):
                    strategy_id = execution.strategy
                else:
                    raise TypeError(f"Unexpected type for strategy: {type(execution.strategy)}")
            else:
                raise ValueError("Strategy is required")

            # Look up IDs for trading pair and exchanges
            if isinstance(execution.pair, str):
                trading_pair_id = self.get_trading_pair_id(execution.pair)
            else:
                raise TypeError(f"Unexpected type for pair: {type(execution.pair)}")

            if isinstance(execution.buy_exchange, str):
                buy_exchange_id = self.get_exchange_id(execution.buy_exchange)
            else:
                raise TypeError(f"Unexpected type for buy_exchange: {type(execution.buy_exchange)}")

            if isinstance(execution.sell_exchange, str):
                sell_exchange_id = self.get_exchange_id(execution.sell_exchange)
            else:
                raise TypeError(f"Unexpected type for sell_exchange: {type(execution.sell_exchange)}")

            # Convert to match database column names
            db_data = {
                "id": execution.id,
                "strategy_id": strategy_id,
                "trading_pair_id": trading_pair_id,
                "buy_exchange_id": buy_exchange_id,
                "sell_exchange_id": sell_exchange_id,
                "executed_buy_price": execution.executed_buy_price,
                "executed_sell_price": execution.executed_sell_price,
                "spread": execution.spread,
                "volume": execution.volume,
                "execution_timestamp": execution.execution_timestamp,
                "execution_id": execution.execution_id,
                "opportunity_id": execution.opportunity_id,
            }

            # Convert the timestamp before insertion
            if "execution_timestamp" in db_data:
                db_data["execution_timestamp"] = convert_unix_timestamp_for_db(db_data["execution_timestamp"])

            self.logger.debug(f"Inserting execution with data: {db_data}")

            with self.engine.begin() as conn:
                result = conn.execute(self.table.insert().values(**db_data).returning(*self.table.c))
                row = result.first()

                # Store the original string values before submitting to database
                original_strategy = execution.strategy
                original_pair = execution.pair
                original_buy_exchange = execution.buy_exchange
                original_sell_exchange = execution.sell_exchange

                # Map database column names back to model field names, preserving original string values
                return TradeExecution(
                    id=str(row.id),
                    strategy=original_strategy,
                    pair=original_pair,
                    buy_exchange=original_buy_exchange,
                    sell_exchange=original_sell_exchange,
                    executed_buy_price=float(row.executed_buy_price),
                    executed_sell_price=float(row.executed_sell_price),
                    spread=float(row.spread),
                    volume=float(row.volume),
                    execution_timestamp=row.execution_timestamp.timestamp() if row.execution_timestamp else None,
                    execution_id=row.execution_id,
                    opportunity_id=str(row.opportunity_id) if row.opportunity_id else None,
                )
        except Exception as e:
            self.logger.error(f"Error creating trade execution: {e}", exc_info=True)
            raise

    def get_by_id(self, execution_id: Union[str, UUID]) -> Optional[TradeExecution]:
        """Get a trade execution by ID"""
        try:
            if isinstance(execution_id, str):
                execution_id = UUID(execution_id)

            with self.engine.begin() as conn:
                result = conn.execute(self.table.select().where(self.table.c.id == execution_id))
                row = result.first()
                if not row:
                    return None

                # Get string values for FK fields
                strategy_name = self.get_strategy_name(row.strategy_id, conn)
                pair_symbol = self.get_trading_pair_symbol(row.trading_pair_id, conn)
                buy_exchange_name = self.get_exchange_name(row.buy_exchange_id, conn)
                sell_exchange_name = self.get_exchange_name(row.sell_exchange_id, conn)

                return TradeExecution(
                    id=str(row.id),
                    strategy=strategy_name,
                    pair=pair_symbol,
                    buy_exchange=buy_exchange_name,
                    sell_exchange=sell_exchange_name,
                    executed_buy_price=float(row.executed_buy_price),
                    executed_sell_price=float(row.executed_sell_price),
                    spread=float(row.spread),
                    volume=float(row.volume),
                    execution_timestamp=row.execution_timestamp.timestamp() if row.execution_timestamp else None,
                    execution_id=row.execution_id,
                    opportunity_id=str(row.opportunity_id) if row.opportunity_id else None,
                )
        except Exception as e:
            self.logger.error(f"Error getting trade execution by ID {execution_id}: {e}")
            raise

    def get_strategy_name(self, strategy_id: int, conn=None) -> str:
        """
        Get strategy name from ID

        Raises:
            ValueError: If strategy with the given ID doesn't exist
        """
        try:
            close_conn = False
            if conn is None:
                conn = self.engine.connect()
                close_conn = True

            query = sa.select(self.strategies_table.c.name).where(self.strategies_table.c.id == strategy_id)
            result = conn.execute(query).first()

            if close_conn:
                conn.close()

            if result is None:
                raise ValueError(f"Strategy with ID {strategy_id} not found")

            return result[0]
        except Exception as e:
            self.logger.error(f"Error getting strategy name for ID {strategy_id}: {e}")
            raise

    def get_trading_pair_symbol(self, trading_pair_id: int, conn=None) -> str:
        """
        Get trading pair symbol from ID

        Raises:
            ValueError: If trading pair with the given ID doesn't exist
        """
        try:
            close_conn = False
            if conn is None:
                conn = self.engine.connect()
                close_conn = True

            query = sa.select(self.trading_pairs_table.c.symbol).where(self.trading_pairs_table.c.id == trading_pair_id)
            result = conn.execute(query).first()

            if close_conn:
                conn.close()

            if result is None:
                raise ValueError(f"Trading pair with ID {trading_pair_id} not found")

            return result[0]
        except Exception as e:
            self.logger.error(f"Error getting trading pair symbol for ID {trading_pair_id}: {e}")
            raise

    def get_exchange_name(self, exchange_id: int, conn=None) -> str:
        """
        Get exchange name from ID

        Raises:
            ValueError: If exchange with the given ID doesn't exist
        """
        try:
            close_conn = False
            if conn is None:
                conn = self.engine.connect()
                close_conn = True

            query = sa.select(self.exchanges_table.c.name).where(self.exchanges_table.c.id == exchange_id)
            result = conn.execute(query).first()

            if close_conn:
                conn.close()

            if result is None:
                raise ValueError(f"Exchange with ID {exchange_id} not found")

            return result[0]
        except Exception as e:
            self.logger.error(f"Error getting exchange name for ID {exchange_id}: {e}")
            raise

    def get_by_strategy_id(self, strategy_id: int) -> List[TradeExecution]:
        """Get all trade executions for a specific strategy by strategy ID"""
        try:
            with self.engine.begin() as conn:
                # Use strategy_id to match the schema - expecting an integer
                result = conn.execute(self.table.select().where(self.table.c.strategy_id == strategy_id))
                executions = []

                for row in result:
                    # Get string values for FK fields
                    strategy_name = self.get_strategy_name(row.strategy_id, conn)
                    pair_symbol = self.get_trading_pair_symbol(row.trading_pair_id, conn)
                    buy_exchange_name = self.get_exchange_name(row.buy_exchange_id, conn)
                    sell_exchange_name = self.get_exchange_name(row.sell_exchange_id, conn)

                    # Create a TradeExecution instance with correct field mappings
                    execution = TradeExecution(
                        id=str(row.id),
                        strategy=strategy_name,  # Use string name instead of ID
                        pair=pair_symbol,  # Use string symbol instead of ID
                        buy_exchange=buy_exchange_name,  # Use string name instead of ID
                        sell_exchange=sell_exchange_name,  # Use string name instead of ID
                        executed_buy_price=float(row.executed_buy_price),
                        executed_sell_price=float(row.executed_sell_price),
                        spread=float(row.spread),
                        volume=float(row.volume),
                        execution_timestamp=row.execution_timestamp.timestamp() if row.execution_timestamp else None,
                        execution_id=row.execution_id,
                        opportunity_id=str(row.opportunity_id) if row.opportunity_id else None,
                    )
                    executions.append(execution)
                return executions
        except Exception as e:
            self.logger.error(f"Error getting trade executions by strategy ID {strategy_id}: {e}")
            raise

    def get_by_strategy(self, strategy_name: str) -> List[TradeExecution]:
        """Get all trade executions for a specific strategy by name"""
        try:
            # First get the strategy ID - must be an integer for the database query
            strategy_id = self.get_strategy_id_by_name(strategy_name)

            # Then get executions by ID
            return self.get_by_strategy_id(strategy_id)
        except ValueError as e:
            # Explicitly catch ValueError which might be raised if strategy doesn't exist
            self.logger.warning(f"Strategy with name '{strategy_name}' not found: {str(e)}")
            return []
        except Exception as e:
            self.logger.error(f"Error getting executions for strategy '{strategy_name}': {e}")
            return []

    def get_recent(self, count: int = 10, strategy_id: Optional[int] = None) -> List[TradeExecution]:
        """Get most recent trade executions"""
        try:
            query = self.table.select().order_by(self.table.c.execution_timestamp.desc()).limit(count)

            # Add strategy filter if provided - use strategy_id as an integer
            if strategy_id is not None:
                query = query.where(self.table.c.strategy_id == strategy_id)

            with self.engine.begin() as conn:
                result = conn.execute(query)
                executions = []

                for row in result:
                    # Get human-readable names for the IDs
                    strategy_name = self.get_strategy_name(row.strategy_id, conn)
                    pair_symbol = self.get_trading_pair_symbol(row.trading_pair_id, conn)
                    buy_exchange_name = self.get_exchange_name(row.buy_exchange_id, conn)
                    sell_exchange_name = self.get_exchange_name(row.sell_exchange_id, conn)

                    # Create a proper TradeExecution model instance instead of a dictionary
                    execution = TradeExecution(
                        id=str(row.id),
                        strategy=strategy_name,
                        pair=pair_symbol,
                        buy_exchange=buy_exchange_name,
                        sell_exchange=sell_exchange_name,
                        executed_buy_price=float(row.executed_buy_price),
                        executed_sell_price=float(row.executed_sell_price),
                        spread=float(row.spread),
                        volume=float(row.volume),
                        execution_timestamp=row.execution_timestamp.timestamp() if row.execution_timestamp else None,
                        execution_id=row.execution_id,
                        opportunity_id=str(row.opportunity_id) if row.opportunity_id else None,
                    )
                    executions.append(execution)

                return executions
        except Exception as e:
            self.logger.error(f"Error fetching recent executions: {e}")
            return []
