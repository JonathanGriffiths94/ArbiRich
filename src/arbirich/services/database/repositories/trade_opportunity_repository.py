import logging
from typing import List, Optional, Union
from uuid import UUID

import sqlalchemy as sa

from src.arbirich.models.models import TradeOpportunity
from src.arbirich.models.schema import exchanges, strategies, trade_opportunities, trading_pairs
from src.arbirich.services.database.base_repository import BaseRepository
from src.arbirich.services.database.utils.timestamp_converter import convert_unix_timestamp_for_db


class TradeOpportunityRepository(BaseRepository[TradeOpportunity]):
    """Repository for Trade Opportunity entity operations"""

    def __init__(self, *args, **kwargs):
        super().__init__(model_class=TradeOpportunity, *args, **kwargs)
        self.table = trade_opportunities
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

    def create(self, opportunity: TradeOpportunity) -> TradeOpportunity:
        """
        Create a new trade opportunity

        Raises:
            ValueError: If any referenced entity (strategy, trading pair, exchange) doesn't exist
        """
        try:
            if opportunity.strategy:
                if isinstance(opportunity.strategy, str):
                    strategy_id = self.get_strategy_id_by_name(opportunity.strategy)
                elif isinstance(opportunity.strategy, int):
                    strategy_id = opportunity.strategy
                else:
                    raise TypeError(f"Unexpected type for strategy: {type(opportunity.strategy)}")
            else:
                raise ValueError("Strategy is required")

            if isinstance(opportunity.pair, str):
                trading_pair_id = self.get_trading_pair_id(opportunity.pair)
            else:
                raise TypeError(f"Unexpected type for pair: {type(opportunity.pair)}")

            if isinstance(opportunity.buy_exchange, str):
                buy_exchange_id = self.get_exchange_id(opportunity.buy_exchange)
            else:
                raise TypeError(f"Unexpected type for buy_exchange: {type(opportunity.buy_exchange)}")

            if isinstance(opportunity.sell_exchange, str):
                sell_exchange_id = self.get_exchange_id(opportunity.sell_exchange)
            else:
                raise TypeError(f"Unexpected type for sell_exchange: {type(opportunity.sell_exchange)}")

            db_data = {
                "id": opportunity.id,
                "strategy_id": strategy_id,
                "trading_pair_id": trading_pair_id,
                "buy_exchange_id": buy_exchange_id,
                "sell_exchange_id": sell_exchange_id,
                "buy_price": opportunity.buy_price,
                "sell_price": opportunity.sell_price,
                "spread": opportunity.spread,
                "volume": opportunity.volume,
                "opportunity_timestamp": opportunity.opportunity_timestamp,
            }

            if "opportunity_timestamp" in db_data:
                db_data["opportunity_timestamp"] = convert_unix_timestamp_for_db(db_data["opportunity_timestamp"])

            self.logger.debug(f"Inserting opportunity with data: {db_data}")

            with self.engine.begin() as conn:
                result = conn.execute(self.table.insert().values(**db_data).returning(*self.table.c))
                row = result.first()

                original_strategy = opportunity.strategy
                original_pair = opportunity.pair
                original_buy_exchange = opportunity.buy_exchange
                original_sell_exchange = opportunity.sell_exchange

                return TradeOpportunity(
                    id=str(row.id),
                    strategy=original_strategy,
                    pair=original_pair,
                    buy_exchange=original_buy_exchange,
                    sell_exchange=original_sell_exchange,
                    buy_price=float(row.buy_price),
                    sell_price=float(row.sell_price),
                    spread=float(row.spread),
                    volume=float(row.volume),
                    opportunity_timestamp=row.opportunity_timestamp.timestamp() if row.opportunity_timestamp else None,
                )
        except Exception as e:
            self.logger.error(f"Error creating trade opportunity: {e}", exc_info=True)
            raise

    def get_by_id(self, opportunity_id: Union[str, UUID]) -> Optional[TradeOpportunity]:
        """Get a trade opportunity by ID"""
        try:
            if isinstance(opportunity_id, str):
                opportunity_id = UUID(opportunity_id)

            with self.engine.begin() as conn:
                result = conn.execute(self.table.select().where(self.table.c.id == opportunity_id))
                row = result.first()
                if not row:
                    return None

                strategy_name = self.get_strategy_name(row.strategy_id, conn)
                pair_symbol = self.get_trading_pair_symbol(row.trading_pair_id, conn)
                buy_exchange_name = self.get_exchange_name(row.buy_exchange_id, conn)
                sell_exchange_name = self.get_exchange_name(row.sell_exchange_id, conn)

                return TradeOpportunity(
                    id=str(row.id),
                    strategy=strategy_name,
                    pair=pair_symbol,
                    buy_exchange=buy_exchange_name,
                    sell_exchange=sell_exchange_name,
                    buy_price=float(row.buy_price),
                    sell_price=float(row.sell_price),
                    spread=float(row.spread),
                    volume=float(row.volume),
                    opportunity_timestamp=row.opportunity_timestamp.timestamp() if row.opportunity_timestamp else None,
                )
        except Exception as e:
            self.logger.error(f"Error getting trade opportunity by ID {opportunity_id}: {e}")
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

    def get_recent(self, count: int = 10, strategy: Optional[Union[int, str]] = None) -> List[TradeOpportunity]:
        """
        Get most recent trade opportunities

        Args:
            count: Maximum number of opportunities to return
            strategy: Either a strategy ID (int) or strategy name (str)

        Returns:
            List of trade opportunities
        """
        try:
            query = self.table.select().order_by(self.table.c.opportunity_timestamp.desc()).limit(count)

            if strategy is not None:
                strategy_id = strategy
                # Convert strategy name to ID if a string is provided
                if isinstance(strategy, str):
                    strategy_id = self.get_strategy_id_by_name(strategy)

                query = query.where(self.table.c.strategy_id == strategy_id)

            with self.engine.begin() as conn:
                result = conn.execute(query)
                opportunities = []

                for row in result:
                    # Get human-readable names for the IDs
                    strategy_name = self.get_strategy_name(row.strategy_id, conn)
                    pair_symbol = self.get_trading_pair_symbol(row.trading_pair_id, conn)
                    buy_exchange_name = self.get_exchange_name(row.buy_exchange_id, conn)
                    sell_exchange_name = self.get_exchange_name(row.sell_exchange_id, conn)

                    # Create a proper TradeOpportunity model instance
                    opportunity = TradeOpportunity(
                        id=str(row.id),
                        strategy=strategy_name,
                        pair=pair_symbol,
                        buy_exchange=buy_exchange_name,
                        sell_exchange=sell_exchange_name,
                        buy_price=float(row.buy_price),
                        sell_price=float(row.sell_price),
                        spread=float(row.spread),
                        volume=float(row.volume),
                        opportunity_timestamp=row.opportunity_timestamp.timestamp()
                        if row.opportunity_timestamp
                        else None,
                    )
                    opportunities.append(opportunity)

                return opportunities
        except Exception as e:
            self.logger.error(f"Error fetching recent opportunities: {e}")
            return []

    def get_by_strategy_name(self, strategy_name: str) -> List[TradeOpportunity]:
        """Get opportunities by strategy name"""
        try:
            # First get the strategy ID - must be an integer for the database query
            strategy_id = self.get_strategy_id_by_name(strategy_name)

            # Now use the integer ID to query the database
            with self.engine.begin() as conn:
                # Use strategy_id (integer) instead of strategy_name (string) in the WHERE clause
                query = self.table.select().where(self.table.c.strategy_id == strategy_id)
                result = conn.execute(query)
                opportunities = []

                for row in result:
                    pair_symbol = self.get_trading_pair_symbol(row.trading_pair_id, conn)
                    buy_exchange_name = self.get_exchange_name(row.buy_exchange_id, conn)
                    sell_exchange_name = self.get_exchange_name(row.sell_exchange_id, conn)

                    opportunity = TradeOpportunity(
                        id=str(row.id),
                        strategy=strategy_name,  # Use the original string name in the returned model
                        pair=pair_symbol,
                        buy_exchange=buy_exchange_name,
                        sell_exchange=sell_exchange_name,
                        buy_price=float(row.buy_price),
                        sell_price=float(row.sell_price),
                        spread=float(row.spread),
                        volume=float(row.volume),
                        opportunity_timestamp=row.opportunity_timestamp.timestamp()
                        if row.opportunity_timestamp
                        else None,
                    )
                    opportunities.append(opportunity)

                return opportunities
        except Exception as e:
            self.logger.error(f"Error getting opportunities for strategy '{strategy_name}': {e}")
            return []
