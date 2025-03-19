import logging
from datetime import datetime
from typing import List, Optional, Union
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.sql import select

from src.arbirich.config.config import DATABASE_URL
from src.arbirich.models.models import (
    Exchange,
    Pair,
    Strategy,
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

    def __enter__(self):
        self.connection = self.engine.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()
            self.connection = None

    # ---- Exchange operations ----
    def create_exchange(self, exchange: Exchange) -> Exchange:
        with self.engine.begin() as conn:
            result = conn.execute(
                exchanges.insert()
                .values(
                    name=exchange.name,
                    api_rate_limit=exchange.api_rate_limit,
                    trade_fees=exchange.trade_fees,
                    additional_info=exchange.additional_info,
                )
                .returning(*exchanges.c)
            )
            row = result.first()
            return Exchange.model_validate(row._asdict())

    def get_exchange(self, exchange_id: int) -> Optional[Exchange]:
        with self.engine.begin() as conn:
            result = conn.execute(exchanges.select().where(exchanges.c.id == exchange_id))
            row = result.first()
            return Exchange.model_validate(row._asdict()) if row else None

    def get_all_exchanges(self) -> List[Exchange]:
        with self.engine.begin() as conn:
            result = conn.execute(exchanges.select())
            return [Exchange.model_validate(row._asdict()) for row in result]

    def update_exchange(self, exchange_id: int, **kwargs) -> Optional[Exchange]:
        with self.engine.begin() as conn:
            conn.execute(exchanges.update().where(exchanges.c.id == exchange_id).values(**kwargs))
            result = conn.execute(exchanges.select().where(exchanges.c.id == exchange_id))
            row = result.first()
            return Exchange.model_validate(row._asdict()) if row else None

    def delete_exchange(self, exchange_id: int) -> bool:
        with self.engine.begin() as conn:
            result = conn.execute(exchanges.delete().where(exchanges.c.id == exchange_id))
            return result.rowcount > 0

    # ---- Pair operations ----
    def create_pair(self, pair: Pair) -> Pair:
        """Create a pair with proper symbol handling."""
        with self.engine.begin() as conn:
            # Ensure symbol is set if not provided
            if not pair.symbol:
                pair.symbol = f"{pair.base_currency}-{pair.quote_currency}"

            # Debug log to verify symbol is being set
            print(f"Creating pair with symbol: {pair.symbol}")

            # Important: Include symbol in the values dictionary
            result = conn.execute(
                pairs.insert()
                .values(
                    base_currency=pair.base_currency,
                    quote_currency=pair.quote_currency,
                    symbol=pair.symbol,  # Now included in the SQL query
                )
                .returning(*pairs.c)
            )
            row = result.first()
            return Pair.model_validate(row._asdict())

    def get_pair(self, pair_id: int) -> Optional[Pair]:
        with self.engine.begin() as conn:
            result = conn.execute(pairs.select().where(pairs.c.id == pair_id))
            row = result.first()
            return Pair.model_validate(row._asdict()) if row else None

    def get_all_pairs(self) -> List[Pair]:
        with self.engine.begin() as conn:
            result = conn.execute(pairs.select())
            return [Pair.model_validate(row._asdict()) for row in result]

    # ---- Strategy operations ----
    def create_strategy(self, strategy: Strategy) -> Strategy:
        with self.engine.begin() as conn:
            result = conn.execute(
                strategies.insert()
                .values(
                    name=strategy.name,
                    starting_capital=strategy.starting_capital,
                    min_spread=strategy.min_spread,
                    additional_info=strategy.additional_info,
                )
                .returning(*strategies.c)
            )
            row = result.first()
            return Strategy.model_validate(row._asdict())

    def get_strategy(self, strategy_id: int) -> Optional[Strategy]:
        with self.engine.begin() as conn:
            result = conn.execute(strategies.select().where(strategies.c.id == strategy_id))
            row = result.first()
            return Strategy.model_validate(row._asdict()) if row else None

    def get_all_strategies(self) -> List[Strategy]:
        with self.engine.begin() as conn:
            result = conn.execute(strategies.select())
            return [Strategy.model_validate(row._asdict()) for row in result]

    def update_strategy_stats(self, strategy_name: str, profit: float = 0, loss: float = 0, trade_count: int = 0):
        """Update strategy statistics after a trade execution"""
        try:
            from decimal import Decimal

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
