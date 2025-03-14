from datetime import datetime
from typing import List, Optional, Union
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.sql import select

from src.arbirich.config import DATABASE_URL
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
        with self.engine.begin() as conn:
            result = conn.execute(
                pairs.insert()
                .values(
                    base_currency=pair.base_currency,
                    quote_currency=pair.quote_currency,
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

    def update_strategy_stats(self, strategy_name: str, profit: float, loss: float, trade_count: int = 1) -> None:
        """Update a strategy's performance metrics"""
        with self.engine.begin() as conn:
            # Get current values
            query = select([strategies.c.total_profit, strategies.c.total_loss, strategies.c.trades_count]).where(
                strategies.c.name == strategy_name
            )
            result = conn.execute(query)
            row = result.first()

            if row:
                current_profit = float(row.total_profit)
                current_loss = float(row.total_loss)
                current_trades = row.trades_count

                # Calculate new values
                new_profit = current_profit + profit if profit > 0 else current_profit
                new_loss = current_loss + abs(loss) if loss < 0 else current_loss
                new_trades = current_trades + trade_count
                net_profit = new_profit - new_loss

                # Update strategy
                conn.execute(
                    strategies.update()
                    .where(strategies.c.name == strategy_name)
                    .values(
                        total_profit=new_profit,
                        total_loss=new_loss,
                        net_profit=net_profit,
                        trades_count=new_trades,
                        last_updated=datetime.utcnow(),
                    )
                )

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
        with self.engine.begin() as conn:
            result = conn.execute(
                trade_executions.insert().values(**execution.to_db_dict()).returning(*trade_executions.c)
            )
            row = result.first()
            return TradeExecution.model_validate(
                {
                    **row._asdict(),
                    "execution_timestamp": row.execution_timestamp.timestamp(),
                    "id": str(row.id),
                    "opportunity_id": str(row.opportunity_id) if row.opportunity_id else None,
                }
            )

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
