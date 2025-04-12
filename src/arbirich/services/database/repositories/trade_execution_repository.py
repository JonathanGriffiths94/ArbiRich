from typing import Any, Dict, List, Optional, Union
from uuid import UUID

from src.arbirich.models.models import TradeExecution
from src.arbirich.models.schema import trade_executions
from src.arbirich.services.database.base_repository import BaseRepository


class TradeExecutionRepository(BaseRepository[TradeExecution]):
    """Repository for Trade Execution entity operations"""

    def __init__(self, *args, **kwargs):
        super().__init__(model_class=TradeExecution, *args, **kwargs)
        self.table = trade_executions

    def create(self, execution: TradeExecution) -> TradeExecution:
        """Create a new trade execution"""
        try:
            # Convert to match database column names
            db_data = {
                "id": execution.id,
                "strategy_id": execution.strategy,  # Map strategy -> strategy_id
                "trading_pair_id": execution.pair,  # Map pair -> trading_pair_id
                "buy_exchange_id": execution.buy_exchange,  # Map buy_exchange -> buy_exchange_id
                "sell_exchange_id": execution.sell_exchange,  # Map sell_exchange -> sell_exchange_id
                "executed_buy_price": execution.executed_buy_price,
                "executed_sell_price": execution.executed_sell_price,
                "spread": execution.spread,
                "volume": execution.volume,
                "execution_timestamp": execution.execution_timestamp,
                "execution_id": execution.execution_id,
                "opportunity_id": execution.opportunity_id,
            }

            with self.engine.begin() as conn:
                result = conn.execute(self.table.insert().values(**db_data).returning(*self.table.c))
                row = result.first()

                # Map database column names back to model field names
                return TradeExecution(
                    id=str(row.id),
                    strategy=row.strategy_id,  # Map strategy_id -> strategy
                    pair=row.trading_pair_id,  # Map trading_pair_id -> pair
                    buy_exchange=row.buy_exchange_id,  # Map buy_exchange_id -> buy_exchange
                    sell_exchange=row.sell_exchange_id,  # Map sell_exchange_id -> sell_exchange
                    executed_buy_price=float(row.executed_buy_price),
                    executed_sell_price=float(row.executed_sell_price),
                    spread=float(row.spread),
                    volume=float(row.volume),
                    execution_timestamp=row.execution_timestamp.timestamp(),
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

                return TradeExecution.model_validate(
                    {
                        **row._asdict(),
                        "execution_timestamp": row.execution_timestamp.timestamp(),
                        "id": str(row.id),
                        "opportunity_id": str(row.opportunity_id) if row.opportunity_id else None,
                    }
                )
        except Exception as e:
            self.logger.error(f"Error getting trade execution by ID {execution_id}: {e}")
            raise

    def get_by_strategy_id(self, strategy_id: int) -> List[TradeExecution]:
        """Get all trade executions for a specific strategy by strategy ID"""
        try:
            with self.engine.begin() as conn:
                # Use strategy_id to match the schema - expecting an integer
                result = conn.execute(self.table.select().where(self.table.c.strategy_id == strategy_id))
                executions = []
                for row in result:
                    # Create a TradeExecution instance with correct field mappings
                    execution = TradeExecution(
                        id=str(row.id),
                        strategy=row.strategy_id,  # Map strategy_id -> strategy
                        pair=row.trading_pair_id,  # Map trading_pair_id -> pair
                        buy_exchange=row.buy_exchange_id,  # Map buy_exchange_id -> buy_exchange
                        sell_exchange=row.sell_exchange_id,  # Map sell_exchange_id -> sell_exchange
                        executed_buy_price=float(row.executed_buy_price),
                        executed_sell_price=float(row.executed_sell_price),
                        spread=float(row.spread),
                        volume=float(row.volume),
                        execution_timestamp=row.execution_timestamp.timestamp(),
                        execution_id=row.execution_id,
                        opportunity_id=str(row.opportunity_id) if row.opportunity_id else None,
                    )
                    executions.append(execution)
                return executions
        except Exception as e:
            self.logger.error(f"Error getting trade executions by strategy ID {strategy_id}: {e}")
            raise

    def get_by_strategy(self, strategy_name: str) -> List[TradeExecution]:
        """Get all trade executions for a specific strategy - now deprecated, use get_by_strategy_id"""
        self.logger.warning("get_by_strategy is deprecated - should use get_by_strategy_id instead")

        # This will work only if strategy_name is already an integer ID
        try:
            strategy_id = int(strategy_name)
            return self.get_by_strategy_id(strategy_id)
        except (ValueError, TypeError):
            self.logger.error(f"Cannot convert strategy name '{strategy_name}' to integer ID")
            return []

    def get_recent(self, count: int = 10, strategy_id: Optional[int] = None) -> List[Dict[str, Any]]:
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
                    execution = {
                        **row._asdict(),
                        "execution_timestamp": row.execution_timestamp.timestamp(),
                        "id": str(row.id),
                        "opportunity_id": str(row.opportunity_id) if row.opportunity_id else None,
                    }
                    executions.append(execution)

                return executions
        except Exception as e:
            self.logger.error(f"Error fetching recent executions: {e}")
            return []
