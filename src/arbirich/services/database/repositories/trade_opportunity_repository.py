from typing import Any, Dict, List, Optional, Union
from uuid import UUID

from src.arbirich.models.models import TradeOpportunity
from src.arbirich.models.schema import trade_opportunities
from src.arbirich.services.database.base_repository import BaseRepository


class TradeOpportunityRepository(BaseRepository[TradeOpportunity]):
    """Repository for Trade Opportunity entity operations"""

    def __init__(self, *args, **kwargs):
        super().__init__(model_class=TradeOpportunity, *args, **kwargs)
        self.table = trade_opportunities

    def create(self, opportunity: TradeOpportunity) -> TradeOpportunity:
        """Create a new trade opportunity"""
        try:
            # Convert the model fields to match database column names
            db_data = {
                "id": opportunity.id,
                "strategy_id": opportunity.strategy,  # Map strategy -> strategy_id
                "trading_pair_id": opportunity.pair,  # Map pair -> trading_pair_id
                "buy_exchange_id": opportunity.buy_exchange,  # Map buy_exchange -> buy_exchange_id
                "sell_exchange_id": opportunity.sell_exchange,  # Map sell_exchange -> sell_exchange_id
                "buy_price": opportunity.buy_price,
                "sell_price": opportunity.sell_price,
                "spread": opportunity.spread,
                "volume": opportunity.volume,
                "opportunity_timestamp": opportunity.opportunity_timestamp,
            }

            with self.engine.begin() as conn:
                result = conn.execute(self.table.insert().values(**db_data).returning(*self.table.c))
                row = result.first()

                # Map database column names back to model field names
                return TradeOpportunity(
                    id=str(row.id),
                    strategy=row.strategy_id,  # Map strategy_id -> strategy
                    pair=row.trading_pair_id,  # Map trading_pair_id -> pair
                    buy_exchange=row.buy_exchange_id,  # Map buy_exchange_id -> buy_exchange
                    sell_exchange=row.sell_exchange_id,  # Map sell_exchange_id -> sell_exchange
                    buy_price=float(row.buy_price),
                    sell_price=float(row.sell_price),
                    spread=float(row.spread),
                    volume=float(row.volume),
                    opportunity_timestamp=row.opportunity_timestamp.timestamp(),
                )
        except Exception as e:
            self.logger.error(f"Error creating trade opportunity: {e}")
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

                return TradeOpportunity(
                    id=str(row.id),
                    strategy=row.strategy_id,  # Map strategy_id -> strategy
                    pair=row.trading_pair_id,  # Map trading_pair_id -> pair
                    buy_exchange=row.buy_exchange_id,  # Map buy_exchange_id -> buy_exchange
                    sell_exchange=row.sell_exchange_id,  # Map sell_exchange_id -> sell_exchange
                    buy_price=float(row.buy_price),
                    sell_price=float(row.sell_price),
                    spread=float(row.spread),
                    volume=float(row.volume),
                    opportunity_timestamp=row.opportunity_timestamp.timestamp(),
                )
        except Exception as e:
            self.logger.error(f"Error getting trade opportunity by ID {opportunity_id}: {e}")
            raise

    def get_by_strategy(self, strategy_name: str) -> List[TradeOpportunity]:
        """Get all trade opportunities for a specific strategy"""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(self.table.select().where(self.table.c.strategy_id == strategy_name))
                opportunities = []
                for row in result:
                    opportunity = TradeOpportunity(
                        id=str(row.id),
                        strategy=row.strategy_id,  # Map strategy_id -> strategy
                        pair=row.trading_pair_id,  # Map trading_pair_id -> pair
                        buy_exchange=row.buy_exchange_id,  # Map buy_exchange_id -> buy_exchange
                        sell_exchange=row.sell_exchange_id,  # Map sell_exchange_id -> sell_exchange
                        buy_price=float(row.buy_price),
                        sell_price=float(row.sell_price),
                        spread=float(row.spread),
                        volume=float(row.volume),
                        opportunity_timestamp=row.opportunity_timestamp.timestamp(),
                    )
                    opportunities.append(opportunity)
                return opportunities
        except Exception as e:
            self.logger.error(f"Error getting trade opportunities by strategy {strategy_name}: {e}")
            raise

    def get_recent(self, count: int = 10, strategy_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get most recent trade opportunities"""
        try:
            query = self.table.select().order_by(self.table.c.opportunity_timestamp.desc()).limit(count)

            # Add strategy filter if provided
            if strategy_name:
                query = query.where(self.table.c.strategy_id == strategy_name)

            with self.engine.begin() as conn:
                result = conn.execute(query)
                opportunities = []

                for row in result:
                    opportunity = {
                        "id": str(row.id),
                        "strategy": row.strategy_id,  # Map strategy_id -> strategy
                        "pair": row.trading_pair_id,  # Map trading_pair_id -> pair
                        "buy_exchange": row.buy_exchange_id,  # Map buy_exchange_id -> buy_exchange
                        "sell_exchange": row.sell_exchange_id,  # Map sell_exchange_id -> sell_exchange
                        "buy_price": float(row.buy_price),
                        "sell_price": float(row.sell_price),
                        "spread": float(row.spread),
                        "volume": float(row.volume),
                        "opportunity_timestamp": row.opportunity_timestamp.timestamp(),
                    }
                    opportunities.append(opportunity)

                return opportunities
        except Exception as e:
            self.logger.error(f"Error fetching recent opportunities: {e}")
            return []
