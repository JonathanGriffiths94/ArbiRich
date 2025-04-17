import json
from typing import List, Optional

from arbirich.models.db.schema import exchanges
from src.arbirich.models.models import Exchange
from src.arbirich.services.database.base_repository import BaseRepository


class ExchangeRepository(BaseRepository[Exchange]):
    """Repository for Exchange entity operations"""

    def __init__(self, *args, **kwargs):
        super().__init__(model_class=Exchange, *args, **kwargs)
        self.table = exchanges

    def create(self, exchange: Exchange) -> Exchange:
        """Create a new exchange"""
        try:
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

            with self.engine.begin() as conn:
                result = conn.execute(self.table.insert().values(**exchange_data).returning(*self.table.c))
                row = result.first()
                return Exchange.model_validate(row._asdict())
        except Exception as e:
            self.logger.error(f"Error creating exchange: {e}")
            raise

    def get_by_id(self, exchange_id: int) -> Optional[Exchange]:
        """Get exchange by ID"""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(self.table.select().where(self.table.c.id == exchange_id))
                row = result.first()
                return Exchange.model_validate(row._asdict()) if row else None
        except Exception as e:
            self.logger.error(f"Error getting exchange by ID {exchange_id}: {e}")
            raise

    def get_by_name(self, name: str) -> Optional[Exchange]:
        """Get exchange by name"""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(self.table.select().where(self.table.c.name == name))
                row = result.first()
                return Exchange.model_validate(row._asdict()) if row else None
        except Exception as e:
            self.logger.error(f"Error getting exchange by name {name}: {e}")
            raise

    def get_all(self) -> List[Exchange]:
        """Get all exchanges"""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(self.table.select())
                return [Exchange.model_validate(row._asdict()) for row in result]
        except Exception as e:
            self.logger.error(f"Error getting all exchanges: {e}")
            raise

    def get_active(self) -> List[Exchange]:
        """Get all active exchanges"""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(self.table.select().where(self.table.c.is_active))
                return [Exchange.model_validate(row._asdict()) for row in result]
        except Exception as e:
            self.logger.error(f"Error getting active exchanges: {e}")
            raise

    def update(self, exchange: Exchange) -> Exchange:
        """Update an existing exchange"""
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
                conn.execute(self.table.update().where(self.table.c.id == exchange_id).values(**exchange_data))
                result = conn.execute(self.table.select().where(self.table.c.id == exchange_id))
                row = result.first()
                return Exchange.model_validate(row._asdict())
        except Exception as e:
            self.logger.error(f"Error updating exchange {exchange.name}: {e}")
            raise

    def set_active_status(self, exchange_id: int, is_active: bool) -> bool:
        """Set exchange active status"""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(
                    self.table.update().where(self.table.c.id == exchange_id).values(is_active=is_active)
                )
                return result.rowcount > 0
        except Exception as e:
            self.logger.error(f"Error setting exchange {exchange_id} active status: {e}")
            return False

    def set_active_status_by_name(self, name: str, is_active: bool) -> bool:
        """Set exchange active status by name."""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(
                    self.table.update()
                    .where(self.table.c.name == name)
                    .values(is_active=is_active)
                    .returning(self.table.c.id)
                )
                updated_id = result.scalar()
                return updated_id is not None
        except Exception as e:
            self.logger.error(f"Error updating exchange status for '{name}': {e}")
            return False

    def delete(self, exchange_id: int) -> bool:
        """Delete an exchange"""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(self.table.delete().where(self.table.c.id == exchange_id))
                return result.rowcount > 0
        except Exception as e:
            self.logger.error(f"Error deleting exchange {exchange_id}: {e}")
            return False

    def is_in_use(self, exchange: str) -> bool:
        """Check if an exchange is in use by any active strategy."""
        try:
            # Import here to avoid circular imports
            from src.arbirich.services.database.repositories.strategy_repository import StrategyRepository

            # Get all active strategies and check their additional_info field for the exchange
            strategy_repo = StrategyRepository()
            strategies = strategy_repo.get_active()

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
            self.logger.error(f"Error checking if exchange '{exchange}' is in use: {e}")
            # Default to False to allow deactivation
            return False
