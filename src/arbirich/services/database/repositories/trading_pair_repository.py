import json
from typing import List, Optional, Tuple

import sqlalchemy as sa

from arbirich.models.db.schema import trading_pairs
from src.arbirich.models.models import TradingPair
from src.arbirich.services.database.base_repository import BaseRepository


class TradingPairRepository(BaseRepository[TradingPair]):
    """Repository for Trading Pair entity operations"""

    def __init__(self, *args, **kwargs):
        super().__init__(model_class=TradingPair, *args, **kwargs)
        self.table = trading_pairs

    def create(self, pair: TradingPair) -> TradingPair:
        """Create a trading pair with proper symbol handling"""
        try:
            # Ensure symbol is set if not provided
            if not pair.symbol:
                pair.symbol = f"{pair.base_currency}-{pair.quote_currency}"

            # Debug log to verify symbol is being set
            self.logger.debug(f"Creating pair with symbol: {pair.symbol}")

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
                # Include symbol in the values dictionary
                result = conn.execute(self.table.insert().values(**pair_data).returning(*self.table.c))
                row = result.first()
                return TradingPair.model_validate(row._asdict())
        except Exception as e:
            self.logger.error(f"Error creating pair: {e}")
            raise

    def get_by_id(self, pair_id: int) -> Optional[TradingPair]:
        """Get trading pair by ID"""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(self.table.select().where(self.table.c.id == pair_id))
                row = result.first()
                return TradingPair.model_validate(row._asdict()) if row else None
        except Exception as e:
            self.logger.error(f"Error getting pair by ID {pair_id}: {e}")
            raise

    def get_by_symbol(self, symbol: str) -> Optional[TradingPair]:
        """Get a trading pair by its symbol"""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(self.table.select().where(self.table.c.symbol == symbol))
                row = result.first()
                return TradingPair.model_validate(row._asdict()) if row else None
        except Exception as e:
            self.logger.error(f"Error getting pair by symbol {symbol}: {e}")
            raise

    def get_all(self) -> List[TradingPair]:
        """Get all trading pairs"""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(self.table.select())
                return [TradingPair.model_validate(row._asdict()) for row in result]
        except Exception as e:
            self.logger.error(f"Error getting all pairs: {e}")
            raise

    def get_active(self) -> List[TradingPair]:
        """Get all active trading pairs"""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(self.table.select().where(self.table.c.is_active))
                return [TradingPair.model_validate(row._asdict()) for row in result]
        except Exception as e:
            self.logger.error(f"Error getting active pairs: {e}")
            raise

    def update(self, pair: TradingPair) -> TradingPair:
        """Update an existing trading pair"""
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
                conn.execute(self.table.update().where(self.table.c.id == pair_id).values(**pair_data))
                result = conn.execute(self.table.select().where(self.table.c.id == pair_id))
                row = result.first()
                return TradingPair.model_validate(row._asdict())
        except Exception as e:
            self.logger.error(f"Error updating pair {pair.symbol}: {e}")
            raise

    def set_active_status(self, pair_id: int, is_active: bool) -> bool:
        """Set trading pair active status"""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(self.table.update().where(self.table.c.id == pair_id).values(is_active=is_active))
                return result.rowcount > 0
        except Exception as e:
            self.logger.error(f"Error setting pair {pair_id} active status: {e}")
            return False

    def set_active_status_by_pair(self, pair: Tuple[str, str], active: bool) -> bool:
        """Set the active status of a trading pair by tuple of (base, quote) currencies"""
        try:
            symbol = "-".join(pair)  # Construct the symbol dynamically
            with self.engine.begin() as conn:
                result = conn.execute(
                    sa.text("UPDATE trading_pairs SET is_active = :active WHERE symbol = :symbol"),
                    {"active": active, "symbol": symbol},
                )
                return result.rowcount > 0
        except Exception as e:
            self.logger.error(f"Error setting pair '{pair}' active status to {active}: {e}")
            return False

    def set_active_status_by_symbol(self, symbol: str, is_active: bool) -> bool:
        """Set the active status of a trading pair by symbol."""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(
                    self.table.update()
                    .where(self.table.c.symbol == symbol)
                    .values(is_active=is_active)
                    .returning(self.table.c.id)
                )
                updated_id = result.scalar()
                return updated_id is not None
        except Exception as e:
            self.logger.error(f"Error updating trading pair status for '{symbol}': {e}")
            return False

    def is_in_use(self, pair: Tuple[str, str]) -> bool:
        """Check if a trading pair is in use by any active strategy"""
        try:
            symbol = "-".join(pair)  # Construct the symbol dynamically

            # Import here to avoid circular imports
            from src.arbirich.services.database.repositories.strategy_repository import StrategyRepository

            # Get all active strategies and check their additional_info field for the pair
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
                    # Check if 'pairs' exists in additional_info and contains the target pair
                    if "pairs" in additional_info and isinstance(additional_info["pairs"], list):
                        if symbol in additional_info["pairs"]:
                            self.logger.debug(f"Pair {symbol} is used by strategy {strategy.name}")
                            return True

            # No active strategy is using this pair
            return False
        except Exception as e:
            self.logger.error(f"Error checking if pair '{pair}' is in use: {e}")
            # Default to False to allow deactivation
            return False
