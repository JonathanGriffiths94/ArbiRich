import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Type, TypeVar, Union

import sqlalchemy as sa
from pydantic import BaseModel
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.sql.schema import Table

from arbirich.models.db.schema import (
    exchanges,
    risk_profiles,
    strategies,
    strategy_exchange_pair_mappings,
    strategy_execution_mapping,
    strategy_parameters,
    strategy_type_parameters,
    strategy_types,
    trading_pairs,
)
from src.arbirich.models.models import (
    RiskProfile,
    Strategy,
    StrategyExchangePairMapping,
    StrategyExecutionMapping,
    StrategyParameters,
    StrategyType,
    StrategyTypeParameters,
)
from src.arbirich.services.database.base_repository import BaseRepository

T = TypeVar("T", bound=BaseModel)


class StrategyRepository(BaseRepository[Strategy]):
    """Repository for Strategy entity operations with full Pydantic model integration"""

    def __init__(self, engine: Engine, *args, **kwargs):
        super().__init__(model_class=Strategy, engine=engine, *args, **kwargs)
        self.table = strategies
        self.params_table = strategy_parameters
        self.type_params_table = strategy_type_parameters
        self.strategy_types_table = strategy_types
        self.risk_profiles_table = risk_profiles
        self.exec_mapping_table = strategy_execution_mapping
        self.exchange_pair_mapping_table = strategy_exchange_pair_mappings
        self.exchanges_table = exchanges
        self.trading_pairs_table = trading_pairs

    def create(self, strategy: Strategy) -> Strategy:
        """Create a new strategy with all its related entities"""
        try:
            with self.engine.begin() as conn:
                # Convert Strategy to dict for db insertion
                strategy_dict = self._prepare_strategy_dict(strategy)

                # Insert strategy and get ID
                strategy_result = conn.execute(self.table.insert().values(**strategy_dict).returning(*self.table.c))
                strategy_row = strategy_result.first()
                strategy_id = strategy_row.id

                # Create and insert parameters
                self._create_strategy_parameters(conn, strategy_id, strategy)

                # Create and insert type parameters if provided
                self._create_type_parameters(conn, strategy_id, strategy)

                # Create execution mappings if provided
                self._create_execution_mappings(conn, strategy_id, strategy)

                # Create exchange-pair mappings if provided
                self._create_exchange_pair_mappings(conn, strategy_id, strategy)

                # Return full strategy object with all relations
                return self.get_by_id(strategy_id)
        except Exception as e:
            self.logger.error(f"Error creating strategy {strategy.name}: {e}", exc_info=True)
            raise

    def get_by_id(self, strategy_id: int) -> Optional[Strategy]:
        """Get a strategy by its ID with all related entities"""
        try:
            with self.engine.begin() as conn:
                # Get the base strategy
                strategy_result = conn.execute(self.table.select().where(self.table.c.id == strategy_id))
                strategy_row = strategy_result.first()

                if not strategy_row:
                    return None

                # Convert row to dict using _asdict() method for sqlalchemy Row objects
                strategy_dict = strategy_row._asdict()

                # Parse JSON fields
                strategy_dict["additional_info"] = self._parse_json_field(strategy_dict.get("additional_info"))

                # Get and add parameters
                params = self._get_strategy_parameters(conn, strategy_id)
                if params:
                    strategy_dict.update(
                        {
                            "min_spread": params.min_spread,
                            "threshold": params.threshold,
                            "max_slippage": params.max_slippage,
                            "min_volume": params.min_volume,
                        }
                    )

                # Get and add type parameters
                type_params = self._get_type_parameters(conn, strategy_id)
                if type_params:
                    strategy_dict["type_parameters"] = type_params.model_dump(
                        exclude={"id", "strategy_id", "created_at", "updated_at"}
                    )

                # Get and add strategy type
                strategy_type = self._get_strategy_type(conn, strategy_dict["strategy_type_id"])
                if strategy_type:
                    strategy_dict["strategy_type"] = strategy_type.model_dump(exclude={"created_at"})

                # Get and add risk profile
                risk_profile = self._get_risk_profile(conn, strategy_dict["risk_profile_id"])
                if risk_profile:
                    strategy_dict["risk_profile"] = risk_profile.model_dump(exclude={"created_at", "updated_at"})

                # Get and add execution mappings
                execution_mappings = self._get_execution_mappings(conn, strategy_id)
                strategy_dict["execution_mappings"] = [mapping.model_dump() for mapping in execution_mappings]

                # Get and add exchange-pair mappings
                exchange_pair_mappings = self._get_exchange_pair_mappings(conn, strategy_id)
                strategy_dict["exchange_pair_mappings"] = [mapping.model_dump() for mapping in exchange_pair_mappings]

                # Create Strategy Pydantic model
                return Strategy.model_validate(strategy_dict)
        except Exception as e:
            self.logger.error(f"Error getting strategy by ID {strategy_id}: {e}", exc_info=True)
            raise

    def get_by_name(self, name: str) -> Optional[Strategy]:
        """Get a strategy by its name"""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(self.table.select().where(self.table.c.name == name))
                row = result.first()

                if not row:
                    self.logger.info(f"Strategy with name '{name}' not found in database")
                    return None

                return self.get_by_id(row.id)
        except Exception as e:
            self.logger.error(f"Error getting strategy by name {name}: {e}", exc_info=True)
            raise

    def get_all(self) -> List[Strategy]:
        """Get all strategies with their parameters"""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(self.table.select())
                strategy_ids = [row.id for row in result]

                return [
                    strategy for strategy_id in strategy_ids if (strategy := self.get_by_id(strategy_id)) is not None
                ]
        except Exception as e:
            self.logger.error(f"Error getting all strategies: {e}", exc_info=True)
            raise

    def get_active(self) -> List[Strategy]:
        """Get all active strategies with their parameters"""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(self.table.select().where(self.table.c.is_active))
                strategy_ids = [row.id for row in result]

                return [
                    strategy for strategy_id in strategy_ids if (strategy := self.get_by_id(strategy_id)) is not None
                ]
        except Exception as e:
            self.logger.error(f"Error getting active strategies: {e}", exc_info=True)
            raise

    def update(self, strategy: Strategy) -> Optional[Strategy]:
        """Update an existing strategy with all its related entities"""
        try:
            if not strategy.id:
                self.logger.error("Cannot update strategy without ID")
                return None

            with self.engine.begin() as conn:
                # Verify strategy exists
                check_result = conn.execute(self.table.select().where(self.table.c.id == strategy.id))
                if not check_result.first():
                    self.logger.error(f"Strategy with ID {strategy.id} not found for update")
                    return None

                # Convert Strategy to dict for db update
                strategy_dict = self._prepare_strategy_dict(strategy)
                strategy_dict["last_updated"] = datetime.now()

                # Update strategy table
                conn.execute(self.table.update().where(self.table.c.id == strategy.id).values(**strategy_dict))

                # Update or create parameters
                self._update_strategy_parameters(conn, strategy.id, strategy)

                # Update or create type parameters
                self._update_type_parameters(conn, strategy.id, strategy)

                # Handle execution mappings
                self._update_execution_mappings(conn, strategy.id, strategy)

                # Handle exchange-pair mappings
                self._update_exchange_pair_mappings(conn, strategy.id, strategy)

                # Return updated strategy with all relations
                return self.get_by_id(strategy.id)
        except Exception as e:
            self.logger.error(
                f"Error updating strategy {strategy.name if strategy.name else strategy.id}: {e}", exc_info=True
            )
            raise

    def delete(self, strategy_id: int) -> bool:
        """Delete a strategy and all its related entities"""
        try:
            with self.engine.begin() as conn:
                # First check if strategy exists
                check_result = conn.execute(self.table.select().where(self.table.c.id == strategy_id))
                if not check_result.first():
                    self.logger.warning(f"Strategy ID {strategy_id} not found for deletion")
                    return False

                # Delete related records in this order to maintain referential integrity
                # 1. Delete exchange-pair mappings
                conn.execute(
                    self.exchange_pair_mapping_table.delete().where(
                        self.exchange_pair_mapping_table.c.strategy_id == strategy_id
                    )
                )

                # 2. Delete execution mappings
                conn.execute(
                    self.exec_mapping_table.delete().where(self.exec_mapping_table.c.strategy_id == strategy_id)
                )

                # 3. Delete type parameters
                conn.execute(self.type_params_table.delete().where(self.type_params_table.c.strategy_id == strategy_id))

                # 4. Delete strategy parameters
                conn.execute(self.params_table.delete().where(self.params_table.c.strategy_id == strategy_id))

                # 5. Finally delete the strategy itself
                conn.execute(self.table.delete().where(self.table.c.id == strategy_id))

                self.logger.info(f"Successfully deleted strategy ID {strategy_id} and all related data")
                return True
        except Exception as e:
            self.logger.error(f"Error deleting strategy ID {strategy_id}: {e}", exc_info=True)
            return False

    def delete_by_name(self, strategy_name: str) -> bool:
        """Delete a strategy and its related data by name"""
        try:
            with self.engine.begin() as conn:
                # First get the ID
                result = conn.execute(self.table.select().where(self.table.c.name == strategy_name))
                row = result.first()

                if not row:
                    self.logger.warning(f"Strategy '{strategy_name}' not found for deletion")
                    return False

                # Use the ID-based delete method
                return self.delete(row.id)
        except Exception as e:
            self.logger.error(f"Error deleting strategy '{strategy_name}': {e}", exc_info=True)
            return False

    def set_active_status(self, strategy_id: int, is_active: bool) -> bool:
        """Set strategy active status"""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(
                    self.table.update()
                    .where(self.table.c.id == strategy_id)
                    .values(is_active=is_active, last_updated=datetime.now())
                    .returning(self.table.c.id)
                )
                updated_id = result.scalar()
                return updated_id is not None
        except Exception as e:
            self.logger.error(f"Error updating strategy status for ID {strategy_id}: {e}", exc_info=True)
            return False

    def set_active_status_by_name(self, strategy_name: str, is_active: bool) -> bool:
        """Update strategy status by name."""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(self.table.select().where(self.table.c.name == strategy_name))
                row = result.first()

                if not row:
                    self.logger.warning(f"Strategy '{strategy_name}' not found for status update")
                    return False

                return self.set_active_status(row.id, is_active)
        except Exception as e:
            self.logger.error(f"Error updating strategy status for '{strategy_name}': {e}", exc_info=True)
            return False

    def activate_by_name(self, strategy_name: str) -> bool:
        """Activate a strategy by name."""
        return self.set_active_status_by_name(strategy_name, True)

    def deactivate_by_name(self, strategy_name: str) -> bool:
        """Deactivate a strategy by name."""
        return self.set_active_status_by_name(strategy_name, False)

    def update_stats(self, strategy_id: int, profit: float = 0, loss: float = 0, trade_count: int = 0) -> bool:
        """Update strategy statistics after trade execution by ID"""
        try:
            with self.engine.begin() as conn:
                # Get current stats
                query = sa.select(
                    [self.table.c.total_profit, self.table.c.total_loss, self.table.c.trade_count, self.table.c.name]
                ).where(self.table.c.id == strategy_id)

                result = conn.execute(query).first()
                if not result:
                    self.logger.warning(f"Strategy ID {strategy_id} not found for stats update")
                    return False

                # Convert to compatible types
                current_profit = Decimal(str(result.total_profit))
                current_loss = Decimal(str(result.total_loss))
                current_trade_count = result.trade_count
                strategy_name = result.name

                # Convert input params to Decimal
                profit_decimal = Decimal(str(profit))
                loss_decimal = Decimal(str(loss))

                # Calculate new values
                new_total_profit = current_profit + profit_decimal
                new_total_loss = current_loss + loss_decimal
                new_net_profit = new_total_profit - new_total_loss
                new_trade_count = current_trade_count + trade_count

                # Update with new values
                conn.execute(
                    self.table.update()
                    .where(self.table.c.id == strategy_id)
                    .values(
                        total_profit=new_total_profit,
                        total_loss=new_total_loss,
                        net_profit=new_net_profit,
                        trade_count=new_trade_count,
                        last_updated=datetime.now(),
                    )
                )

                self.logger.info(
                    f"Updated stats for {strategy_name} (ID: {strategy_id}): "
                    f"profit +{profit}, loss +{loss}, net profit = {new_net_profit}, trades +{trade_count}"
                )
                return True
        except Exception as e:
            self.logger.error(f"Error updating strategy stats for ID {strategy_id}: {e}", exc_info=True)
            return False

    def update_stats_by_name(
        self, strategy_name: str, profit: float = 0, loss: float = 0, trade_count: int = 0
    ) -> bool:
        """Update strategy statistics after a trade execution by name"""
        try:
            with self.engine.begin() as conn:
                # Get ID
                result = conn.execute(self.table.select().where(self.table.c.name == strategy_name))
                row = result.first()

                if not row:
                    self.logger.warning(f"Strategy '{strategy_name}' not found for stats update")
                    return False

                return self.update_stats(row.id, profit, loss, trade_count)
        except Exception as e:
            self.logger.error(f"Error updating stats for strategy '{strategy_name}': {e}", exc_info=True)
            return False

    def add_exchange_pair_mapping(
        self, strategy_id: int, exchange_id: int, trading_pair_id: int, is_active: bool = True
    ) -> bool:
        """Add a new exchange-pair mapping to a strategy"""
        try:
            with self.engine.begin() as conn:
                # Check if the mapping already exists
                check_query = sa.select([self.exchange_pair_mapping_table.c.id]).where(
                    sa.and_(
                        self.exchange_pair_mapping_table.c.strategy_id == strategy_id,
                        self.exchange_pair_mapping_table.c.exchange_id == exchange_id,
                        self.exchange_pair_mapping_table.c.trading_pair_id == trading_pair_id,
                    )
                )

                check_result = conn.execute(check_query)
                existing_row = check_result.first()

                if existing_row:
                    # Update the existing mapping
                    conn.execute(
                        self.exchange_pair_mapping_table.update()
                        .where(self.exchange_pair_mapping_table.c.id == existing_row.id)
                        .values(is_active=is_active, updated_at=datetime.now())
                    )
                    self.logger.info(f"Updated existing exchange-pair mapping for strategy ID {strategy_id}")
                else:
                    # Insert a new mapping
                    # Create a StrategyExchangePairMapping model
                    mapping = StrategyExchangePairMapping(
                        strategy_id=strategy_id,
                        exchange_id=exchange_id,
                        trading_pair_id=trading_pair_id,
                        is_active=is_active,
                    )

                    # Insert into database
                    conn.execute(self.exchange_pair_mapping_table.insert().values(mapping.model_dump(exclude={"id"})))
                    self.logger.info(f"Added new exchange-pair mapping for strategy ID {strategy_id}")

                return True
        except Exception as e:
            self.logger.error(f"Error adding exchange-pair mapping for strategy ID {strategy_id}: {e}", exc_info=True)
            return False

    def add_execution_strategy(
        self, strategy_id: int, execution_strategy_id: int, priority: int = 100, is_active: bool = True
    ) -> bool:
        """Add or update an execution strategy mapping for a strategy"""
        try:
            with self.engine.begin() as conn:
                # Check if the mapping already exists
                check_query = sa.select([self.exec_mapping_table.c.id]).where(
                    sa.and_(
                        self.exec_mapping_table.c.strategy_id == strategy_id,
                        self.exec_mapping_table.c.execution_strategy_id == execution_strategy_id,
                    )
                )

                check_result = conn.execute(check_query)
                existing_row = check_result.first()

                if existing_row:
                    # Update the existing mapping
                    conn.execute(
                        self.exec_mapping_table.update()
                        .where(self.exec_mapping_table.c.id == existing_row.id)
                        .values(is_active=is_active, priority=priority)
                    )
                    self.logger.info(f"Updated existing execution strategy mapping for strategy ID {strategy_id}")
                else:
                    # Create a StrategyExecutionMapping model
                    mapping = StrategyExecutionMapping(
                        strategy_id=strategy_id,
                        execution_strategy_id=execution_strategy_id,
                        is_active=is_active,
                        priority=priority,
                    )

                    # Insert into database
                    conn.execute(self.exec_mapping_table.insert().values(mapping.model_dump(exclude={"id"})))
                    self.logger.info(f"Added new execution strategy mapping for strategy ID {strategy_id}")

                return True
        except Exception as e:
            self.logger.error(f"Error adding execution strategy for strategy ID {strategy_id}: {e}", exc_info=True)
            return False

    # Helper methods for working with models

    def _prepare_strategy_dict(self, strategy: Strategy) -> Dict[str, Any]:
        """Prepare a strategy dict for database operations, excluding related entities"""
        # Use model_dump to get a dict of all fields
        strategy_dict = strategy.model_dump(
            exclude={
                "id",
                "parameters",
                "type_parameters",
                "risk_profile",
                "strategy_type",
                "execution_mappings",
                "exchange_pair_mappings",
                "min_spread",
                "threshold",
                "max_slippage",
                "min_volume",
                "latest_metrics",
                "metrics",
            }
        )

        # Serialize JSON fields
        strategy_dict["additional_info"] = self._serialize_json_field(strategy_dict.get("additional_info"))

        return strategy_dict

    def _row_to_model(self, row: sa.engine.row.Row, model_class: Type[T]) -> T:
        """Convert a SQLAlchemy row to a Pydantic model"""
        # Convert row to dict using _asdict() method which is reliable for SQLAlchemy Row objects
        data = row._asdict()

        # Parse any JSON fields if needed
        for field_name, field_value in data.items():
            if isinstance(field_value, str) and (
                field_name.endswith("_info")
                or field_name in ["parameters", "circuit_breaker_conditions", "mapping", "withdrawal_fee"]
            ):
                data[field_name] = self._parse_json_field(field_value)

        # Create and return the model
        return model_class.model_validate(data)

    def _get_model_by_id(self, conn: Connection, table: Table, model_class: Type[T], id_value: int) -> Optional[T]:
        """Generic method to get a model by ID"""
        result = conn.execute(table.select().where(table.c.id == id_value))
        row = result.first()

        if not row:
            return None

        return self._row_to_model(row, model_class)

    def _create_strategy_parameters(self, conn: Connection, strategy_id: int, strategy: Strategy) -> None:
        """Create strategy parameters for a new strategy"""
        # Create a StrategyParameters model
        params = StrategyParameters(
            strategy_id=strategy_id,
            min_spread=strategy.min_spread if strategy.min_spread is not None else 0.0001,
            threshold=strategy.threshold if strategy.threshold is not None else 0.0001,
            max_slippage=strategy.max_slippage,
            min_volume=strategy.min_volume,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Insert into database
        conn.execute(self.params_table.insert().values(params.model_dump(exclude={"id"})))

    def _update_strategy_parameters(self, conn: Connection, strategy_id: int, strategy: Strategy) -> None:
        """Update or create strategy parameters"""
        # Check if parameters exist
        params_result = conn.execute(self.params_table.select().where(self.params_table.c.strategy_id == strategy_id))
        params_row = params_result.first()

        # Create params model with updated values
        params = StrategyParameters(
            strategy_id=strategy_id,
            min_spread=strategy.min_spread if strategy.min_spread is not None else 0.0001,
            threshold=strategy.threshold if strategy.threshold is not None else 0.0001,
            max_slippage=strategy.max_slippage,
            min_volume=strategy.min_volume,
            updated_at=datetime.now(),
        )

        if params_row:
            # Update existing parameters
            conn.execute(
                self.params_table.update()
                .where(self.params_table.c.strategy_id == strategy_id)
                .values(
                    min_spread=params.min_spread,
                    threshold=params.threshold,
                    max_slippage=params.max_slippage,
                    min_volume=params.min_volume,
                    updated_at=params.updated_at,
                )
            )
        else:
            # Insert new parameters
            params.created_at = datetime.now()
            conn.execute(self.params_table.insert().values(params.model_dump(exclude={"id"})))

    def _create_type_parameters(self, conn: Connection, strategy_id: int, strategy: Strategy) -> None:
        """Create type parameters for a new strategy if provided"""
        if not hasattr(strategy, "type_parameters") or not strategy.type_parameters:
            return

        # Extract type parameters
        type_params_data = strategy.type_parameters
        if isinstance(type_params_data, dict):
            # Create StrategyTypeParameters model
            type_params = StrategyTypeParameters(
                strategy_id=strategy_id,
                target_volume=type_params_data.get("target_volume"),
                min_depth=type_params_data.get("min_depth"),
                min_depth_percentage=type_params_data.get("min_depth_percentage"),
                parameters=type_params_data.get("parameters"),
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )

            # Insert into database
            conn.execute(self.type_params_table.insert().values(type_params.model_dump(exclude={"id"})))

    def _update_type_parameters(self, conn: Connection, strategy_id: int, strategy: Strategy) -> None:
        """Update or create type parameters if provided"""
        if not hasattr(strategy, "type_parameters") or not strategy.type_parameters:
            return

        # Check if type parameters exist
        type_params_result = conn.execute(
            self.type_params_table.select().where(self.type_params_table.c.strategy_id == strategy_id)
        )
        type_params_row = type_params_result.first()

        # Extract type parameters
        type_params_data = strategy.type_parameters
        if isinstance(type_params_data, dict):
            # Create StrategyTypeParameters model with updated values
            type_params = StrategyTypeParameters(
                strategy_id=strategy_id,
                target_volume=type_params_data.get("target_volume"),
                min_depth=type_params_data.get("min_depth"),
                min_depth_percentage=type_params_data.get("min_depth_percentage"),
                parameters=type_params_data.get("parameters"),
                updated_at=datetime.now(),
            )

            # Prepare the parameters field for database
            params_json = self._serialize_json_field(type_params.parameters)

            if type_params_row:
                # Update existing parameters
                conn.execute(
                    self.type_params_table.update()
                    .where(self.type_params_table.c.strategy_id == strategy_id)
                    .values(
                        target_volume=type_params.target_volume,
                        min_depth=type_params.min_depth,
                        min_depth_percentage=type_params.min_depth_percentage,
                        parameters=params_json,
                        updated_at=type_params.updated_at,
                    )
                )
            else:
                # Insert new parameters
                type_params.created_at = datetime.now()
                conn.execute(
                    self.type_params_table.insert().values(
                        strategy_id=type_params.strategy_id,
                        target_volume=type_params.target_volume,
                        min_depth=type_params.min_depth,
                        min_depth_percentage=type_params.min_depth_percentage,
                        parameters=params_json,
                        created_at=type_params.created_at,
                        updated_at=type_params.updated_at,
                    )
                )

    def _create_execution_mappings(self, conn: Connection, strategy_id: int, strategy: Strategy) -> None:
        """Create execution mappings for a new strategy if provided"""
        if not hasattr(strategy, "execution_mappings") or not strategy.execution_mappings:
            return

        for mapping_data in strategy.execution_mappings:
            # Create StrategyExecutionMapping model
            if isinstance(mapping_data, dict):
                mapping = StrategyExecutionMapping(
                    strategy_id=strategy_id,
                    execution_strategy_id=mapping_data.get("execution_strategy_id"),
                    is_active=mapping_data.get("is_active", True),
                    priority=mapping_data.get("priority", 100),
                    created_at=datetime.now(),
                )

                # Insert into database
                conn.execute(self.exec_mapping_table.insert().values(mapping.model_dump(exclude={"id"})))
            elif isinstance(mapping_data, StrategyExecutionMapping):
                # Already a model, just update the strategy_id
                mapping_data.strategy_id = strategy_id
                if not mapping_data.created_at:
                    mapping_data.created_at = datetime.now()

                # Insert into database
                conn.execute(self.exec_mapping_table.insert().values(mapping_data.model_dump(exclude={"id"})))

    def _update_execution_mappings(self, conn: Connection, strategy_id: int, strategy: Strategy) -> None:
        """Update execution mappings for a strategy"""
        if not hasattr(strategy, "execution_mappings") or not strategy.execution_mappings:
            return

        # Check if we should replace all mappings
        if getattr(strategy, "replace_execution_mappings", False):
            # Delete all existing mappings
            conn.execute(self.exec_mapping_table.delete().where(self.exec_mapping_table.c.strategy_id == strategy_id))

            # Create new mappings
            self._create_execution_mappings(conn, strategy_id, strategy)
        else:
            # Update existing or create new mappings
            for mapping_data in strategy.execution_mappings:
                if isinstance(mapping_data, dict):
                    mapping_id = mapping_data.get("id")
                    if mapping_id:
                        # Update existing mapping
                        conn.execute(
                            self.exec_mapping_table.update()
                            .where(self.exec_mapping_table.c.id == mapping_id)
                            .values(
                                execution_strategy_id=mapping_data.get("execution_strategy_id"),
                                is_active=mapping_data.get("is_active", True),
                                priority=mapping_data.get("priority", 100),
                            )
                        )
                    else:
                        # Create new mapping
                        mapping = StrategyExecutionMapping(
                            strategy_id=strategy_id,
                            execution_strategy_id=mapping_data.get("execution_strategy_id"),
                            is_active=mapping_data.get("is_active", True),
                            priority=mapping_data.get("priority", 100),
                            created_at=datetime.now(),
                        )

                        conn.execute(self.exec_mapping_table.insert().values(mapping.model_dump(exclude={"id"})))
                elif isinstance(mapping_data, StrategyExecutionMapping):
                    # Handle Pydantic model instances
                    if mapping_data.id:
                        # Update existing mapping
                        conn.execute(
                            self.exec_mapping_table.update()
                            .where(self.exec_mapping_table.c.id == mapping_data.id)
                            .values(
                                execution_strategy_id=mapping_data.execution_strategy_id,
                                is_active=mapping_data.is_active,
                                priority=mapping_data.priority,
                            )
                        )
                    else:
                        # Create new mapping
                        mapping_data.strategy_id = strategy_id
                        if not mapping_data.created_at:
                            mapping_data.created_at = datetime.now()

                        conn.execute(self.exec_mapping_table.insert().values(mapping_data.model_dump(exclude={"id"})))

    def _create_exchange_pair_mappings(self, conn: Connection, strategy_id: int, strategy: Strategy) -> None:
        """Create exchange-pair mappings for a new strategy if provided"""
        if not hasattr(strategy, "exchange_pair_mappings") or not strategy.exchange_pair_mappings:
            return

        for mapping_data in strategy.exchange_pair_mappings:
            # Create StrategyExchangePairMapping model
            if isinstance(mapping_data, dict):
                mapping = StrategyExchangePairMapping(
                    strategy_id=strategy_id,
                    exchange_id=mapping_data.get("exchange_id"),
                    trading_pair_id=mapping_data.get("trading_pair_id"),
                    is_active=mapping_data.get("is_active", True),
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                )

                # Insert into database
                conn.execute(self.exchange_pair_mapping_table.insert().values(mapping.model_dump(exclude={"id"})))
            elif isinstance(mapping_data, StrategyExchangePairMapping):
                # Already a model, just update the strategy_id
                mapping_data.strategy_id = strategy_id
                if not mapping_data.created_at:
                    mapping_data.created_at = datetime.now()
                if not mapping_data.updated_at:
                    mapping_data.updated_at = datetime.now()

                # Insert into database
                conn.execute(self.exchange_pair_mapping_table.insert().values(mapping_data.model_dump(exclude={"id"})))

    def _update_exchange_pair_mappings(self, conn: Connection, strategy_id: int, strategy: Strategy) -> None:
        """Update exchange-pair mappings for a strategy"""
        if not hasattr(strategy, "exchange_pair_mappings") or not strategy.exchange_pair_mappings:
            return

        # Check if we should replace all mappings
        if getattr(strategy, "replace_exchange_pair_mappings", False):
            # Delete all existing mappings
            conn.execute(
                self.exchange_pair_mapping_table.delete().where(
                    self.exchange_pair_mapping_table.c.strategy_id == strategy_id
                )
            )

            # Create new mappings
            self._create_exchange_pair_mappings(conn, strategy_id, strategy)
        else:
            # Update existing or create new mappings
            for mapping_data in strategy.exchange_pair_mappings:
                if isinstance(mapping_data, dict):
                    mapping_id = mapping_data.get("id")
                    if mapping_id:
                        # Update existing mapping
                        conn.execute(
                            self.exchange_pair_mapping_table.update()
                            .where(self.exchange_pair_mapping_table.c.id == mapping_id)
                            .values(
                                exchange_id=mapping_data.get("exchange_id"),
                                trading_pair_id=mapping_data.get("trading_pair_id"),
                                is_active=mapping_data.get("is_active", True),
                                updated_at=datetime.now(),
                            )
                        )
                    else:
                        # Create new mapping
                        mapping = StrategyExchangePairMapping(
                            strategy_id=strategy_id,
                            exchange_id=mapping_data.get("exchange_id"),
                            trading_pair_id=mapping_data.get("trading_pair_id"),
                            is_active=mapping_data.get("is_active", True),
                            created_at=datetime.now(),
                            updated_at=datetime.now(),
                        )

                        conn.execute(
                            self.exchange_pair_mapping_table.insert().values(mapping.model_dump(exclude={"id"}))
                        )
                elif isinstance(mapping_data, StrategyExchangePairMapping):
                    # Handle Pydantic model instances
                    if mapping_data.id:
                        # Update existing mapping
                        conn.execute(
                            self.exchange_pair_mapping_table.update()
                            .where(self.exchange_pair_mapping_table.c.id == mapping_data.id)
                            .values(
                                exchange_id=mapping_data.exchange_id,
                                trading_pair_id=mapping_data.trading_pair_id,
                                is_active=mapping_data.is_active,
                                updated_at=datetime.now(),
                            )
                        )
                    else:
                        # Create new mapping
                        mapping_data.strategy_id = strategy_id
                        if not mapping_data.created_at:
                            mapping_data.created_at = datetime.now()
                        mapping_data.updated_at = datetime.now()

                        conn.execute(
                            self.exchange_pair_mapping_table.insert().values(mapping_data.model_dump(exclude={"id"}))
                        )

    def _get_strategy_parameters(self, conn: Connection, strategy_id: int) -> Optional[StrategyParameters]:
        """Get strategy parameters for a strategy"""
        try:
            result = conn.execute(self.params_table.select().where(self.params_table.c.strategy_id == strategy_id))
            row = result.first()

            if not row:
                raise ValueError(f"No parameters found for strategy ID {strategy_id}")

            # Convert row to dict
            params_dict = row._asdict()

            # Parse the additional_parameters JSON if present
            if "additional_parameters" in params_dict and params_dict["additional_parameters"]:
                params_dict["additional_parameters"] = self._parse_json_field(params_dict["additional_parameters"])
            else:
                raise ValueError(f"Missing additional_parameters for strategy ID {strategy_id}")

            return StrategyParameters.model_validate(params_dict)
        except Exception as e:
            self.logger.error(f"Error getting strategy parameters for strategy ID {strategy_id}: {e}", exc_info=True)
            raise  # Re-raise the exception instead of returning None

    def _get_type_parameters(self, conn: Connection, strategy_id: int) -> Optional[StrategyTypeParameters]:
        """Get type parameters for a strategy"""
        result = conn.execute(
            self.type_params_table.select().where(self.type_params_table.c.strategy_id == strategy_id)
        )
        row = result.first()

        if not row:
            return None

        return self._row_to_model(row, StrategyTypeParameters)

    def _get_strategy_type(self, conn: Connection, strategy_type_id: int) -> Optional[StrategyType]:
        """Get strategy type by ID"""
        return self._get_model_by_id(conn, self.strategy_types_table, StrategyType, strategy_type_id)

    def _get_risk_profile(self, conn: Connection, risk_profile_id: int) -> Optional[RiskProfile]:
        """Get risk profile by ID"""
        return self._get_model_by_id(conn, self.risk_profiles_table, RiskProfile, risk_profile_id)

    def _get_execution_mappings(self, conn: Connection, strategy_id: int) -> List[StrategyExecutionMapping]:
        """Get all execution mappings for a strategy with execution strategy details"""
        # Use raw SQL for the join to get both mapping and execution strategy details
        query = sa.text("""
            SELECT sem.*, es.name as execution_name, es.description as execution_description
            FROM strategy_execution_mapping sem
            JOIN execution_strategies es ON sem.execution_strategy_id = es.id
            WHERE sem.strategy_id = :strategy_id
            ORDER BY sem.priority ASC
        """)

        result = conn.execute(query, {"strategy_id": strategy_id})
        mappings = []

        for row in result:
            # Convert row to dict using _asdict()
            row_dict = row._asdict()

            # Create StrategyExecutionMapping model
            mapping = StrategyExecutionMapping.model_validate(
                {
                    "id": row_dict["id"],
                    "strategy_id": row_dict["strategy_id"],
                    "execution_strategy_id": row_dict["execution_strategy_id"],
                    "is_active": row_dict["is_active"],
                    "priority": row_dict["priority"],
                    "created_at": row_dict["created_at"],
                    # Add execution strategy details
                    "execution_name": row_dict["execution_name"],
                    "execution_description": row_dict["execution_description"],
                }
            )

            mappings.append(mapping)

        return mappings

    def _get_exchange_pair_mappings(self, conn: Connection, strategy_id: int) -> List[StrategyExchangePairMapping]:
        """Get all exchange-pair mappings for a strategy with exchange and pair details"""
        try:
            # Use raw SQL for the join to get both mapping and exchange/pair details
            query = sa.text("""
                SELECT 
                    sepm.id, 
                    sepm.strategy_id, 
                    sepm.exchange_id, 
                    sepm.trading_pair_id, 
                    sepm.is_active,
                    sepm.created_at,
                    sepm.updated_at,
                    e.name as exchange_name, 
                    tp.symbol as pair_symbol,
                    tp.base_currency,
                    tp.quote_currency
                FROM strategy_exchange_pair_mappings sepm
                JOIN exchanges e ON sepm.exchange_id = e.id
                JOIN trading_pairs tp ON sepm.trading_pair_id = tp.id
                WHERE sepm.strategy_id = :strategy_id
            """)

            result = conn.execute(query, {"strategy_id": strategy_id})
            mappings = []

            for row in result:
                # Convert row to dict using _asdict()
                row_dict = row._asdict()

                # Create StrategyExchangePairMapping model
                mapping = StrategyExchangePairMapping.model_validate(
                    {
                        "id": row_dict["id"],
                        "strategy_id": row_dict["strategy_id"],
                        "exchange_id": row_dict["exchange_id"],
                        "trading_pair_id": row_dict["trading_pair_id"],
                        "is_active": row_dict["is_active"],
                        "created_at": row_dict["created_at"],
                        "updated_at": row_dict["updated_at"],
                        # Add exchange and pair details
                        "exchange_name": row_dict["exchange_name"],
                        "pair_symbol": row_dict["pair_symbol"],
                        "base_currency": row_dict["base_currency"],
                        "quote_currency": row_dict["quote_currency"],
                    }
                )

                mappings.append(mapping)

            return mappings
        except Exception as e:
            self.logger.error(f"Error getting exchange-pair mappings for strategy {strategy_id}: {e}", exc_info=True)
            return []

    def _parse_json_field(self, json_data: Optional[Union[str, dict]]) -> Optional[Dict[str, Any]]:
        """Safely parse a JSON field from the database"""
        if json_data is None:
            return {}

        if isinstance(json_data, dict):
            return json_data

        try:
            if isinstance(json_data, str):
                return json.loads(json_data)
            else:
                self.logger.warning(f"Unexpected JSON data type: {type(json_data)}")
                return {}
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON data: {e}, data: {json_data[:100]}...", exc_info=True)
            return {}

    def _serialize_json_field(self, data: Optional[Union[Dict[str, Any], str]]) -> Optional[str]:
        """Serialize a dict to JSON for database storage"""
        if data is None:
            return None

        try:
            if isinstance(data, dict):
                return json.dumps(data)
            elif isinstance(data, str):
                # Validate it's proper JSON by parsing it
                try:
                    json.loads(data)
                    return data
                except json.JSONDecodeError:
                    self.logger.warning(f"Invalid JSON string provided: {data[:100]}...", exc_info=True)
                    return json.dumps({})
            else:
                self.logger.warning(f"Unexpected data type for JSON serialization: {type(data)}", exc_info=True)
                return json.dumps({})
        except Exception as e:
            self.logger.error(f"Error serializing JSON data: {e}", exc_info=True)
            return json.dumps({})
