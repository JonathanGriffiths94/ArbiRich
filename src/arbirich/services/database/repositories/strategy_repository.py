import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

import sqlalchemy as sa

from src.arbirich.models.models import Strategy
from src.arbirich.models.schema import strategies, strategy_parameters
from src.arbirich.services.database.base_repository import BaseRepository


class StrategyRepository(BaseRepository[Strategy]):
    """Repository for Strategy entity operations"""

    def __init__(self, *args, **kwargs):
        super().__init__(model_class=Strategy, *args, **kwargs)
        self.table = strategies
        self.params_table = strategy_parameters

    def create(self, strategy: Strategy) -> Strategy:
        """Create a new strategy with its parameters"""
        try:
            with self.engine.begin() as conn:
                # Extract parameters for strategy_parameters table
                min_spread = strategy.min_spread if strategy.min_spread is not None else 0.0001
                threshold = (
                    strategy.threshold if hasattr(strategy, "threshold") and strategy.threshold is not None else 0.0001
                )
                max_slippage = (
                    strategy.max_slippage
                    if hasattr(strategy, "max_slippage") and strategy.max_slippage is not None
                    else None
                )
                min_volume = (
                    strategy.min_volume if hasattr(strategy, "min_volume") and strategy.min_volume is not None else None
                )

                # Prepare strategy data
                strategy_data = {
                    "name": strategy.name,
                    "description": strategy.description,
                    "strategy_type_id": strategy.strategy_type_id,
                    "risk_profile_id": strategy.risk_profile_id,
                    "starting_capital": strategy.starting_capital,
                    "total_profit": strategy.total_profit,
                    "total_loss": strategy.total_loss,
                    "net_profit": strategy.net_profit,
                    "trade_count": strategy.trade_count,
                    "start_timestamp": strategy.start_timestamp,
                    "last_updated": strategy.last_updated or datetime.now(),
                    "is_active": strategy.is_active,
                    "created_by": strategy.created_by,
                    "additional_info": json.dumps(strategy.additional_info) if strategy.additional_info else None,
                }

                # Insert strategy and get ID
                strategy_result = conn.execute(self.table.insert().values(**strategy_data).returning(*self.table.c))
                strategy_row = strategy_result.first()
                strategy_id = strategy_row.id

                # Insert strategy parameters
                params_data = {
                    "strategy_id": strategy_id,
                    "min_spread": min_spread,
                    "threshold": threshold,
                    "max_slippage": max_slippage,
                    "min_volume": min_volume,
                }

                conn.execute(self.params_table.insert().values(**params_data))

                # Return full strategy object with parameters
                return self.get_by_id(strategy_id)
        except Exception as e:
            self.logger.error(f"Error creating strategy: {e}")
            raise

    def get_by_id(self, strategy_id: int) -> Optional[Strategy]:
        """Get a strategy by its ID"""
        try:
            with self.engine.begin() as conn:
                # First, get the strategy from the strategies table
                result = conn.execute(self.table.select().where(self.table.c.id == strategy_id))
                row = result.first()

                if not row:
                    return None

                # Create a dict from the row
                strategy_data = row._asdict()

                # Get parameters from the strategy_parameters table
                params_query = sa.text("""
                    SELECT min_spread, threshold, max_slippage, min_volume FROM strategy_parameters
                    WHERE strategy_id = :strategy_id
                """)
                params_result = conn.execute(params_query, {"strategy_id": strategy_data["id"]})
                params_row = params_result.first()

                # Add parameters to the strategy data
                if params_row:
                    strategy_data["min_spread"] = float(params_row.min_spread)
                    strategy_data["threshold"] = float(params_row.threshold) if params_row.threshold else None
                    strategy_data["max_slippage"] = float(params_row.max_slippage) if params_row.max_slippage else None
                    strategy_data["min_volume"] = float(params_row.min_volume) if params_row.min_volume else None
                else:
                    # Provide default values if no parameters exist
                    strategy_data["min_spread"] = 0.0001
                    strategy_data["threshold"] = 0.0001
                    strategy_data["max_slippage"] = None
                    strategy_data["min_volume"] = None

                # Get type parameters
                type_params_query = sa.text("""
                    SELECT target_volume, min_depth, min_depth_percentage, parameters 
                    FROM strategy_type_parameters
                    WHERE strategy_id = :strategy_id
                """)
                type_params_result = conn.execute(type_params_query, {"strategy_id": strategy_data["id"]})
                type_params_row = type_params_result.first()

                if type_params_row:
                    strategy_data["type_parameters"] = {
                        "target_volume": float(type_params_row.target_volume)
                        if type_params_row.target_volume
                        else None,
                        "min_depth": type_params_row.min_depth,
                        "min_depth_percentage": float(type_params_row.min_depth_percentage)
                        if type_params_row.min_depth_percentage
                        else None,
                        "parameters": json.loads(type_params_row.parameters) if type_params_row.parameters else None,
                    }

                # Get strategy type information
                strategy_type_query = sa.text("""
                    SELECT id, name, description, implementation_class
                    FROM strategy_types 
                    WHERE id = :strategy_type_id
                """)
                strategy_type_result = conn.execute(
                    strategy_type_query, {"strategy_type_id": strategy_data["strategy_type_id"]}
                )
                strategy_type_row = strategy_type_result.first()

                if strategy_type_row:
                    strategy_data["strategy_type"] = {
                        "id": strategy_type_row.id,
                        "name": strategy_type_row.name,
                        "description": strategy_type_row.description,
                        "implementation_class": strategy_type_row.implementation_class,
                    }

                # Get risk profile information
                risk_profile_query = sa.text("""
                    SELECT id, name, description, max_position_size_percentage, max_drawdown_percentage,
                           max_exposure_per_asset_percentage, circuit_breaker_conditions
                    FROM risk_profiles 
                    WHERE id = :risk_profile_id
                """)
                risk_profile_result = conn.execute(
                    risk_profile_query, {"risk_profile_id": strategy_data["risk_profile_id"]}
                )
                risk_profile_row = risk_profile_result.first()

                if risk_profile_row:
                    strategy_data["risk_profile"] = {
                        "id": risk_profile_row.id,
                        "name": risk_profile_row.name,
                        "description": risk_profile_row.description,
                        "max_position_size_percentage": float(risk_profile_row.max_position_size_percentage)
                        if risk_profile_row.max_position_size_percentage
                        else None,
                        "max_drawdown_percentage": float(risk_profile_row.max_drawdown_percentage)
                        if risk_profile_row.max_drawdown_percentage
                        else None,
                        "max_exposure_per_asset_percentage": float(risk_profile_row.max_exposure_per_asset_percentage)
                        if risk_profile_row.max_exposure_per_asset_percentage
                        else None,
                        "circuit_breaker_conditions": json.loads(risk_profile_row.circuit_breaker_conditions)
                        if risk_profile_row.circuit_breaker_conditions
                        else None,
                    }

                # Get execution mappings
                exec_mappings_query = sa.text("""
                    SELECT sem.id, sem.strategy_id, sem.execution_strategy_id, sem.is_active, sem.priority,
                           es.name as execution_name, es.description as execution_description
                    FROM strategy_execution_mapping sem
                    JOIN execution_strategies es ON sem.execution_strategy_id = es.id
                    WHERE sem.strategy_id = :strategy_id
                """)
                exec_mappings_result = conn.execute(exec_mappings_query, {"strategy_id": strategy_data["id"]})

                execution_mappings = []
                for row in exec_mappings_result:
                    execution_mappings.append(
                        {
                            "id": row.id,
                            "strategy_id": row.strategy_id,
                            "execution_strategy_id": row.execution_strategy_id,
                            "is_active": row.is_active,
                            "priority": row.priority,
                            "execution_name": row.execution_name,
                            "execution_description": row.execution_description,
                        }
                    )
                strategy_data["execution_mappings"] = execution_mappings

                # Get exchange-pair mappings
                exchange_mappings_query = sa.text("""
                    SELECT sepm.id, sepm.strategy_id, sepm.exchange_id, sepm.trading_pair_id, sepm.is_active,
                           e.name as exchange_name, tp.symbol as pair_symbol
                    FROM strategy_exchange_pair_mappings sepm
                    JOIN exchanges e ON sepm.exchange_id = e.id
                    JOIN trading_pairs tp ON sepm.trading_pair_id = tp.id
                    WHERE sepm.strategy_id = :strategy_id
                """)
                exchange_mappings_result = conn.execute(exchange_mappings_query, {"strategy_id": strategy_data["id"]})

                exchange_pair_mappings = []
                for row in exchange_mappings_result:
                    exchange_pair_mappings.append(
                        {
                            "id": row.id,
                            "strategy_id": row.strategy_id,
                            "exchange_id": row.exchange_id,
                            "trading_pair_id": row.trading_pair_id,
                            "is_active": row.is_active,
                            "exchange_name": row.exchange_name,
                            "pair_symbol": row.pair_symbol,
                        }
                    )
                strategy_data["exchange_pair_mappings"] = exchange_pair_mappings

                # Convert additional_info from JSON string to dict if it exists
                if strategy_data.get("additional_info") and isinstance(strategy_data["additional_info"], str):
                    try:
                        strategy_data["additional_info"] = json.loads(strategy_data["additional_info"])
                    except json.JSONDecodeError:
                        pass

                # Create the Strategy object with the updated data
                return Strategy.model_validate(strategy_data)
        except Exception as e:
            self.logger.error(f"Error getting strategy by ID {strategy_id}: {e}")
            raise

    def get_by_name(self, name: str) -> Optional[Strategy]:
        """Get a strategy by its name"""
        try:
            with self.engine.begin() as conn:
                # First, get the strategy from the strategies table
                result = conn.execute(self.table.select().where(self.table.c.name == name))
                row = result.first()

                if not row:
                    return None

                # Use get_by_id to reuse code and get all related entities
                return self.get_by_id(row.id)
        except Exception as e:
            self.logger.error(f"Error getting strategy by name {name}: {e}")
            raise

    def get_all(self) -> List[Strategy]:
        """Get all strategies with their parameters"""
        try:
            with self.engine.begin() as conn:
                # Join strategies with strategy_parameters to get parameters
                query = sa.text("""
                    SELECT s.*, p.min_spread, p.threshold, p.max_slippage, p.min_volume
                    FROM strategies s
                    LEFT JOIN strategy_parameters p ON s.id = p.strategy_id
                """)

                result = conn.execute(query)
                strategies_list = []

                for row in result:
                    # Create a dict from the row
                    strategy_data = {col: getattr(row, col) for col in row._mapping.keys()}

                    # Convert parameters to float if they exist
                    if "min_spread" in strategy_data and strategy_data["min_spread"] is not None:
                        strategy_data["min_spread"] = float(strategy_data["min_spread"])
                    else:
                        strategy_data["min_spread"] = 0.0001  # Default value

                    if "threshold" in strategy_data and strategy_data["threshold"] is not None:
                        strategy_data["threshold"] = float(strategy_data["threshold"])

                    if "max_slippage" in strategy_data and strategy_data["max_slippage"] is not None:
                        strategy_data["max_slippage"] = float(strategy_data["max_slippage"])

                    if "min_volume" in strategy_data and strategy_data["min_volume"] is not None:
                        strategy_data["min_volume"] = float(strategy_data["min_volume"])

                    # Convert additional_info from JSON string to dict if it exists
                    if "additional_info" in strategy_data and isinstance(strategy_data["additional_info"], str):
                        try:
                            strategy_data["additional_info"] = json.loads(strategy_data["additional_info"])
                        except json.JSONDecodeError:
                            pass

                    # Create Strategy object
                    strategy = Strategy.model_validate(strategy_data)
                    strategies_list.append(strategy)

                return strategies_list
        except Exception as e:
            self.logger.error(f"Error getting all strategies: {e}")
            raise

    def get_active(self) -> List[Strategy]:
        """Get all active strategies with their parameters"""
        try:
            with self.engine.begin() as conn:
                # Join strategies with strategy_parameters to get parameters
                query = sa.text("""
                    SELECT s.*, p.min_spread, p.threshold, p.max_slippage, p.min_volume
                    FROM strategies s
                    LEFT JOIN strategy_parameters p ON s.id = p.strategy_id
                    WHERE s.is_active = TRUE
                """)

                result = conn.execute(query)
                strategies_list = []

                for row in result:
                    # Create a dict from the row
                    strategy_data = {col: getattr(row, col) for col in row._mapping.keys()}

                    # Convert parameters to float if they exist
                    if "min_spread" in strategy_data and strategy_data["min_spread"] is not None:
                        strategy_data["min_spread"] = float(strategy_data["min_spread"])
                    else:
                        strategy_data["min_spread"] = 0.0001  # Default value

                    if "threshold" in strategy_data and strategy_data["threshold"] is not None:
                        strategy_data["threshold"] = float(strategy_data["threshold"])

                    if "max_slippage" in strategy_data and strategy_data["max_slippage"] is not None:
                        strategy_data["max_slippage"] = float(strategy_data["max_slippage"])

                    if "min_volume" in strategy_data and strategy_data["min_volume"] is not None:
                        strategy_data["min_volume"] = float(strategy_data["min_volume"])

                    # Convert additional_info from JSON string to dict if it exists
                    if "additional_info" in strategy_data and isinstance(strategy_data["additional_info"], str):
                        try:
                            strategy_data["additional_info"] = json.loads(strategy_data["additional_info"])
                        except json.JSONDecodeError:
                            pass

                    # Create Strategy object
                    strategy = Strategy.model_validate(strategy_data)
                    strategies_list.append(strategy)

                return strategies_list
        except Exception as e:
            self.logger.error(f"Error getting active strategies: {e}")
            raise

    def set_active_status(self, strategy_id: int, is_active: bool) -> bool:
        """Set strategy active status"""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(
                    self.table.update()
                    .where(self.table.c.id == strategy_id)
                    .values(is_active=is_active)
                    .returning(self.table.c.id)
                )
                updated_id = result.scalar()
                return updated_id is not None
        except Exception as e:
            self.logger.error(f"Error updating strategy status for ID {strategy_id}: {e}")
            return False

    def set_active_status_by_name(self, strategy_name: str, is_active: bool) -> bool:
        """Update strategy status by name."""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(
                    self.table.update()
                    .where(self.table.c.name == strategy_name)
                    .values(is_active=is_active)
                    .returning(self.table.c.id)
                )
                updated_id = result.scalar()
                return updated_id is not None
        except Exception as e:
            self.logger.error(f"Error updating strategy status for '{strategy_name}': {e}")
            return False

    def activate_by_name(self, strategy_name: str) -> bool:
        """Activate a strategy by name."""
        return self.set_active_status_by_name(strategy_name, True)

    def deactivate_by_name(self, strategy_name: str) -> bool:
        """Deactivate a strategy by name."""
        return self.set_active_status_by_name(strategy_name, False)

    def update_stats(self, strategy_name: str, profit: float = 0, loss: float = 0, trade_count: int = 0):
        """Update strategy statistics after a trade execution"""
        try:
            with self.engine.begin() as conn:
                # Get current stats
                query = sa.select([self.table.c.total_profit, self.table.c.total_loss, self.table.c.trade_count]).where(
                    self.table.c.name == strategy_name
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
                        self.table.update()
                        .where(self.table.c.name == strategy_name)
                        .values(
                            total_profit=new_total_profit,
                            total_loss=new_total_loss,
                            net_profit=new_net_profit,
                            trade_count=current_trade_count + trade_count,
                            last_updated=datetime.now(),
                        )
                    )

                    self.logger.info(
                        f"Updated stats for {strategy_name}: "
                        f"profit +{profit}, loss +{loss}, net profit = {new_net_profit}, trades +{trade_count}"
                    )
                else:
                    self.logger.warning(f"Strategy {strategy_name} not found in database")
        except Exception as e:
            self.logger.error(f"Error updating strategy stats: {e}", exc_info=True)

    def get_exchange_pair_mappings(self, strategy_id: int) -> List[Dict[str, Any]]:
        """Get all exchange-pair mappings for a strategy"""
        try:
            with self.engine.begin() as conn:
                # Join the mappings table with exchanges and trading_pairs to get names and symbols
                query = sa.text("""
                    SELECT 
                        m.id as mapping_id,
                        s.name as strategy_name,
                        e.name as exchange_name,
                        tp.symbol as pair_symbol,
                        m.is_active as is_active
                    FROM 
                        strategy_exchange_pair_mappings m
                    JOIN 
                        strategies s ON m.strategy_id = s.id
                    JOIN 
                        exchanges e ON m.exchange_id = e.id
                    JOIN 
                        trading_pairs tp ON m.trading_pair_id = tp.id
                    WHERE 
                        m.strategy_id = :strategy_id
                """)

                result = conn.execute(query, {"strategy_id": strategy_id})
                mappings = []

                for row in result:
                    mappings.append(
                        {
                            "id": row.mapping_id,
                            "strategy_name": row.strategy_name,
                            "exchange_name": row.exchange_name,
                            "pair_symbol": row.pair_symbol,
                            "is_active": row.is_active,
                        }
                    )

                return mappings
        except Exception as e:
            self.logger.error(f"Error getting strategy-exchange-pair mappings for strategy {strategy_id}: {e}")
            return []

    def update(self, strategy: Strategy) -> Strategy:
        """Update an existing strategy"""
        try:
            with self.engine.begin() as conn:
                # Extract parameters for strategy_parameters table
                min_spread = strategy.min_spread if strategy.min_spread is not None else 0.0001
                threshold = (
                    strategy.threshold if hasattr(strategy, "threshold") and strategy.threshold is not None else 0.0001
                )
                max_slippage = (
                    strategy.max_slippage
                    if hasattr(strategy, "max_slippage") and strategy.max_slippage is not None
                    else None
                )
                min_volume = (
                    strategy.min_volume if hasattr(strategy, "min_volume") and strategy.min_volume is not None else None
                )

                # Prepare strategy data for update
                strategy_data = {
                    "name": strategy.name,
                    "description": strategy.description,
                    "strategy_type_id": strategy.strategy_type_id,
                    "risk_profile_id": strategy.risk_profile_id,
                    "starting_capital": strategy.starting_capital,
                    "total_profit": strategy.total_profit,
                    "total_loss": strategy.total_loss,
                    "net_profit": strategy.net_profit,
                    "trade_count": strategy.trade_count,
                    "is_active": strategy.is_active,
                    "last_updated": datetime.now(),
                    "additional_info": json.dumps(strategy.additional_info) if strategy.additional_info else None,
                }

                # Update strategy table
                conn.execute(self.table.update().where(self.table.c.id == strategy.id).values(**strategy_data))

                # Update or insert strategy parameters
                params_query = sa.text("SELECT id FROM strategy_parameters WHERE strategy_id = :strategy_id")
                params_result = conn.execute(params_query, {"strategy_id": strategy.id})
                params_row = params_result.first()

                params_data = {
                    "min_spread": min_spread,
                    "threshold": threshold,
                    "max_slippage": max_slippage,
                    "min_volume": min_volume,
                }

                if params_row:
                    # Update existing parameters
                    conn.execute(
                        sa.text("""
                            UPDATE strategy_parameters 
                            SET min_spread = :min_spread, 
                                threshold = :threshold, 
                                max_slippage = :max_slippage, 
                                min_volume = :min_volume
                            WHERE strategy_id = :strategy_id
                        """),
                        {**params_data, "strategy_id": strategy.id},
                    )
                else:
                    # Insert new parameters
                    conn.execute(self.params_table.insert().values(strategy_id=strategy.id, **params_data))

                # Return updated strategy with parameters
                return self.get_by_id(strategy.id)
        except Exception as e:
            self.logger.error(f"Error updating strategy {strategy.name}: {e}")
            raise
