import logging
import time

import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from src.arbirich.config.config import DATABASE_URL
from src.arbirich.models.models import Exchange, Strategy, TradingPair
from src.arbirich.models.schema import metadata
from src.arbirich.services.database.repositories.exchange_repository import ExchangeRepository
from src.arbirich.services.database.repositories.strategy_metrics_repository import (
    StrategyExchangeMetricsRepository,
    StrategyMetricsRepository,
    StrategyTradingPairMetricsRepository,
)
from src.arbirich.services.database.repositories.strategy_repository import StrategyRepository
from src.arbirich.services.database.repositories.trade_execution_repository import TradeExecutionRepository
from src.arbirich.services.database.repositories.trade_opportunity_repository import TradeOpportunityRepository
from src.arbirich.services.database.repositories.trading_pair_repository import TradingPairRepository

logger = logging.getLogger(__name__)

# Create engine
engine = sa.create_engine(DATABASE_URL)
metadata.create_all(engine)


class DatabaseService:
    """
    Main database service that provides access to all repositories.
    This service acts as a facade to the different repositories.
    """

    def __init__(self, engine: Engine = engine):
        self.engine = engine
        self.connection = None
        self._session = None
        self.Session = sessionmaker(bind=engine)
        self.logger = logger

        # Initialize repositories
        self._exchange_repo = None
        self._trading_pair_repo = None
        self._strategy_repo = None
        self._trade_opportunity_repo = None
        self._trade_execution_repo = None
        self._strategy_metrics_repo = None
        self._strategy_pair_metrics_repo = None
        self._strategy_exchange_metrics_repo = None

    def __enter__(self):
        self.connection = self.engine.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def session(self):
        """Get a SQLAlchemy session."""
        if self._session is None:
            self._session = self.Session()
        return self._session

    def close(self):
        """Close the database connection and release resources."""
        try:
            if self.connection:
                self.connection.close()
                self.connection = None

            if self._session:
                self._session.close()
                self._session = None

            if hasattr(self, "engine"):
                self.engine.dispose()

            logger.debug("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")

    # Repository properties for lazy initialization

    @property
    def exchange_repo(self) -> ExchangeRepository:
        if self._exchange_repo is None:
            self._exchange_repo = ExchangeRepository(engine=self.engine)
        return self._exchange_repo

    @property
    def trading_pair_repo(self) -> TradingPairRepository:
        if self._trading_pair_repo is None:
            self._trading_pair_repo = TradingPairRepository(engine=self.engine)
        return self._trading_pair_repo

    @property
    def strategy_repo(self) -> StrategyRepository:
        if self._strategy_repo is None:
            self._strategy_repo = StrategyRepository(engine=self.engine)
        return self._strategy_repo

    @property
    def trade_opportunity_repo(self) -> TradeOpportunityRepository:
        if self._trade_opportunity_repo is None:
            self._trade_opportunity_repo = TradeOpportunityRepository(engine=self.engine)
        return self._trade_opportunity_repo

    @property
    def trade_execution_repo(self) -> TradeExecutionRepository:
        if self._trade_execution_repo is None:
            self._trade_execution_repo = TradeExecutionRepository(engine=self.engine)
        return self._trade_execution_repo

    @property
    def strategy_metrics_repo(self) -> StrategyMetricsRepository:
        if self._strategy_metrics_repo is None:
            self._strategy_metrics_repo = StrategyMetricsRepository(engine=self.engine)
        return self._strategy_metrics_repo

    @property
    def strategy_pair_metrics_repo(self) -> StrategyTradingPairMetricsRepository:
        if self._strategy_pair_metrics_repo is None:
            self._strategy_pair_metrics_repo = StrategyTradingPairMetricsRepository(engine=self.engine)
        return self._strategy_pair_metrics_repo

    @property
    def strategy_exchange_metrics_repo(self) -> StrategyExchangeMetricsRepository:
        if self._strategy_exchange_metrics_repo is None:
            self._strategy_exchange_metrics_repo = StrategyExchangeMetricsRepository(engine=self.engine)
        return self._strategy_exchange_metrics_repo

    # Exchange facade methods
    def get_exchange_by_name(self, name: str) -> Exchange:
        """Get an exchange by name."""
        return self.exchange_repo.get_by_name(name)

    def get_exchange_by_id(self, exchange_id: int) -> Exchange:
        """Get an exchange by ID."""
        return self.exchange_repo.get_by_id(exchange_id)

    def get_active_exchanges(self) -> list[Exchange]:
        """Get all active exchanges."""
        return self.exchange_repo.get_active()

    def get_all_exchanges(self) -> list[Exchange]:
        """Get all exchanges."""
        return self.exchange_repo.get_all()

    # Trading pair facade methods
    def get_pair_by_symbol(self, symbol: str) -> TradingPair:
        """Get a trading pair by symbol."""
        return self.trading_pair_repo.get_by_symbol(symbol)

    def create_pair(self, pair: TradingPair) -> TradingPair:
        """Create a new trading pair."""
        return self.trading_pair_repo.create(pair)

    def update_pair(self, pair: TradingPair) -> TradingPair:
        """Update an existing trading pair."""
        return self.trading_pair_repo.update(pair)

    def get_all_pairs(self) -> list[TradingPair]:
        """Get all trading pairs."""
        return self.trading_pair_repo.get_all()

    def get_active_pairs(self) -> list[TradingPair]:
        """Get all active trading pairs."""
        return self.trading_pair_repo.get_active()

    def get_pair_by_id(self, pair_id: int) -> TradingPair:
        """Get a trading pair by ID."""
        return self.trading_pair_repo.get_by_id(pair_id)

    # Strategy facade methods
    def get_strategy_by_name(self, name: str) -> Strategy:
        """Get a strategy by name."""
        return self.strategy_repo.get_by_name(name)

    def get_active_strategies(self) -> list[Strategy]:
        """Get all active strategies."""
        return self.strategy_repo.get_active()

    def get_all_strategies(self) -> list[Strategy]:
        """Get all strategies."""
        return self.strategy_repo.get_all()

    def get_strategy_by_id(self, strategy_id: int) -> Strategy:
        """Get a strategy by ID."""
        return self.strategy_repo.get_by_id(strategy_id)

    def update_strategy_status_by_name(self, strategy_name: str, is_active: bool) -> bool:
        """Update strategy active status by name."""
        return self.strategy_repo.set_active_status_by_name(strategy_name, is_active)

    def activate_strategy_by_name(self, strategy_name: str) -> bool:
        """Activate a strategy by name."""
        return self.strategy_repo.activate_by_name(strategy_name)

    def deactivate_strategy_by_name(self, strategy_name: str) -> bool:
        """Deactivate a strategy by name."""
        return self.strategy_repo.deactivate_by_name(strategy_name)

    def set_exchange_active(self, exchange_name: str, is_active: bool) -> bool:
        """Set exchange active status."""
        return self.exchange_repo.set_active_status_by_name(exchange_name, is_active)

    def set_pair_active(self, pair_symbol: str, is_active: bool) -> bool:
        """Set trading pair active status."""
        # For pairs that may be in tuple format like ('BTC', 'USDT')
        if isinstance(pair_symbol, (tuple, list)) and len(pair_symbol) == 2:
            pair_symbol = f"{pair_symbol[0]}-{pair_symbol[1]}"
        return self.trading_pair_repo.set_active_status_by_symbol(pair_symbol, is_active)

    def is_exchange_in_use(self, exchange_name: str) -> bool:
        """Check if an exchange is used by any active strategy."""
        active_strategies = self.get_active_strategies()
        for strategy in active_strategies:
            if hasattr(strategy, "additional_info") and strategy.additional_info:
                if isinstance(strategy.additional_info, str):
                    try:
                        import json

                        additional_info = json.loads(strategy.additional_info)
                    except json.JSONDecodeError:
                        additional_info = {}
                else:
                    additional_info = strategy.additional_info

                exchanges = additional_info.get("exchanges", [])
                if exchange_name in exchanges:
                    return True
        return False

    def is_pair_in_use(self, pair_symbol: str) -> bool:
        """Check if a trading pair is used by any active strategy."""
        # Convert pair to symbol format if it's a tuple
        if isinstance(pair_symbol, (tuple, list)) and len(pair_symbol) == 2:
            pair_symbol = f"{pair_symbol[0]}-{pair_symbol[1]}"

        active_strategies = self.get_active_strategies()
        for strategy in active_strategies:
            if hasattr(strategy, "additional_info") and strategy.additional_info:
                if isinstance(strategy.additional_info, str):
                    try:
                        import json

                        additional_info = json.loads(strategy.additional_info)
                    except json.JSONDecodeError:
                        additional_info = {}
                else:
                    additional_info = strategy.additional_info

                pairs = additional_info.get("pairs", [])
                for pair in pairs:
                    # Convert pair format if needed
                    if isinstance(pair, (tuple, list)) and len(pair) == 2:
                        pair = f"{pair[0]}-{pair[1]}"
                    if pair == pair_symbol:
                        return True
        return False

    # Trade execution facade methods
    def get_executions_by_strategy(self, strategy_name: str) -> list:
        """Get all trade executions for a specific strategy."""
        # First get the strategy by name to get its ID
        strategy = self.get_strategy_by_name(strategy_name)
        if not strategy:
            self.logger.warning(f"Strategy not found with name: {strategy_name}")
            return []

        # Then get executions by strategy ID
        return self.trade_execution_repo.get_by_strategy_id(strategy.id)

    def get_recent_executions(self, count: int = 10, strategy_name: str = None) -> list:
        """Get most recent trade executions."""
        # If strategy name is provided, get its ID first
        strategy_id = None
        if strategy_name:
            strategy = self.get_strategy_by_name(strategy_name)
            if strategy:
                strategy_id = strategy.id
            else:
                self.logger.warning(f"Strategy not found with name: {strategy_name}")
                return []

        return self.trade_execution_repo.get_recent(count, strategy_id)

    # Trade opportunity facade methods
    def get_opportunities_by_strategy(self, strategy_name: str) -> list:
        """Get all trade opportunities for a specific strategy."""
        return self.trade_opportunity_repo.get_by_strategy(strategy_name)

    def get_recent_opportunities(self, count: int = 10, strategy_name: str = None) -> list:
        """Get most recent trade opportunities."""
        return self.trade_opportunity_repo.get_recent(count, strategy_name)

    async def get_trading_statistics(self):
        """
        Get system-wide trading statistics.

        Returns:
            Dict containing aggregated trading statistics
        """
        try:
            # This is a stub implementation - you would typically query your database
            # for this information. For now, we'll return some default values.
            stats = {
                "total_profit": 0.0,
                "total_trades": 0,
                "successful_trades": 0,
                "failed_trades": 0,
                "start_time": time.time() - 3600,  # 1 hour ago
                "strategy_performance": {},
                "pair_performance": {},
                "exchange_performance": {},
            }

            # You would typically get this from your database, e.g.:
            # with self.engine.connect() as conn:
            #     result = conn.execute(text("SELECT SUM(profit) FROM trade_executions"))
            #     stats["total_profit"] = result.scalar() or 0.0

            logger.info("Retrieved trading statistics")
            return stats
        except Exception as e:
            logger.error(f"Error retrieving trading statistics: {e}")
            return None

    def get_latest_strategy_metrics(self, strategy_id: int):
        """
        Get the latest metrics for a strategy.

        Args:
            strategy_id: ID of the strategy

        Returns:
            StrategyMetrics object or None if not found
        """
        try:
            from src.arbirich.services.database.repositories.strategy_metrics_repository import (
                StrategyMetricsRepository,
            )

            # Create a repository instance using engine parameter to match other repository initializations
            repository = StrategyMetricsRepository(engine=self.engine)

            # Get the latest metrics
            return repository.get_latest_by_strategy(strategy_id)
        except Exception as e:
            logger.error(f"Error getting latest metrics for strategy {strategy_id}: {e}")
            return None


def cleanup_db_connections():
    """
    Cleanup any open database connections.
    This is called during application shutdown.
    """
    import sqlalchemy

    logger = logging.getLogger(__name__)
    try:
        # Dispose of the engine's connection pool
        db_instance = DatabaseService()
        logger.info("Disposing of database engine connections")

        if hasattr(db_instance, "engine") and db_instance.engine:
            db_instance.engine.dispose()
            logger.info("Database engine connections disposed")

        # Check for SQLAlchemy's global engine registry
        if hasattr(sqlalchemy, "_registries"):
            for registry in sqlalchemy._registries:
                if hasattr(registry, "dispose_all"):
                    logger.info("Disposing of all engines in registry")
                    registry.dispose_all()

        logger.info("Database connections cleanup completed")
    except Exception as e:
        logger.error(f"Error cleaning up database connections: {e}")
