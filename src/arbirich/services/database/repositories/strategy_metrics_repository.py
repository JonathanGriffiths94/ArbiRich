from datetime import datetime
from typing import List, Optional

from src.arbirich.models.models import StrategyExchangeMetrics, StrategyMetrics, StrategyTradingPairMetrics
from src.arbirich.services.database.base_repository import BaseRepository


class StrategyMetricsRepository(BaseRepository[StrategyMetrics]):
    """Repository for Strategy Metrics entity operations"""

    def __init__(self, *args, **kwargs):
        super().__init__(model_class=StrategyMetrics, *args, **kwargs)

    def create(self, metrics: StrategyMetrics) -> StrategyMetrics:
        """Create a new strategy metrics record"""
        try:
            self.session.add(metrics)
            self.session.commit()
            return metrics
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error creating strategy metrics: {e}")
            raise

    def get_by_id(self, metrics_id: int) -> Optional[StrategyMetrics]:
        """Get strategy metrics by ID"""
        try:
            return self.session.query(StrategyMetrics).filter(StrategyMetrics.id == metrics_id).first()
        except Exception as e:
            self.logger.error(f"Error getting strategy metrics by ID {metrics_id}: {e}")
            raise

    def get_all_by_strategy(self, strategy_id: int) -> List[StrategyMetrics]:
        """Get all strategy metrics for a specific strategy"""
        try:
            return (
                self.session.query(StrategyMetrics)
                .filter(StrategyMetrics.strategy_id == strategy_id)
                .order_by(StrategyMetrics.period_end.desc())
                .all()
            )
        except Exception as e:
            self.logger.error(f"Error getting all metrics for strategy {strategy_id}: {e}")
            raise

    def get_latest_by_strategy(self, strategy_id: int) -> Optional[StrategyMetrics]:
        """Get the most recent metrics for a strategy"""
        try:
            return (
                self.session.query(StrategyMetrics)
                .filter(StrategyMetrics.strategy_id == strategy_id)
                .order_by(StrategyMetrics.period_end.desc())
                .first()
            )
        except Exception as e:
            self.logger.error(f"Error getting latest metrics for strategy {strategy_id}: {e}")
            raise

    def get_for_period(self, strategy_id: int, start_date: datetime, end_date: datetime) -> List[StrategyMetrics]:
        """Get all metrics for a strategy within a date range"""
        try:
            return (
                self.session.query(StrategyMetrics)
                .filter(
                    StrategyMetrics.strategy_id == strategy_id,
                    StrategyMetrics.period_end >= start_date,
                    StrategyMetrics.period_end <= end_date,
                )
                .order_by(StrategyMetrics.period_end.asc())
                .all()
            )
        except Exception as e:
            self.logger.error(
                f"Error getting metrics for strategy {strategy_id} in period {start_date} to {end_date}: {e}"
            )
            return []

    def save(self, metrics: StrategyMetrics) -> StrategyMetrics:
        """Save strategy metrics to database"""
        try:
            self.session.add(metrics)
            self.session.commit()
            return metrics
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error saving strategy metrics: {e}")
            raise


class StrategyTradingPairMetricsRepository(BaseRepository[StrategyTradingPairMetrics]):
    """Repository for Strategy Trading Pair Metrics entity operations"""

    def __init__(self, *args, **kwargs):
        super().__init__(model_class=StrategyTradingPairMetrics, *args, **kwargs)

    def create(self, metrics: StrategyTradingPairMetrics) -> StrategyTradingPairMetrics:
        """Create a new strategy trading pair metrics record"""
        try:
            self.session.add(metrics)
            self.session.commit()
            return metrics
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error creating strategy trading pair metrics: {e}")
            raise

    def get_by_metrics_id(self, metrics_id: int) -> List[StrategyTradingPairMetrics]:
        """Get all trading pair metrics for a specific strategy metrics record"""
        try:
            return (
                self.session.query(StrategyTradingPairMetrics)
                .filter(StrategyTradingPairMetrics.strategy_metrics_id == metrics_id)
                .all()
            )
        except Exception as e:
            self.logger.error(f"Error getting trading pair metrics by metrics ID {metrics_id}: {e}")
            raise


class StrategyExchangeMetricsRepository(BaseRepository[StrategyExchangeMetrics]):
    """Repository for Strategy Exchange Metrics entity operations"""

    def __init__(self, *args, **kwargs):
        super().__init__(model_class=StrategyExchangeMetrics, *args, **kwargs)

    def create(self, metrics: StrategyExchangeMetrics) -> StrategyExchangeMetrics:
        """Create a new strategy exchange metrics record"""
        try:
            self.session.add(metrics)
            self.session.commit()
            return metrics
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error creating strategy exchange metrics: {e}")
            raise

    def get_by_metrics_id(self, metrics_id: int) -> List[StrategyExchangeMetrics]:
        """Get all exchange metrics for a specific strategy metrics record"""
        try:
            return (
                self.session.query(StrategyExchangeMetrics)
                .filter(StrategyExchangeMetrics.strategy_metrics_id == metrics_id)
                .all()
            )
        except Exception as e:
            self.logger.error(f"Error getting exchange metrics by metrics ID {metrics_id}: {e}")
            raise
