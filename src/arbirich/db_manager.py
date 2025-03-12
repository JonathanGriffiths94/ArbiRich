from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from src.arbirich.config import DATABASE_URL
from src.arbirich.models.orm_models import (
    Base,
    Exchange,
    TradeExecution,
    TradeOpportunity,
    TradingPair,
)

engine = create_engine(DATABASE_URL)
SessionLocal = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine)
)

Base.metadata.create_all(bind=engine)


class DatabaseManager:
    def __init__(self):
        self.db = SessionLocal()

    # ----- CRUD Operations for Exchanges -----
    def create_exchange(
        self, name: str, api_rate_limit: int = None, trade_fees=None, additional_info=None
    ):
        new_exchange = Exchange(
            name=name,
            api_rate_limit=api_rate_limit,
            trade_fees=trade_fees,
            additional_info=additional_info,
        )
        self.db.add(new_exchange)
        self.db.commit()
        self.db.refresh(new_exchange)
        return new_exchange

    def get_exchange(self, exchange_id: int):
        return self.db.query(Exchange).filter(Exchange.id == exchange_id).first()

    def update_exchange(self, exchange_id: int, **kwargs):
        exchange = self.get_exchange(exchange_id)
        if exchange:
            for key, value in kwargs.items():
                setattr(exchange, key, value)
            self.db.commit()
            self.db.refresh(exchange)
        return exchange

    def delete_exchange(self, exchange_id: int):
        exchange = self.get_exchange(exchange_id)
        if exchange:
            self.db.delete(exchange)
            self.db.commit()
        return exchange

    # ----- CRUD Operations for Trading Pairs -----
    def create_trading_pair(self, base_currency: str, quote_currency: str):
        new_pair = TradingPair(base_currency=base_currency, quote_currency=quote_currency)
        self.db.add(new_pair)
        self.db.commit()
        self.db.refresh(new_pair)
        return new_pair

    def get_trading_pair(self, pair_id: int):
        return self.db.query(TradingPair).filter(TradingPair.id == pair_id).first()

    # ----- CRUD Operations for Trade Opportunities -----
    def create_trade_opportunity(
        self,
        trading_pair_id: int,
        buy_exchange_id: int,
        sell_exchange_id: int,
        buy_price,
        sell_price,
        spread,
        volume,
        opportunity_timestamp: datetime,
    ):
        new_opportunity = TradeOpportunity(
            trading_pair_id=trading_pair_id,
            buy_exchange_id=buy_exchange_id,
            sell_exchange_id=sell_exchange_id,
            buy_price=buy_price,
            sell_price=sell_price,
            spread=spread,
            volume=volume,
            opportunity_timestamp=opportunity_timestamp,
        )
        self.db.add(new_opportunity)
        self.db.commit()
        self.db.refresh(new_opportunity)
        return new_opportunity

    def get_trade_opportunity(self, opportunity_uuid):
        return (
            self.db.query(TradeOpportunity)
            .filter(TradeOpportunity.id == opportunity_uuid)
            .first()
        )

    def update_trade_opportunity(self, opportunity_uuid, **kwargs):
        opportunity = self.get_trade_opportunity(opportunity_uuid)
        if opportunity:
            for key, value in kwargs.items():
                setattr(opportunity, key, value)
            self.db.commit()
            self.db.refresh(opportunity)
        return opportunity

    def delete_trade_opportunity(self, opportunity_uuid):
        opportunity = self.get_trade_opportunity(opportunity_uuid)
        if opportunity:
            self.db.delete(opportunity)
            self.db.commit()
        return opportunity

    # ----- CRUD Operations for Trade Executions -----
    def create_trade_execution(
        self,
        trading_pair_id: int,
        buy_exchange_id: int,
        sell_exchange_id: int,
        executed_buy_price,
        executed_sell_price,
        spread,
        volume,
        execution_timestamp: datetime,
        execution_id: str = None,
        opportunity_id=None,
    ):
        new_execution = TradeExecution(
            trading_pair_id=trading_pair_id,
            buy_exchange_id=buy_exchange_id,
            sell_exchange_id=sell_exchange_id,
            executed_buy_price=executed_buy_price,
            executed_sell_price=executed_sell_price,
            spread=spread,
            volume=volume,
            execution_timestamp=execution_timestamp,
            execution_id=execution_id,
            opportunity_id=opportunity_id,
        )
        self.db.add(new_execution)
        self.db.commit()
        self.db.refresh(new_execution)
        return new_execution

    def get_trade_execution(self, execution_uuid):
        return (
            self.db.query(TradeExecution)
            .filter(TradeExecution.id == execution_uuid)
            .first()
        )

    def update_trade_execution(self, execution_uuid, **kwargs):
        execution = self.get_trade_execution(execution_uuid)
        if execution:
            for key, value in kwargs.items():
                setattr(execution, key, value)
            self.db.commit()
            self.db.refresh(execution)
        return execution

    def delete_trade_execution(self, execution_uuid):
        execution = self.get_trade_execution(execution_uuid)
        if execution:
            self.db.delete(execution)
            self.db.commit()
        return execution

    # ----- Utility: Close Session -----
    def close(self):
        self.db.close()


if __name__ == "__main__":
    db_manager = DatabaseManager()
    try:
        # Create a new exchange
        exchange = db_manager.create_exchange(name="Test Exchange", api_rate_limit=1000)
        print(f"Created Exchange: {exchange.id} - {exchange.name}")

        # Create a trading pair
        pair = db_manager.create_trading_pair("BTC", "USDT")
        print(
            f"Created Trading Pair: {pair.id} - {pair.base_currency}/{pair.quote_currency}"
        )

        # Create a trade opportunity (use your own values for prices, spread, etc.)
        from datetime import datetime

        opportunity = db_manager.create_trade_opportunity(
            trading_pair_id=pair.id,
            buy_exchange_id=exchange.id,
            sell_exchange_id=exchange.id,  # Example uses same exchange for both sides
            buy_price=50000,
            sell_price=50500,
            spread=0.001,
            volume=0.1,
            opportunity_timestamp=datetime.utcnow(),
        )
        print(f"Created Trade Opportunity: {opportunity.id}")

    finally:
        db_manager.close()
