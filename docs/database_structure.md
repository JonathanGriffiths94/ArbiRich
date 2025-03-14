# ArbiRich Database Structure

## Overview

ArbiRich uses a SQLAlchemy-based database layer with a modern approach that separates:

1. **Schema Definitions**: Tables and relationships defined in `schema.py`
2. **Pydantic Models**: Used for validation and serialization in `models.py`

## Database Tables

The database consists of the following tables:

### exchanges
- Stores the supported cryptocurrency exchanges
- Fields: id, name, api_rate_limit, trade_fees, additional_info, created_at

### pairs
- Stores trading pairs (e.g., BTC-USDT)
- Fields: id, base_currency, quote_currency

### strategies
- Stores trading strategies with their configurations
- Fields: id, name, starting_capital, min_spread, additional_info, total_profit, total_loss, net_profit, trades_count, start_timestamp, last_updated

### trade_opportunities
- Records arbitrage opportunities identified by the system
- Fields: id, strategy_id, pair_id, buy_exchange_id, sell_exchange_id, buy_price, sell_price, spread, volume, opportunity_timestamp

### trade_executions
- Records executed trades based on opportunities
- Fields: id, strategy_id, pair_id, buy_exchange_id, sell_exchange_id, executed_buy_price, executed_sell_price, spread, volume, execution_timestamp, execution_id, opportunity_id

## Database Access

The application uses the `DatabaseService` class in `services/database_service.py` to interact with the database. This service provides methods for all common database operations related to the application's entities.

## Database Initialization

The database is initialized with required reference data using the `prefill_database.py` script:

```bash
python -m src.arbirich.prefill_database
```

This script populates the database with:
- Supported exchanges
- Common trading pairs
- Strategy definitions
