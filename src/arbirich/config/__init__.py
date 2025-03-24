"""
Configuration module for ArbiRich.

This module provides access to configuration settings for the application.
"""

DEBUG = True

from src.arbirich.config.config import (
    ALL_EXCHANGES,
    ALL_PAIRS,
    ALL_STRATEGIES,
    DATABASE_URL,
    REDIS_URL,
    get_exchange_config,
    get_pair_config,
    get_strategy_config,
)

__all__ = [
    "DEBUG",
    "DATABASE_URL",
    "REDIS_URL",
    "ALL_EXCHANGES",
    "ALL_PAIRS",
    "ALL_STRATEGIES",
    "get_exchange_config",
    "get_pair_config",
    "get_strategy_config",
]
