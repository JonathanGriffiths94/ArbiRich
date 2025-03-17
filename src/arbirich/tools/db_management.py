#!/usr/bin/env python3
"""
Database management utility for ArbiRich.
Provides tools to drop, create, and reset the database.
"""

import argparse
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent.parent))

from sqlalchemy import create_engine, text

from src.arbirich.config import (
    DATABASE_URL,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)
from src.arbirich.models.schema import metadata
from src.arbirich.services.database.prefill_database import prefill_database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def create_db():
    """Create the database if it doesn't exist"""
    # Connect to postgres DB to create our application DB
    postgres_url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/postgres"
    engine = create_engine(postgres_url)

    # Check if database exists
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1 FROM pg_database WHERE datname = :db_name"), {"db_name": POSTGRES_DB})
        exists = result.scalar() == 1

    if not exists:
        # DB doesn't exist, create it
        with engine.connect() as conn:
            # Disconnect any existing connections
            conn.execute(
                text(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{POSTGRES_DB}'")
            )
            conn.execute(text("COMMIT"))

            # Create database
            conn.execute(text(f"CREATE DATABASE {POSTGRES_DB}"))
            logger.info(f"Database {POSTGRES_DB} created")
    else:
        logger.info(f"Database {POSTGRES_DB} already exists")

    # Connect to the app database and create tables
    app_engine = create_engine(DATABASE_URL)
    metadata.create_all(app_engine)
    logger.info("Tables created")

    # Close connections
    engine.dispose()
    app_engine.dispose()


def drop_db(confirm=False):
    """Drop the database"""
    if not confirm:
        response = input(f"Are you sure you want to drop database {POSTGRES_DB}? This will delete ALL data. (y/N): ")
        if response.lower() not in ("y", "yes"):
            logger.info("Database drop cancelled")
            return False

    # Connect to postgres DB
    postgres_url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/postgres"
    engine = create_engine(postgres_url)

    try:
        with engine.connect() as conn:
            # Disconnect any existing connections
            conn.execute(
                text(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{POSTGRES_DB}'")
            )
            conn.execute(text("COMMIT"))

            # Drop database
            conn.execute(text(f"DROP DATABASE IF EXISTS {POSTGRES_DB}"))
            logger.info(f"Database {POSTGRES_DB} dropped")
    except Exception as e:
        logger.error(f"Error dropping database: {e}")
        return False
    finally:
        engine.dispose()

    return True


def reset_db(confirm=False):
    """Drop and recreate the database"""
    if drop_db(confirm):
        create_db()
        logger.info("Database has been reset")

        # Prefill database with initial data
        logger.info("Prefilling database with initial data...")
        prefill_database()
        logger.info("Database prefill complete")
        return True
    return False


def main():
    parser = argparse.ArgumentParser(description="ArbiRich Database Management Utility")
    parser.add_argument(
        "--action", choices=["create", "drop", "reset"], required=True, help="Action to perform on the database"
    )
    parser.add_argument("--confirm", action="store_true", help="Confirm destructive actions without prompting")

    args = parser.parse_args()

    logger.info(f"Database URL: {DATABASE_URL}")

    if args.action == "create":
        create_db()
    elif args.action == "drop":
        drop_db(args.confirm)
    elif args.action == "reset":
        reset_db(args.confirm)


if __name__ == "__main__":
    main()
