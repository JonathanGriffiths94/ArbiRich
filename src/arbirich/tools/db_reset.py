#!/usr/bin/env python3
"""
Simple script to drop and recreate the ArbiRich database.
"""

import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent.parent))

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine

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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def reset_database():
    """Drop and recreate the database"""
    # First, connect to postgres database to drop/create the app database
    postgres_conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, user=POSTGRES_USER, password=POSTGRES_PASSWORD, dbname="postgres"
    )

    # Set isolation level to AUTOCOMMIT - needed for DROP/CREATE DATABASE
    postgres_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    # Get cursor
    cursor = postgres_conn.cursor()

    try:
        # 1. First, try to terminate all connections to our database
        logger.info(f"Terminating connections to {POSTGRES_DB}...")
        cursor.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{POSTGRES_DB}'")

        # 2. Drop database if it exists
        logger.info(f"Dropping database {POSTGRES_DB} if it exists...")
        cursor.execute(f"DROP DATABASE IF EXISTS {POSTGRES_DB}")

        # 3. Create database
        logger.info(f"Creating database {POSTGRES_DB}...")
        cursor.execute(f"CREATE DATABASE {POSTGRES_DB}")

        logger.info(f"Database {POSTGRES_DB} created successfully")
    except Exception as e:
        logger.error(f"Error resetting database: {e}")
        return False
    finally:
        cursor.close()
        postgres_conn.close()

    # Now connect to the newly created database and create the tables
    try:
        logger.info("Creating tables...")
        engine = create_engine(DATABASE_URL)
        metadata.create_all(engine)
        logger.info("Tables created successfully")

        # Prefill database with initial data
        logger.info("Prefilling database with initial data...")
        prefill_database()
        logger.info("Database prefill complete")

        return True
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        return False


def main():
    """Main function"""
    logger.info("Starting database reset...")

    try:
        if reset_database():
            logger.info("Database reset completed successfully")
            return 0
        else:
            logger.error("Database reset failed")
            return 1
    except KeyboardInterrupt:
        logger.info("Database reset cancelled by user")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
