import os

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def drop_database():
    conn = psycopg2.connect(
        dbname="postgres",
        user=os.getenv("POSTGRES_USER", "arbiuser"),
        password=os.getenv("POSTGRES_PASSWORD", "arbipassword"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {os.getenv('POSTGRES_DB', 'arbidb')}")
    cursor.close()
    conn.close()


if __name__ == "__main__":
    drop_database()
