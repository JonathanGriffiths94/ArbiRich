import logging
import sqlite3
from datetime import datetime


class DatabaseHandler(logging.Handler):
    def __init__(self, db_path):
        super().__init__()
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.create_table()

    def create_table(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trade_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                quantity REAL NOT NULL,
                price REAL NOT NULL,
                status TEXT NOT NULL,
                message TEXT
            )
        """)
        self.conn.commit()

    def emit(self, record):
        cursor = self.conn.cursor()
        log_entry = self.format(record)
        cursor.execute(
            """
            INSERT INTO trade_logs (timestamp, exchange, symbol, side, quantity, price, status, message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                datetime.now().isoformat(),
                record.exchange,
                record.symbol,
                record.side,
                record.quantity,
                record.price,
                record.status,
                log_entry,
            ),
        )
        self.conn.commit()

    def close(self):
        self.conn.close()
        super().close()


# Initialize logger
logger = logging.getLogger("trade_logger")
logger.setLevel(logging.INFO)

# Create and add the database handler
db_handler = DatabaseHandler("trades.db")
formatter = logging.Formatter("%(message)s")
db_handler.setFormatter(formatter)
logger.addHandler(db_handler)
