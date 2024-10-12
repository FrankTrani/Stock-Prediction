from sqlalchemy import create_engine, text
import logging
import logging.handlers


logging.basicConfig(
    level=logging.WARNING, format="%(asctime)s - %(levelname)s - %(message)s"
)

engine = create_engine("sqlite:///stocks.db", echo=False)


def create_tables():
    """Creates the necessary tables: 'current', 'abnormal_stocks', and 'stock_symbols'."""
    logging.info("Creating 'current', 'abnormal_stocks', and 'stock_symbols' tables.")
    try:
        with engine.begin() as conn:
            # Drop 'current' table if it exists, to avoid duplicate data
            conn.execute(text("DROP TABLE IF EXISTS current"))
            # Create the 'current' table to store normal stocks with Z-scores
            conn.execute(
                text(
                    """
                    CREATE TABLE current (
                        symbol TEXT PRIMARY KEY,
                        name TEXT,
                        z_score REAL
                    )
                    """
                )
            )
            # Create the 'abnormal_stocks' table for stocks that don't follow a normal distribution
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS abnormal_stocks (
                        symbol TEXT PRIMARY KEY,
                        name TEXT,
                        sector TEXT
                    )
                    """
                )
            )
            # Create the 'stock_symbols' table to store stock symbols
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS stock_symbols (
                        symbol TEXT PRIMARY KEY
                    )
                    """
                )
            )
        logging.info("Tables are ready.")
    except Exception as e:
        logging.error(f"Error creating tables: {e}", exc_info=True)


if __name__ == "__main__":
    create_tables()
